import logging
import geopandas as gpd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from app.config import settings
from unidecode import unidecode

logger = logging.getLogger(__name__)

# Limites para avaliação de cobertura obs: isso está embasado na portaria nº 2.436, de 21 de setembro de 2017
INTERVALO_MAX = 0.5  # 1 UBS para cada 2000 pessoas => 0.5/1000
INTERVALO_MIN = 0.29 # 1 UBS para cada ~3500 pessoas => ~0.29/1000

def find_column(possible_columns, df):
    normalized_cols = {unidecode(c).lower(): c for c in df.columns}
    for candidate in possible_columns:
        cand_norm = unidecode(candidate).lower()
        if cand_norm in normalized_cols:
            real_name = normalized_cols[cand_norm]
            logger.info("Coluna '%s' encontrada.", real_name)
            return real_name
    logger.warning("Nenhuma coluna encontrada dentre: %s", possible_columns)
    return None

def analyze_knn_allocation(knn_df, demands_gdf, opportunities_gdf, settings):
    """
    Obs: Aqui eu substitui o 'allocate_demands' antigo (que usava cKDTree).
    Agora recebe diretamente o resultado KNN (knn_df).
    
    - knn_df: DataFrame com colunas:
        demand_id, opportunity_name, distance_km, ...
      (gerado pelo seu método KNN, seja geodesic ou pandana)
    - demands_gdf: GeoDataFrame das demandas (com colunas de população e raça).
    - opportunities_gdf: GeoDataFrame das UBS (para identificar nomes, cidades, etc.).
    - settings: Configurações (listas de possíveis nomes de coluna).

    Retorna:
      allocation, summary_data
      - allocation: dicionário (chave = ID do estabelecimento [cnes_column], valor = dict com informações)
      - summary_data: dict com estatísticas gerais (população total, total UBS, etc.)

    Observação: Mantemos as MESMAS chaves que a função 'allocate_demands' produzia,
    para que create_charts(allocation) e generate_allocation_pdf(allocation, summary_data) funcionem sem mudar.
    """

    logger.info("Iniciando análise socioeconômica a partir de um DataFrame KNN.")

    # ----------------------------
    # 1) Identifica colunas de população e raça no demands_gdf
    # ----------------------------
    pop_column = find_column(settings.POPULATION_POSSIBLE_COLUMNS, demands_gdf)
    black_column = find_column(settings.BLACK_POPULATION_POSSIBLE_COLUMNS, demands_gdf)
    brown_column = find_column(settings.BROWN_POPULATION_POSSIBLE_COLUMNS, demands_gdf)
    indigenous_column = find_column(settings.INDIGENOUS_POPULATION_POSSIBLE_COLUMNS, demands_gdf)
    yellow_column = find_column(settings.YELLOW_POPULATION_POSSIBLE_COLUMNS, demands_gdf)
    sector_column = find_column(settings.DEMAND_ID_POSSIBLE_COLUMNS, demands_gdf)

    if not pop_column:
        logger.error("Coluna de população não encontrada em demands_gdf.")
        raise ValueError("Nenhuma coluna correspondente à população encontrada.")

    # Conversão de chave de merge para string
    knn_df["demand_id"] = knn_df["demand_id"].astype(str)
    demands_gdf[sector_column] = demands_gdf[sector_column].astype(str)

    # ----------------------------
    # 2) Calcula população total da cidade
    # ----------------------------
    total_people_city = demands_gdf.drop_duplicates(subset=[sector_column])[pop_column].sum()
    logger.info("População total da cidade calculada: %s", total_people_city)

    # Calcula total para grupos raciais
    total_people_negros = demands_gdf.drop_duplicates(subset=[sector_column])[black_column].sum() if black_column else 0
    total_people_pardos = demands_gdf.drop_duplicates(subset=[sector_column])[brown_column].sum() if brown_column else 0
    total_people_indigenas = demands_gdf.drop_duplicates(subset=[sector_column])[indigenous_column].sum() if indigenous_column else 0
    total_people_amarela = demands_gdf.drop_duplicates(subset=[sector_column])[yellow_column].sum() if yellow_column else 0

    # ----------------------------
    # 3) Conta total de UBS
    # ----------------------------
    total_ubs = len(opportunities_gdf)  # ou unique() de algo
    ubs_per_1000 = (total_ubs / total_people_city) * 1000 if total_people_city > 0 else 0

    if ubs_per_1000 >= INTERVALO_MAX:
        ubs_situation = "Suficiente"
    elif ubs_per_1000 >= INTERVALO_MIN:
        ubs_situation = "Intermediário"
    else:
        ubs_situation = "Deficitário"

    # ----------------------------
    # 4) Colunas para identificar cada UBS
    # ----------------------------
    cnes_column = find_column(settings.ESTABLISHMENT_ID_POSSIBLE_COLUMNS, opportunities_gdf)
    city_column = find_column(settings.CITY_POSSIBLE_COLUMNS, opportunities_gdf)
    name_column = find_column(settings.NAME_POSSIBLE_COLUMNS, opportunities_gdf)

    if not cnes_column or not city_column or not name_column:
        logger.error("Colunas essenciais para estabelecimento não encontradas.")
        raise ValueError("Colunas essenciais (CNES, cidade, nome) não encontradas em opportunities_gdf.")

    # Cria um "mapa" do nome da oportunidade → (cnes, city, nome_ubs)
    # pois no knn_df 'opportunity_name' deve bater com a coluna do name_column no opportunities_gdf
    # Ex.: se no knn vc fez dist_df.columns = ubs_gdf[name_column], então "opportunity_name" == ubs_gdf[name_column].
    # Então fara um dict com base em opportunities_gdf.
    establishments_map = {}
    for idx, row in opportunities_gdf.iterrows():
        key_name = str(row[name_column])  
        cnes_val = row[cnes_column]
        city_val = row[city_column]
        ub_name = row[name_column]

        establishments_map[key_name] = (cnes_val, city_val, ub_name)

    # ----------------------------
    # 5) Mescla knn_df com demands_gdf para trazer colunas de população e raça
    # ----------------------------
    merged = knn_df.merge(
        demands_gdf[[sector_column, pop_column, black_column, brown_column, indigenous_column, yellow_column]],
        left_on="demand_id",
        right_on=sector_column,
        how="left"
    )

    # Agora "merged" tem: 
    #  demand_id, opportunity_name, distance_km, [pop_column, black_column, ...] etc.

    # ----------------------------
    # 6) Agrupa por "opportunity_name" e calcular estatísticas
    # ----------------------------
    grouped = merged.groupby("opportunity_name")
    
    # Montaremos o dicionário 'allocation', onde a chave será o valor do "cnes_column" (ou algo identificador),
    allocation = {}

    for opp_name, subdf in grouped:
        # Distância média em km
        mean_distance_km = subdf["distance_km"].mean() if not subdf["distance_km"].isnull().all() else 0.0

        # Para manter a mesma lógica de "se <=700 => 'Ótima (700m)'", etc., precisamos converter para metros:
        mean_distance_m = mean_distance_km * 1000.0

        if mean_distance_m <= 700:
            radius = 'Ótima (700m)'
        elif mean_distance_m <= 1000:
            radius = 'Boa (1000m)'
        elif mean_distance_m <= 2000:
            radius = 'Regular (2000m)'
        else:
            radius = 'Ruim (>2000m)'

        # Soma das populações atendidas
        total_people_ubs = subdf[pop_column].sum()
        percentage_ubs = (total_people_ubs / total_people_city) * 100 if total_people_city > 0 else 0

        total_negros = subdf[black_column].sum() if black_column else 0
        total_pardos = subdf[brown_column].sum() if brown_column else 0
        total_indigenas = subdf[indigenous_column].sum() if indigenous_column else 0
        total_amarela = subdf[yellow_column].sum() if yellow_column else 0

        percentage_ubs_negros = (total_negros / total_people_negros * 100) if total_people_negros > 0 else 0
        percentage_ubs_pardos = (total_pardos / total_people_pardos * 100) if total_people_pardos > 0 else 0
        percentage_ubs_indigenas = (total_indigenas / total_people_indigenas * 100) if total_people_indigenas > 0 else 0
        percentage_ubs_amarela = (total_amarela / total_people_amarela * 100) if total_people_amarela > 0 else 0

        # Recuperar info do establishments_map
        if opp_name in establishments_map:
            cnes_val, city_val, ub_name = establishments_map[opp_name]
        else:
            # Se não encontrou, define placeholders
            cnes_val = "Desconhecido"
            city_val = "?"
            ub_name = opp_name  # fallback

        # Monta o dicionário final (clonando a estrutura antiga).
        # Observação: a chave do 'allocation' era est[cnes_column] no código antigo.
        # Então vamos usar cnes_val como chave.
        allocation[cnes_val] = {
            'Establishment': city_val,      
            'UBS_Name': ub_name,            
            'Radius': radius,
            'Mean_Distance': mean_distance_m,        # Em METROS, p/ chart e PDF
            'Total_People': total_people_ubs,
            'Total_People_Negros': total_negros,
            'Total_People_Pardos': total_pardos,
            'Total_People_Indigenas': total_indigenas,
            'Total_People_Amarela': total_amarela,
            'Percentage_City': percentage_ubs,
            'Percentage_Negros': percentage_ubs_negros,
            'Percentage_Pardos': percentage_ubs_pardos,
            'Percentage_Indigenas': percentage_ubs_indigenas,
            'Percentage_Amarela': percentage_ubs_amarela,
        }

    logger.info("Análise finalizada para %d estabelecimentos (UBS).", len(allocation))

    # ----------------------------
    # 7) Monta summary_data
    # ----------------------------
    summary_data = {
        "Total_City_Population": total_people_city,
        "Total_UBS": total_ubs,
        "UBS_per_1000": ubs_per_1000,
        "UBS_Situation": ubs_situation
    }

    logger.info("Resumo calculado: %s", summary_data)
    return allocation, summary_data


#
# As funções de gráfico e PDF permanecem IGUAIS:
#

def create_charts(allocation):
    logger.info("Iniciando criação dos gráficos a partir dos dados de alocação.")
    df = pd.DataFrame.from_dict(allocation, orient='index').dropna()

    if df.empty:
        logger.warning("Nenhum dado disponível para plotagem.")
        return None

    # Cria figura com 4 subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle("Análise de UBS e Demandas", fontsize=16)

    # Gráfico 1: Top 10 UBS com maior atendimento
    top_people = df.sort_values(by='Total_People', ascending=False).head(10)
    axes[0, 0].barh(top_people['UBS_Name'], top_people['Total_People'], color='blue')
    axes[0, 0].set_title("Top 10 UBS com Mais Pessoas Atendidas")
    axes[0, 0].set_xlabel("Total de Pessoas")
    axes[0, 0].invert_yaxis()

    # Gráfico 2: Top 10 UBS com maior média de distância
    top_distance = df.sort_values(by='Mean_Distance', ascending=False).head(10)
    axes[0, 1].barh(top_distance['UBS_Name'], top_distance['Mean_Distance'], color='red')
    axes[0, 1].set_title("Top 10 UBS com Maior Média de Distância")
    axes[0, 1].set_xlabel("Média de Distância (m)")
    axes[0, 1].invert_yaxis()

    # Gráfico 3: Distribuição dos raios de cobertura
    df['Radius'].value_counts().plot(kind='bar', ax=axes[1, 0], color='green')
    axes[1, 0].set_title("Distribuição do Raio de Cobertura")
    axes[1, 0].set_ylabel("Quantidade de UBS")

    # Gráfico 4: Comparação entre grupos raciais atendidos
    racial_data = df[['Total_People_Negros', 'Total_People_Pardos', 'Total_People_Indigenas', 'Total_People_Amarela']].sum()
    racial_data.plot(kind='bar', ax=axes[1, 1], color=['brown', 'orange', 'purple', 'yellow'])
    axes[1, 1].set_title("Atendimento por Grupo Racial")
    axes[1, 1].set_ylabel("Total de Pessoas Atendidas")

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png')
    img_buffer.seek(0)
    plt.close()
    logger.info("Gráficos criados com sucesso.")
    return img_buffer


def generate_allocation_pdf(allocation, summary):
    logger.info("Iniciando geração do relatório PDF.")
    pdf_buffer = BytesIO()
    c = canvas.Canvas(pdf_buffer, pagesize=letter)
    width, height = letter
    
    # Título do relatório
    c.setFont("Helvetica-Bold", 18)
    c.drawString(100, height - 50, "Relatório de Análise de UBS e Demandas")
    c.line(100, height - 55, 500, height - 55)
    
    # Resumo dos principais indicadores
    c.setFont("Helvetica-Bold", 14)
    c.drawString(100, height - 80, "Resumo da Situação das UBS")
    c.setFont("Helvetica", 12)
    summary_text = [
        f"População Total: {summary.get('Total_City_Population', 0):,}".replace(",", "."),
        f"Total de UBS: {summary.get('Total_UBS', 0)}",
        f"UBS por 1000 habitantes: {summary.get('UBS_per_1000', 0):.2f}",
        f"Situação das UBS: {summary.get('UBS_Situation', 'Não disponível')}"
    ]
    
    y_position = height - 110
    for line in summary_text:
        c.drawString(100, y_position, line)
        y_position -= 20
    
    # Detalhamento por UBS
    c.setFont("Helvetica-Bold", 14)
    c.drawString(100, y_position - 20, "Detalhamento das UBS")
    c.setFont("Helvetica", 10)
    y_position -= 40
    
    for key, data in allocation.items():
        if y_position < 100:
            c.showPage()
            c.setFont("Helvetica", 10)
            y_position = height - 50
        
        c.drawString(100, y_position, f"UBS: {data['UBS_Name']} ({data['Establishment']})")
        c.drawString(120, y_position - 15, f"Raio de Cobertura: {data['Radius']}")
        c.drawString(120, y_position - 30, f"Distância Média: {data['Mean_Distance']:.2f}m")
        c.drawString(120, y_position - 45, f"Pessoas Atendidas: {data['Total_People']}")
        c.drawString(120, y_position - 60, f"Cobertura Relativa à Cidade: {data['Percentage_City']:.2f}%")
        c.drawString(120, y_position - 75, f"População Negra Atendida: {data['Percentage_Negros']:.2f}%")
        c.drawString(120, y_position - 90, f"População Parda Atendida: {data['Percentage_Pardos']:.2f}%")
        c.drawString(120, y_position - 105, f"População Indígena Atendida: {data['Percentage_Indigenas']:.2f}%")
        c.drawString(120, y_position - 120, f"População Amarela Atendida: {data['Percentage_Amarela']:.2f}%")
        y_position -= 140
    
    # Inserção dos gráficos no relatório
    charts = create_charts(allocation)
    if charts:
        c.showPage()
        c.setFont("Helvetica-Bold", 14)
        c.drawString(100, height - 50, "Visualização Gráfica dos Dados")
        img = ImageReader(charts)
        c.drawImage(img, 100, height - 400, width=400, height=300)
    
    c.showPage()
    c.save()
    pdf_buffer.seek(0)
    logger.info("Relatório PDF gerado com sucesso.")
    return pdf_buffer
