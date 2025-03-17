import logging
import geopandas as gpd
import pandas as pd
from scipy.spatial import cKDTree
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from app.config import settings

# Configuração do logger para este módulo
logger = logging.getLogger(__name__)

# Limites para avaliação de cobertura
INTERVALO_MAX = 0.5  # 1 UBS para cada 2000 pessoas
INTERVALO_MIN = 0.29  # 1 UBS para cada 3500 pessoas

def find_column(possible_columns, df):
    logger.info("Procurando coluna dentre: %s", possible_columns)
    col = next((col for col in possible_columns if col in df.columns), None)
    if col:
        logger.info("Coluna '%s' encontrada.", col)
    else:
        logger.warning("Nenhuma coluna encontrada dentre: %s", possible_columns)
    return col

def allocate_demands(demands_gdf, establishments_gdf, settings):
    """
    Calcula a alocação de demandas para estabelecimentos usando a distância (via cKDTree)
    e agrega estatísticas socioeconômicas (como percentuais por grupo racial).
    """
    logger.info("Convertendo CRS dos GeoDataFrames para EPSG:3857 para cálculos precisos.")
    demands_gdf = demands_gdf.to_crs(epsg=3857)
    establishments_gdf = establishments_gdf.to_crs(epsg=3857)
    
    logger.info("Inferindo colunas relevantes para população e identificação de setores.")
    pop_column = find_column(settings.POPULATION_POSSIBLE_COLUMNS, demands_gdf)
    black_column = find_column(settings.BLACK_POPULATION_POSSIBLE_COLUMNS, demands_gdf)
    brown_column = find_column(settings.BROWN_POPULATION_POSSIBLE_COLUMNS, demands_gdf)
    indigenous_column = find_column(settings.INDIGENOUS_POPULATION_POSSIBLE_COLUMNS, demands_gdf)
    yellow_column = find_column(settings.YELLOW_POPULATION_POSSIBLE_COLUMNS, demands_gdf)
    sector_column = find_column(settings.DEMAND_ID_POSSIBLE_COLUMNS, demands_gdf)

    if not pop_column:
        logger.error("Coluna de população não encontrada.")
        raise ValueError("Nenhuma coluna correspondente à população encontrada.")

    total_people_city = demands_gdf.drop_duplicates(subset=[sector_column])[pop_column].sum()
    logger.info("População total da cidade calculada: %s", total_people_city)

    total_people_negros = demands_gdf.drop_duplicates(subset=[sector_column])[black_column].sum() if black_column else 0
    total_people_pardos = demands_gdf.drop_duplicates(subset=[sector_column])[brown_column].sum() if brown_column else 0
    total_people_indigenas = demands_gdf.drop_duplicates(subset=[sector_column])[indigenous_column].sum() if indigenous_column else 0
    total_people_amarela = demands_gdf.drop_duplicates(subset=[sector_column])[yellow_column].sum() if yellow_column else 0

    logger.info("Extraindo coordenadas dos pontos de demandas e estabelecimentos.")
    demands_points = list(zip(demands_gdf.geometry.x, demands_gdf.geometry.y))
    establishments_points = list(zip(establishments_gdf.geometry.x, establishments_gdf.geometry.y))

    logger.info("Construindo árvore cKDTree para encontrar o vizinho mais próximo.")
    tree = cKDTree(establishments_points)
    distances, indices = tree.query(demands_points, k=1)

    total_ubs = len(establishments_gdf)
    ubs_per_1000 = (total_ubs / total_people_city) * 1000 if total_people_city > 0 else 0
    logger.info("Total de UBS: %d, UBS por 1000 habitantes: %.2f", total_ubs, ubs_per_1000)

    if ubs_per_1000 >= INTERVALO_MAX:
        ubs_situation = "Suficiente"
    elif ubs_per_1000 >= INTERVALO_MIN:
        ubs_situation = "Intermediário"
    else:
        ubs_situation = "Deficitário"

    allocation = {}

    logger.info("Inferindo colunas para identificação de estabelecimentos.")
    cnes_column = find_column(settings.ESTABLISHMENT_ID_POSSIBLE_COLUMNS, establishments_gdf)
    city_column = find_column(settings.CITY_POSSIBLE_COLUMNS, establishments_gdf)
    name_column = find_column(settings.NAME_POSSIBLE_COLUMNS, establishments_gdf)

    if not cnes_column or not city_column or not name_column:
        logger.error("Colunas essenciais para estabelecimento não encontradas.")
        raise ValueError("Colunas essenciais para estabelecimento não encontradas.")

    logger.info("Iniciando alocação de demandas para cada estabelecimento.")
    for i, (demand, dist, idx) in enumerate(zip(demands_gdf.itertuples(), distances, indices)): 
        est = establishments_gdf.iloc[idx]
        mean_distance = np.mean(dist)
    
        if mean_distance <= 700:
            radius = 'Ótima (700m)'
        elif mean_distance <= 1000:
            radius = 'Boa (1000m)'
        elif mean_distance <= 2000:
            radius = 'Regular (2000m)'
        else:
            radius = 'Ruim (>2000m)'
        
        logger.debug("Demanda %d: distância média = %.2f, categoria = %s", i, mean_distance, radius)
        sectors_within_range = demands_gdf[demands_gdf.geometry.distance(est.geometry) <= mean_distance]
        sectors_within_range = sectors_within_range.drop_duplicates(subset=['CD_SETOR'])
        
        total_people_ubs = sectors_within_range[pop_column].sum()
        percentage_ubs = (total_people_ubs / total_people_city) * 100 if total_people_city > 0 else 0
    
        total_negros = sectors_within_range[black_column].sum() if black_column else 0
        percentage_ubs_negros = (total_negros / total_people_negros) * 100 if total_people_negros > 0 else 0

        total_pardos = sectors_within_range[brown_column].sum() if brown_column else 0
        percentage_ubs_pardos = (total_pardos / total_people_pardos) * 100 if total_people_pardos > 0 else 0
    
        total_indigenas = sectors_within_range[indigenous_column].sum() if indigenous_column else 0
        percentage_ubs_indigenas = (total_indigenas / total_people_indigenas) * 100 if total_people_indigenas > 0 else 0
    
        total_amarela = sectors_within_range[yellow_column].sum() if yellow_column else 0
        percentage_ubs_amarela = (total_amarela / total_people_amarela) * 100 if total_people_amarela > 0 else 0

        allocation[est[cnes_column]] = {
            'Establishment': est[city_column],
            'UBS_Name': est[name_column],
            'Radius': radius,
            'Mean_Distance': mean_distance,
            'Total_People': total_people_ubs,
            'Total_People_Negros': total_negros,
            'Total_People_Pardos': total_pardos,
            'Total_People_Indigenas': total_indigenas,
            'Total_People_Amarela': total_people_amarela,
            'Percentage_City': percentage_ubs,
            'Percentage_Negros': percentage_ubs_negros,
            'Percentage_Pardos': percentage_ubs_pardos,
            'Percentage_Indigenas': percentage_ubs_indigenas,
            'Percentage_Amarela': percentage_ubs_amarela,
            'Sector_Codes': sectors_within_range['CD_SETOR'].tolist()
        }
    logger.info("Alocação de demandas concluída para %d UBS.", len(allocation))

    summary_data = {
        "Total_City_Population": total_people_city,
        "Total_UBS": total_ubs,
        "UBS_per_1000": ubs_per_1000,
        "UBS_Situation": ubs_situation
    }
    
    logger.info("Resumo calculado: %s", summary_data)
    return allocation, summary_data

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
