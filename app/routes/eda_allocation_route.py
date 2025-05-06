import logging
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from io import BytesIO
import zipfile
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from app.config import settings
from pandas.plotting import table
import numpy as np
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator
import textwrap

logger = logging.getLogger(__name__)
router = APIRouter()

def analyze_allocation(allocation_df: pd.DataFrame, demanda_gdf: gpd.GeoDataFrame):
    logger.info("Iniciando analyze_allocation: convertendo chaves para string.")
    allocation_df["demand_id"] = allocation_df["demand_id"].astype(str)
    demanda_gdf["CD_SETOR"] = demanda_gdf["CD_SETOR"].astype(str)
    
    logger.info("Fazendo merge do allocation_df com demanda_gdf (chaves: demand_id <-> CD_SETOR).")
    merged_df = allocation_df.merge(
        demanda_gdf, left_on="demand_id", right_on="CD_SETOR", how="left"
    )
    logger.info("Merge concluído. merged_df shape: %s", merged_df.shape)
    
    # Evitar problemas de serialização com geometria
    if "geometry" in merged_df.columns:
        logger.info("Convertendo geometria para WKT para evitar recursion issues.")
        merged_df["geometry"] = merged_df["geometry"].apply(lambda geom: geom.wkt if geom is not None else None)
    
    logger.info("Agrupando dados por 'opportunity_name' e calculando estatísticas.")

    merged_df["total_alfabetizados"] = merged_df[
    ["15 A 19 ANOS, ALFABETIZADAS",
     "20 A 24 ANOS, ALFABETIZADAS",
     "25 A 29 ANOS, ALFABETIZADAS",
     "30 A 34 ANOS, ALFABETIZADAS",
     "35 A 39 ANOS, ALFABETIZADAS",
     "40 A 44 ANOS, ALFABETIZADAS",
     "45 A 49 ANOS, ALFABETIZADAS",
     "50 A 54 ANOS, ALFABETIZADAS",
     "55 A 59 ANOS, ALFABETIZADAS",
     "60 A 64 ANOS, ALFABETIZADAS",
     "65 A 69 ANOS, ALFABETIZADAS",
     "70 A 79 ANOS, ALFABETIZADAS",
     "80 ANOS OU MAIS, ALFABETIZADAS"]].sum(axis=1)

    group = merged_df.groupby("opportunity_name")
    summary = group.agg(
         total_demands = ("demand_id", "count"),
         avg_distance = ("distance_km", "mean"),
         total_population = ("DEMANDA", "sum"),
         total_negros = ("RAÇA NEGRA TOTAL", "sum"),
         total_pardos = ("RAÇA PARDA TOTAL", "sum"),
         total_indigenas = ("RAÇA INDÍGENA TOTAL", "sum"),
         total_amarela = ("RAÇA AMARELA TOTAL", "sum"),
         total_15_19=("15-19 ANOS", "sum"),
         total_20_24=("20-24 ANOS", "sum"),
         total_25_29=("25-29 ANOS", "sum"),
         total_30_34=("30-34 ANOS", "sum"),
         total_35_39=("35-39 ANOS", "sum"),
         total_40_44=("40-44 ANOS", "sum"),
         total_45_49=("45-49 ANOS", "sum"),
         total_50_54=("50-54 ANOS", "sum"),
         total_55_59=("55-59 ANOS", "sum"),
         total_60_64=("60-64 ANOS", "sum"),
         total_65_69=("65-69 ANOS", "sum"),
         total_70_79=("70-79 ANOS", "sum"),
         total_80_mais=("80 ANOS OU MAIS", "sum"),
         total_alfabetizados=("total_alfabetizados", "sum")

    ).reset_index()

    
    logger.debug("Resumo de estatísticas (primeiras linhas):\n%s", summary.head())
    
    # Calcula percentuais de cada grupo, se houver população registrada
    logger.info("Calculando percentuais de grupos raciais.")
    summary["pct_negros"] = summary["total_negros"] / summary["total_population"] * 100
    summary["pct_pardos"] = summary["total_pardos"] / summary["total_population"] * 100
    summary["pct_indigenas"] = summary["total_indigenas"] / summary["total_population"] * 100
    summary["pct_amarela"] = summary["total_amarela"] / summary["total_population"] * 100
    summary["pessoas_analfabetas"] = summary['total_population']-summary["total_alfabetizados"]
       
    logger.info("Criando colunas agregadas de faixa etária.")
    summary["total_15_29_anos"] = (summary["total_15_19"] +summary["total_20_24"] +summary["total_25_29"])
    summary["total_30_49_anos"] = (summary["total_30_34"] +summary["total_35_39"] +summary["total_40_44"] +summary["total_45_49"])
    summary["total_50_64_anos"] = (summary["total_50_54"] +summary["total_55_59"] +summary["total_60_64"])
    summary["total_65_mais_anos"] = (summary["total_65_69"] +summary["total_70_79"] +summary["total_80_mais"])
    summary['city_name'] = merged_df['NM_MUN']
    
    logger.info("analyze_allocation concluído.")
    return merged_df, summary

def create_allocation_charts(summary: pd.DataFrame):
    logger.info("Iniciando criação dos gráficos de alocação.")
    
    # Gráfico 1: Top 10 Oportunidades por População Atendida
    fig1, ax1 = plt.subplots(figsize=(14, 8))
    top_opp = summary.sort_values(by="total_population", ascending=False).head(10)
    
    bars1 = ax1.barh(
        top_opp["opportunity_name"], 
        top_opp["total_population"], 
        color="#4C72B0"
    )

    ax1.set_title("Top 10 UBS por População Atendida", fontsize=18, fontweight='bold')
    ax1.set_xlabel("População Atendida", fontsize=14)
    ax1.set_ylabel("UBS", fontsize=14)
    ax1.tick_params(axis='both', labelsize=12)
    ax1.invert_yaxis()

    # Adiciona rótulos nas barras
    for bar in bars1:
        width = bar.get_width()
        ax1.annotate(f'{int(width)}', xy=(width, bar.get_y() + bar.get_height() / 2),
                     xytext=(5, 0), textcoords="offset points", ha='left', va='center', fontsize=12)

    buf1 = BytesIO()
    plt.tight_layout()
    plt.savefig(buf1, format="png")
    buf1.seek(0)
    plt.close(fig1)
    logger.info("Gráfico 1 (População) criado com sucesso.")

    # Gráfico 2: Média da Composição Racial
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    avg_pct = summary[["pct_negros", "pct_pardos", "pct_indigenas", "pct_amarela"]].mean()

    bars2 = ax2.bar(
        ["Negros", "Pardos", "Indígenas", "Amarela"], 
        avg_pct, 
        color=["#5D3A9B", "#B07AA1", "#4C9A2A", "#F9C846"]
    )

    ax2.set_title("Média da Composição Racial (%)", fontsize=18, fontweight='bold')
    ax2.set_ylabel("Porcentagem (%)", fontsize=14)
    ax2.set_xlabel("Grupo Racial", fontsize=14)
    ax2.tick_params(axis='y', labelsize=12)

    # Adiciona rótulos nas barras
    for bar in bars2:
        height = bar.get_height()
        ax2.annotate(f'{height:.1f}%', xy=(bar.get_x() + bar.get_width() / 2, height),
                     xytext=(0, 3), textcoords="offset points", ha='center', fontsize=12)

    buf2 = BytesIO()
    plt.tight_layout()
    plt.savefig(buf2, format="png")
    buf2.seek(0)
    plt.close(fig2)
    logger.info("Gráfico 2 (Composição Racial) criado com sucesso.")

    logger.info("Criação de gráficos concluída.")
    return buf1, buf2

def create_coverage_stats(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Exemplo de estatísticas adicionais sobre a cobertura, se desejar.
    Agrupa por 'opportunity_name' e gera min, max, etc. da 'distance_km'.
    """
    logger.info("Gerando estatísticas de cobertura (coverage_stats).")
    if "distance_km" not in merged_df.columns:
        logger.warning("Não há coluna 'distance_km' no merged_df; coverage_stats será vazio.")
        return pd.DataFrame()

    group_opp = merged_df.groupby("opportunity_name")

    coverage_stats = group_opp.agg(
        total_demands=("demand_id", "count"),
        avg_distance=("distance_km", "mean"),
        max_distance=("distance_km", "max"),
        min_distance=("distance_km", "min")
    ).reset_index()

    logger.debug("coverage_stats:\n%s", coverage_stats.head())
    return coverage_stats

def create_distance_boxplot(merged_df: pd.DataFrame):
    """
    Cria um boxplot de 'distance_km' para visualizar a dispersão e possíveis outliers nas distâncias.
    """
    logger.info("Gerando boxplot de 'distance_km'.")
    if "distance_km" not in merged_df.columns:
        logger.warning("Não há coluna 'distance_km' no merged_df; não será gerado boxplot.")
        return None

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.boxplot(merged_df["distance_km"].dropna(), vert=False)
    ax.set_title("Boxplot das Distâncias (km)")
    ax.set_xlabel("Distância (km)")
    buf = BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close(fig)
    return buf




def create_distance_hist(merged_df: pd.DataFrame) -> BytesIO:
    """
    Cria um histograma da coluna 'distance_km' utilizando apenas Matplotlib.

    Args:
        merged_df (pd.DataFrame): DataFrame contendo a coluna 'distance_km'.

    Returns:
        BytesIO: Imagem do gráfico em buffer (PNG) para exportação ou exibição.
    """
    logger.info("Gerando histograma de 'distance_km' com Matplotlib.")

    if "distance_km" not in merged_df.columns:
        logger.warning("Não há coluna 'distance_km' no merged_df; histograma não será gerado.")
        return None

    fig, ax = plt.subplots(figsize=(10, 6))

    # Histograma
    counts, bins, patches = ax.hist(
        merged_df["distance_km"], 
        bins=30, 
        color="#4C72B0", 
        edgecolor="black", 
        alpha=0.7
    )

    # Curva de densidade simples
    bin_centers = 0.5 * (bins[1:] + bins[:-1])
    density = counts / sum(counts)

    ax.plot(bin_centers, density * max(counts), color="darkred", linewidth=2, label="Estimativa de Densidade")

    ax.set_title(f"Distribuição das Distâncias até UBS\nTotal de Registros: {len(merged_df)}", fontsize=14, weight="bold")
    ax.set_xlabel("Distância até UBS (km)", fontsize=12)
    ax.set_ylabel("Frequência", fontsize=12)
    ax.legend()
    ax.grid(visible=True, linestyle='--', alpha=0.5)

    ax.xaxis.set_major_locator(MultipleLocator(5))

    buf = BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png", dpi=300)
    buf.seek(0)
    plt.close(fig)

    return buf

import textwrap

def gerar_perguntas_respostas(summary: pd.DataFrame, merged_df: pd.DataFrame):
    perguntas_respostas = []

    perguntas_respostas.append(
        "Este relatório é guiado por um conjunto de perguntas-problema que "
        "orientam a análise socioeconômica e espacial do atendimento por UBS.\n"
        "A seguir, estão listadas as principais questões acompanhadas de suas respectivas "
        "respostas com base nos dados analisados."
    )
    perguntas_respostas.append("\n")

    # 1. Capacidade Instalada vs. Demanda Alocada
    perguntas_respostas.append("1. Capacidade Instalada vs. Demanda Alocada\n")

    top_ubs = summary.sort_values(by="total_population", ascending=False).head(3)
    perguntas_respostas.append("   - Quais são as UBS mais sobrecarregadas?")
    top_ubs_list = ', '.join(top_ubs['opportunity_name'])
    perguntas_respostas.extend(textwrap.wrap(
        f"     As UBS com maior população alocada são: {top_ubs_list}.", width=100
    ))

    media_atendimento = summary["total_population"].mean()
    criticas = summary[summary["total_population"] > media_atendimento * 1.5]
    perguntas_respostas.append("   - Existem UBS operando em situação crítica de capacidade?")
    if not criticas.empty:
        criticas_list = ', '.join(criticas['opportunity_name'])
        perguntas_respostas.extend(textwrap.wrap(
            f"     Sim, as UBS {criticas_list} atendem mais de 50% acima da média esperada.", width=100
        ))
    else:
        perguntas_respostas.append("     Não há UBS com carga crítica detectada acima de 50% da média de atendimento.")

    subutilizadas = summary[summary["total_population"] < media_atendimento * 0.5]
    perguntas_respostas.append("   - Existem UBS com subutilização em relação à sua capacidade?")
    if not subutilizadas.empty:
        subutilizadas_list = ', '.join(subutilizadas['opportunity_name'])
        perguntas_respostas.extend(textwrap.wrap(
            f"     Sim, as UBS {subutilizadas_list} estão atendendo menos de 50% da média populacional.", width=100
        ))
    else:
        perguntas_respostas.append("     Todas as UBS estão operando acima de 50% da média populacional.")

    perguntas_respostas.append("\n")

    # 2. Perfil Socioeconômico da População
    perguntas_respostas.append("2. Perfil Socioeconômico da População\n")

    vulneraveis = summary[
        summary["pct_negros"] + summary["pct_pardos"] + summary["pct_indigenas"] > 80
    ]
    perguntas_respostas.append("   - Quais UBS estão localizadas em áreas com altos índices de vulnerabilidade social?")
    if not vulneraveis.empty:
        vulneraveis_list = ', '.join(vulneraveis['opportunity_name'])
        perguntas_respostas.extend(textwrap.wrap(
            f"     As UBS em áreas com alta presença de grupos racialmente vulneráveis são: {vulneraveis_list}.", width=100
        ))
    else:
        perguntas_respostas.append("     Não foram identificadas UBS com vulnerabilidade racial superior a 80%.")

    perguntas_respostas.append("   - Qual é a composição etária e racial da população atendida por cada UBS?")
    perguntas_respostas.append("     A composição é detalhada na seção 'Detalhamento por UBS' deste relatório.")

    perguntas_respostas.append("   - Existem populações marginalizadas (preta, parda, indígena) com acesso desigual?")
    if not vulneraveis.empty:
        perguntas_respostas.append(
            "     Sim, há concentração de grupos vulneráveis em algumas UBS. Isso exige atenção para evitar desigualdade no acesso."
        )
    else:
        perguntas_respostas.append("     Os dados não indicam concentração crítica de populações marginalizadas.")

    perguntas_respostas.append("\n")

    # 3. Acessibilidade e Proximidade
    perguntas_respostas.append("3. Acessibilidade e Proximidade\n")

    avg_distances = summary["avg_distance"]
    perguntas_respostas.append("   - Qual é a distância média entre cada região e a UBS à qual foi alocada?")
    perguntas_respostas.append(f"     A distância média geral é de {avg_distances.mean():.2f} km.")

    perguntas_respostas.append("   - Há regiões com distância excessiva da UBS mais próxima?")
    acima_limite = merged_df[merged_df["distance_km"] > 4]
    if not acima_limite.empty:
        perc = 100 * len(acima_limite) / len(merged_df)
        perguntas_respostas.append(
            f"     Sim, {len(acima_limite)} pessoas ({perc:.2f}%) estão a mais de 4 km da UBS alocada."
        )
    else:
        perguntas_respostas.append("     Não há pessoas alocadas a UBS acima de 4 km.")

    perguntas_respostas.append("   - Qual a dispersão das distâncias dentro da área de cobertura de cada UBS?")
    perguntas_respostas.append(
        f"     O desvio padrão médio da distância por UBS é de {summary['avg_distance'].std():.2f} km."
    )

    perguntas_respostas.append("   - Quantas pessoas estão alocadas a UBS fora do raio ideal de cobertura (ex: acima de 4km)?")
    perguntas_respostas.append(
        f"     Total de pessoas fora do raio ideal de cobertura: {len(acima_limite)}."
    )

    return perguntas_respostas




def generate_allocation_pdf(summary: pd.DataFrame, merged_df: pd.DataFrame):
    logger.info("Iniciando geração do PDF de relatório.")
    pdf_buffer = BytesIO()
    c = canvas.Canvas(pdf_buffer, pagesize=letter)
    width, height = letter

    # Constantes de layout
    margin = 50
    line_height = 15
    block_spacing = 30
    min_y_threshold = 100

    def check_page_space(c, y, needed_space=100):
        if y - needed_space < min_y_threshold:
            c.showPage()
            c.setFont("Helvetica", 12)
            return height - margin
        return y

    # Dados Gerais
    city_name = summary['city_name'].iloc[0]
    total_population = summary['total_population'].sum()
    total_ubs = summary['opportunity_name'].nunique()

    # --- CAPA ---
    c.setFont("Helvetica-Bold", 20)
    c.drawString(margin, height - 100, "Relatório de Análise Socioeconômica")
    c.line(margin, height - 110, width - margin, height - 110)
    c.setFont("Helvetica", 12)
    c.drawString(margin, height - 140, "Sumário:")
    sumario_itens = ["1. Visão Geral", "2. Detalhamento por UBS", "3. Perguntas-Guia", "4. Gráficos e Visualizações"]
    y = height - 160
    for item in sumario_itens:
        c.drawString(margin + 20, y, item)
        y -= line_height
    c.showPage()

    # --- VISÃO GERAL ---
    c.setFont("Helvetica-Bold", 16)
    c.drawString(margin, height - 50, "1. Visão Geral")
    c.setFont("Helvetica", 12)
    y = height - 80

    textos_visao_geral = [
        f"Cidade: {city_name}",
        f"População Total: {total_population}",
        f"Total de UBS: {total_ubs}",
        f"",  # espaço extra
        f"Este relatório apresenta uma análise socioeconômica da cidade {city_name}.",
        f"Considera o atendimento das UBS e os principais indicadores sociais.",
        f"Nas próximas páginas, detalhamos informações por UBS específica."
    ]

    for texto in textos_visao_geral:
        y = check_page_space(c, y)
        c.drawString(margin, y, texto)
        y -= line_height

    c.showPage()

    # --- DETALHAMENTO POR UBS ---
    c.setFont("Helvetica-Bold", 16)
    c.drawString(margin, height - 50, "2. Detalhamento por UBS")
    c.setFont("Helvetica", 12)
    y = height - 80

    for _, row in summary.iterrows():
        detalhes = [
            f"Oportunidade: {row['opportunity_name']}",
            f"População Atendida: {row['total_population']:.0f}",
            f"Distância Média: {row['avg_distance']:.2f} km",
            f"Negros: {row['pct_negros']:.2f}%, Pardos: {row['pct_pardos']:.2f}%, ",
            f"Indígenas: {row['pct_indigenas']:.2f}%, Amarelas: {row['pct_amarela']:.2f}%",
            f"Pessoas com 15-29 anos: {row['total_15_29_anos']}",
            f"Pessoas com 30-49 anos: {row['total_30_49_anos']}",
            f"Pessoas com 50-64 anos: {row['total_50_64_anos']}",
            f"Pessoas com 65 anos ou mais: {row['total_65_mais_anos']}",
            f"Total Analfabetos: {row['pessoas_analfabetas']}"
        ]

        for info in detalhes:
            y = check_page_space(c, y)
            c.drawString(margin + 20, y, info)
            y -= line_height

        y -= block_spacing  # Espaço entre UBS

    c.showPage()

    # --- PERGUNTAS-GUIA ---
    c.setFont("Helvetica-Bold", 16)
    c.drawString(margin, height - 50, "3. Perguntas-Guia da Análise com Respostas")
    c.setFont("Helvetica", 11)
    y = height - 80

    perguntas_respostas = gerar_perguntas_respostas(summary, merged_df)
    for linha in perguntas_respostas:
        y = check_page_space(c, y)
        c.drawString(margin, y, linha)
        y -= line_height

    c.showPage()

    # --- GRÁFICOS ---
    c.setFont("Helvetica-Bold", 16)
    c.drawString(margin, height - 50, "4. Gráficos e Visualizações")
    c.setFont("Helvetica", 12)
    y = height - 80

    # Histograma e Boxplot
    hist_buffer = create_distance_hist(merged_df)
    box_plot = create_distance_boxplot(merged_df)

    if hist_buffer:
        img_hist = ImageReader(hist_buffer)
        y = check_page_space(c, y, 300)
        c.drawImage(img_hist, margin, y - 250, width=500, height=250)
        y -= 300

    if box_plot:
        img_box = ImageReader(box_plot)
        y = check_page_space(c, y, 300)
        c.drawImage(img_box, margin, y - 250, width=500, height=250)
        y -= 300

    c.showPage()

    # Gráfico: Top 10 UBS por população
    buf_populacao, buf_composicao = create_allocation_charts(summary)

    if buf_populacao:
        c.setFont("Helvetica-Bold", 14)
        c.drawString(margin, height - 50, "Top 10 UBS por População Atendida")
        img1 = ImageReader(buf_populacao)
        y = height - 100
        c.drawImage(img1, margin, y - 400, width=500, height=400)
        c.showPage()

    if buf_composicao:
        c.setFont("Helvetica-Bold", 14)
        c.drawString(margin, height - 50, "Média da Composição Racial da População")
        img2 = ImageReader(buf_composicao)
        y = height - 100
        c.drawImage(img2, margin, y - 350, width=500, height=350)
        c.showPage()

    # Finaliza
    c.save()
    pdf_buffer.seek(0)
    logger.info("Relatório PDF gerado com sucesso.")
    return pdf_buffer




def create_summary_table(summary: pd.DataFrame) -> pd.DataFrame:
    logger.info("Iniciando criação da Tabela Resumo de Indicadores.")

    mais_sobrecarregada = summary.loc[summary['total_population'].idxmax()]['opportunity_name']
    ocupacao_max = summary['total_population'].max()

    mais_subutilizada = summary.loc[summary['total_population'].idxmin()]['opportunity_name']
    ocupacao_min = summary['total_population'].min()

    media_ocupacao = summary['total_population'].mean()
    mediana_ocupacao = summary['total_population'].median()
    desvio_padrao_ocupacao = summary['total_population'].std()

    resumo = pd.DataFrame({
        'Indicador': [
            'UBS mais sobrecarregada',
            'UBS mais subutilizada',
            'Média de ocupação',
            'Mediana de ocupação',
            'Desvio-padrão de ocupação'
        ],
        'Valor': [
            f"{mais_sobrecarregada} ({ocupacao_max:.0f} pessoas)",
            f"{mais_subutilizada} ({ocupacao_min:.0f} pessoas)",
            f"{media_ocupacao:.2f}",
            f"{mediana_ocupacao:.2f}",
            f"{desvio_padrao_ocupacao:.2f}"
        ]
    })

    logger.info("Tabela Resumo criada com sucesso.")
    return resumo

def save_summary_table_image(resumo: pd.DataFrame) -> BytesIO:
    logger.info("Iniciando criação da imagem da Tabela Resumo.")

    fig, ax = plt.subplots(figsize=(10, 3))
    ax.axis('off')

    tabela = table(ax, resumo, loc='center', cellLoc='center', colWidths=[0.4, 0.6])
    tabela.auto_set_font_size(False)
    tabela.set_fontsize(12)

    # Estilização
    for key, cell in tabela.get_celld().items():
        cell.set_edgecolor('black')
        cell.set_linewidth(1.2)
        if key[0] == 0:
            cell.set_facecolor('#C4DFDF')
            cell.set_fontsize(14)
            cell.set_text_props(weight='bold')

    plt.tight_layout()

    buf = BytesIO()
    plt.savefig(buf, format="png", bbox_inches='tight', dpi=150)
    buf.seek(0)
    plt.close(fig)

    logger.info("Imagem da Tabela Resumo criada com sucesso.")
    return buf



@router.post("/allocation")
async def eda_allocation_endpoint(
    allocation_file: UploadFile = File(...),
    demanda_file: UploadFile = File(...),
):
    """
    Endpoint que recebe:
      - O arquivo CSV de alocação (output da sua API, contendo os cálculos de distância já realizados,
        incluindo as novas colunas 'Destination_State' e 'Destination_City')
      - O arquivo demanda.geojson (com informações socioeconômicas adicionais, como população, raça, etc.)
    
    Realiza o merge entre os dois arquivos usando 'demand_id' do CSV e 'CD_SETOR' do GeoJSON,
    gera estatísticas, gráficos e um relatório PDF, e retorna todos os resultados em um arquivo ZIP.
    Agora inclui também coverage_stats.csv e distance_hist.png.
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        logger.info("Recebendo arquivo CSV de alocação.")
        allocation_content = await allocation_file.read()
        allocation_df = pd.read_csv(BytesIO(allocation_content))
        logger.info("Arquivo CSV de alocação lido. Linhas: %d, Colunas: %d", allocation_df.shape[0], allocation_df.shape[1])
        
        logger.info("Recebendo arquivo GeoJSON de demandas.")
        demanda_content = await demanda_file.read()
        demanda_gdf = gpd.read_file(BytesIO(demanda_content))
        logger.info("Arquivo GeoJSON de demandas lido. Linhas: %d, Colunas: %d", demanda_gdf.shape[0], len(demanda_gdf.columns))

        logger.info("Iniciando processo de análise e merge.")
        merged_df, summary = analyze_allocation(allocation_df, demanda_gdf)
        logger.info("Análise concluída. Tamanho do DataFrame mesclado: %s, e summary: %s", merged_df.shape, summary.shape)
        
        logger.info("Gerando gráficos (chart_population, chart_racial).")
        chart1_buf, chart2_buf = create_allocation_charts(summary)
        
        logger.info("Gerando relatório PDF.")
        pdf_buf = generate_allocation_pdf(summary, merged_df)

        # (Opcional) Gera estatísticas adicionais de cobertura
        coverage_stats = create_coverage_stats(merged_df)

        # (Opcional) Gera histograma de distâncias
        distance_hist_buf = create_distance_hist(merged_df)

        logger.info("Gerando gráficos gráfico boxplot.")
        
        # Gera um box plot com as ditâncias
        box_plo_distence = create_distance_boxplot(merged_df)
        
        logger.info("Gerando tabela mais descritiva por UBS")
        resumo = create_summary_table(summary)
        table_image = save_summary_table_image(resumo)

        logger.info("Empacotando resultados em um arquivo ZIP.")
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            # 1) JSON com a alocação mesclada
            json_str = merged_df.to_json(orient="records", force_ascii=False)
            zipf.writestr("merged_allocation.json", json_str)

            # 2) CSV com o resumo
            zipf.writestr("summary.csv", summary.to_csv(index=False))

            # 3) PNGs (chart_population, chart_racial)
            zipf.writestr("chart_population.png", chart1_buf.getvalue())
            zipf.writestr("chart_racial.png", chart2_buf.getvalue())

            logger.info('Table gerada')
            zipf.writestr("table_image.png", table_image.getvalue())

            # 4) PDF
            zipf.writestr("report.pdf", pdf_buf.getvalue())

            # 5) coverage_stats.csv (se coverage_stats não estiver vazio)
            if not coverage_stats.empty:
                coverage_csv = coverage_stats.to_csv(index=False)
                zipf.writestr("coverage_stats.csv", coverage_csv)

            # 6) distance_hist.png (se gerado)
            if distance_hist_buf:
                zipf.writestr("distance_hist.png", distance_hist_buf.getvalue())

            # 6) distance_hist.png (se gerado)
            if box_plo_distence:
                zipf.writestr("distance_hist.png", box_plo_distence.getvalue())

        zip_buffer.seek(0)
        logger.info("Processo finalizado com sucesso. Retornando arquivo ZIP.")
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=eda_allocation_results.zip"}
        )
    except Exception as e:
        logger.exception("Erro ao processar os arquivos na rota EDA allocation.")
        raise HTTPException(status_code=400, detail=f"Erro ao processar os arquivos: {e}")
