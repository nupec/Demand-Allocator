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
    group = merged_df.groupby("opportunity_name")
    summary = group.agg(
         total_demands = ("demand_id", "count"),
         avg_distance = ("distance_km", "mean"),
         total_population = ("DEMANDA", "sum"),
         total_negros = ("RAÇA NEGRA TOTAL", "sum"),
         total_pardos = ("RAÇA PARDA TOTAL", "sum"),
         total_indigenas = ("RAÇA INDÍGENA TOTAL", "sum"),
         total_amarela = ("RAÇA AMARELA TOTAL", "sum")
    ).reset_index()
    
    logger.debug("Resumo de estatísticas (primeiras linhas):\n%s", summary.head())
    
    # Calcula percentuais de cada grupo, se houver população registrada
    logger.info("Calculando percentuais de grupos raciais.")
    summary["pct_negros"] = summary["total_negros"] / summary["total_population"] * 100
    summary["pct_pardos"] = summary["total_pardos"] / summary["total_population"] * 100
    summary["pct_indigenas"] = summary["total_indigenas"] / summary["total_population"] * 100
    summary["pct_amarela"] = summary["total_amarela"] / summary["total_population"] * 100
    
    logger.info("analyze_allocation concluído.")
    return merged_df, summary

def create_allocation_charts(summary: pd.DataFrame):
    logger.info("Iniciando criação dos gráficos de alocação.")
    
    # Gráfico 1: Top 10 oportunidades por população atendida
    fig1, ax1 = plt.subplots(figsize=(14, 8))
    top_opp = summary.sort_values(by="total_population", ascending=False).head(10)
    ax1.barh(top_opp["opportunity_name"], top_opp["total_population"], color="blue")

    ax1.set_title("Top 10 Oportunidades por População Atendida", fontsize=16)
    ax1.set_xlabel("População", fontsize=14)
    ax1.set_ylabel("Oportunidade", fontsize=14)
    ax1.tick_params(axis='both', labelsize=12)
    ax1.invert_yaxis()

    buf1 = BytesIO()
    plt.tight_layout()
    plt.savefig(buf1, format="png")
    buf1.seek(0)
    plt.close(fig1)
    logger.info("Gráfico 1 (População) criado com sucesso.")

    # Gráfico 2: Média da composição racial
    fig2, ax2 = plt.subplots(figsize=(14, 8))
    avg_pct = summary[["pct_negros", "pct_pardos", "pct_indigenas", "pct_amarela"]].mean()
    avg_pct.plot(kind="bar", ax=ax2, color=["brown", "orange", "purple", "yellow"])

    ax2.set_title("Média de Composição Racial (%)", fontsize=16)
    ax2.set_ylabel("Porcentagem (%)", fontsize=14)
    ax2.set_xticklabels(["Negros", "Pardos", "Indígenas", "Amarela"], rotation=0, fontsize=12)
    ax2.tick_params(axis='y', labelsize=12)
    plt.tight_layout()
    buf2 = BytesIO()
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

def create_distance_hist(merged_df: pd.DataFrame):
    """
    Cria um histograma de 'distance_km' para mostrar a distribuição de distâncias.
    """
    logger.info("Gerando histograma de 'distance_km'.")
    if "distance_km" not in merged_df.columns:
        logger.warning("Não há coluna 'distance_km' no merged_df; não será gerado histograma.")
        return None

    fig, ax = plt.subplots(figsize=(8, 6))
    merged_df["distance_km"].hist(bins=30, ax=ax)
    ax.set_title("Distribuição de Distâncias (km)")
    ax.set_xlabel("Distância (km)")
    ax.set_ylabel("Frequência")
    buf = BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close(fig)
    return buf

def generate_allocation_pdf(summary: pd.DataFrame):
    logger.info("Iniciando geração do PDF de relatório.")
    pdf_buffer = BytesIO()
    c = canvas.Canvas(pdf_buffer, pagesize=letter)
    width, height = letter
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 50, "Relatório de Análise Socioeconômica")
    c.setFont("Helvetica", 12)
    y = height - 80

    for _, row in summary.iterrows():
        c.drawString(50, y, f"Oportunidade: {row['opportunity_name']}")
        c.drawString(70, y-15, f"População Atendida: {row['total_population']:.0f}")
        c.drawString(70, y-30, f"Distância Média: {row['avg_distance']:.2f} km")
        c.drawString(70, y-45, f"Negros: {row['pct_negros']:.2f}%, Pardos: {row['pct_pardos']:.2f}%, "
                    f"Indígenas: {row['pct_indigenas']:.2f}%, Amarelas: {row['pct_amarela']:.2f}%")
        y -= 70
        if y < 100:
            c.showPage()
            c.setFont("Helvetica", 12)
            y = height - 50

    c.save()
    pdf_buffer.seek(0)
    logger.info("Relatório PDF gerado com sucesso.")
    return pdf_buffer

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
        pdf_buf = generate_allocation_pdf(summary)

        # (Opcional) Gera estatísticas adicionais de cobertura
        coverage_stats = create_coverage_stats(merged_df)

        # (Opcional) Gera histograma de distâncias
        distance_hist_buf = create_distance_hist(merged_df)
        
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

            # 4) PDF
            zipf.writestr("report.pdf", pdf_buf.getvalue())

            # 5) coverage_stats.csv (se coverage_stats não estiver vazio)
            if not coverage_stats.empty:
                coverage_csv = coverage_stats.to_csv(index=False)
                zipf.writestr("coverage_stats.csv", coverage_csv)

            # 6) distance_hist.png (se gerado)
            if distance_hist_buf:
                zipf.writestr("distance_hist.png", distance_hist_buf.getvalue())
        
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
