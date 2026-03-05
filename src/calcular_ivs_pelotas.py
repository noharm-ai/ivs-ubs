"""
calcular_ivs_pelotas.py
=======================
Agrega dados do Censo IBGE 2022 (por setor censitário) para os 55 territórios
Voronoi das UBS de Pelotas.

Saídas:
    ivs_pelotas/data/processed/ibge_por_ubs.csv   — indicadores IBGE por UBS
    ivs_pelotas/data/processed/ivs_pelotas.csv    — IVS final por UBS

Uso:
    python src/calcular_ivs_pelotas.py
"""
from __future__ import annotations

import logging
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------------------

BASE = Path(__file__).resolve().parents[1] / "ivs_pelotas"
RAW = BASE / "data" / "raw"
PROC = BASE / "data" / "processed"
PROC.mkdir(parents=True, exist_ok=True)

IBGE_DIR = RAW / "ibge_universo"
SETOR_GEO = RAW / "ibge_setores" / "setores_pelotas.geojson"
VORONOI = PROC / "territorios_voronoi_ubs.geojson"

CRS_GEO = "EPSG:4674"
CRS_UTM = "EPSG:32722"   # UTM 22S — cálculo de áreas

# ---------------------------------------------------------------------------
# Variáveis IBGE usadas (com descrições para referência)
# ---------------------------------------------------------------------------
#
# basico        (CD_SETOR):
#   v0001  = total moradores do setor
#
# domicilio     (CD_setor = dom1):
#   V00001 = domicílios particulares permanentes ocupados (DPPO)
#
# domicilio2    (CD_setor):
#   V00309 = DPPO c/ esgotamento via rede geral/pluvial     (saneamento adequado)
#   V00310 = DPPO c/ fossa séptica ligada à rede            (saneamento adequado)
#   V00397 = DPPO c/ lixo coletado por serviço de limpeza   (coleta adequada)
#   V00398 = DPPO c/ lixo depositado em caçamba de limpeza  (coleta adequada)
#
# alfabetizacao (CD_setor):
#   V00900 = pessoas 15+ que sabem ler e escrever
#   V00901 = pessoas 15+ que NÃO sabem ler e escrever
#
# pessoa01/demografia (CD_setor):
#   V01006 = total moradores
#   V01022 = feminino, 10-14 anos
#   V01023 = feminino, 15-19 anos
#   V01024 = feminino, 20-24 anos
#   V01025 = feminino, 25-29 anos
#   V01026 = feminino, 30-39 anos
#   V01027 = feminino, 40-49 anos
#   V01031 = total, 0-4 anos   (proxy para <1 ano)
#   V01033 = total, 10-14 anos
#   V01034 = total, 15-19 anos
#   V01040 = total, 60-69 anos
#   V01041 = total, 70+ anos
#
# cor_raca (CD_setor):
#   V01317 = branca
#   V01318 = preta
#   V01319 = amarela
#   V01320 = parda
#   V01321 = indígena

DOM1_COLS  = ["CD_setor", "V00001"]
DOM2_COLS  = ["CD_setor", "V00309", "V00310", "V00397", "V00398"]
ALFA_COLS  = ["CD_setor", "V00900", "V00901"]
PES_COLS   = ["CD_setor", "V01006",
              "V01022", "V01023", "V01024", "V01025", "V01026", "V01027",
              "V01031", "V01033", "V01034", "V01040", "V01041"]
COR_COLS   = ["CD_setor", "V01318", "V01320"]  # preta + parda
BAS_COLS   = ["CD_SETOR", "v0001"]


def _read_ibge(path: Path, cols: list[str], na_values=("X",)) -> pd.DataFrame:
    """Lê CSV IBGE filtrando apenas as colunas necessárias."""
    df = pd.read_csv(path, na_values=list(na_values), low_memory=False, dtype=str)
    # normalizar nome da coluna setor
    setor_orig = next(
        (c for c in df.columns if c.lower() in ("cd_setor", "setor")),
        df.columns[0],
    )
    df = df.rename(columns={setor_orig: "CD_setor"})
    # Selecionar colunas disponíveis (após renomear setor_col, CD_setor já não aparece como coluna extra)
    available = [c for c in cols if c in df.columns]
    missing = [c for c in cols if c not in df.columns and c not in ("CD_setor", "CD_SETOR", "setor")]
    if missing:
        log.warning("  %s: colunas ausentes %s", path.name, missing)
    df = df[["CD_setor"] + [c for c in available if c != "CD_setor"]].copy()
    # Converter para numérico
    for c in df.columns:
        if c != "CD_setor":
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def carregar_setores_com_dados() -> gpd.GeoDataFrame:
    """
    Carrega setores censitários de Pelotas + junta todos os dados IBGE.
    Retorna GeoDataFrame com geometria e colunas numéricas.
    """
    log.info("Carregando setores censitários: %s", SETOR_GEO.name)
    setores = gpd.read_file(SETOR_GEO)
    if setores.crs is None:
        setores = setores.set_crs(CRS_GEO)
    elif setores.crs.to_epsg() != 4674:
        setores = setores.to_crs(CRS_GEO)
    log.info("  %d setores carregados", len(setores))

    # Normalizar coluna CD_SETOR
    setor_col = next(
        (c for c in setores.columns if "CD_SETOR" in c.upper()),
        None,
    )
    if setor_col and setor_col != "CD_SETOR":
        setores = setores.rename(columns={setor_col: "CD_SETOR"})

    setores["CD_SETOR"] = setores["CD_SETOR"].astype(str).str.strip()

    # Carregar arquivos IBGE
    basico = _read_ibge(IBGE_DIR / "pelotas_basico.csv", BAS_COLS)
    basico = basico.rename(columns={"CD_setor": "CD_SETOR", "v0001": "pop_total"})
    basico["CD_SETOR"] = basico["CD_SETOR"].astype(str).str.strip()

    dom1 = _read_ibge(IBGE_DIR / "pelotas_domicilio.csv", DOM1_COLS)
    dom2 = _read_ibge(IBGE_DIR / "pelotas_domicilio2.csv", DOM2_COLS)
    alfa = _read_ibge(IBGE_DIR / "pelotas_alfabetizacao.csv", ALFA_COLS)
    pes  = _read_ibge(IBGE_DIR / "pelotas_pessoa01.csv", PES_COLS)
    cor  = _read_ibge(IBGE_DIR / "pelotas_cor_raca.csv", COR_COLS)

    # Juntar tudo pelo CD_setor
    dfs = [dom1, dom2, alfa, pes, cor]
    dados = dfs[0]
    for df in dfs[1:]:
        dados = dados.merge(df, on="CD_setor", how="outer")

    dados = dados.rename(columns={"CD_setor": "CD_SETOR"})
    dados["CD_SETOR"] = dados["CD_SETOR"].astype(str).str.strip()

    setores = setores.merge(basico, on="CD_SETOR", how="left")
    setores = setores.merge(dados, on="CD_SETOR", how="left")

    log.info("  Setores após join: %d, colunas: %d", len(setores), len(setores.columns))
    return setores


def agregar_por_voronoi(
    setores: gpd.GeoDataFrame,
    territorios: gpd.GeoDataFrame,
    colunas_soma: list[str],
) -> pd.DataFrame:
    """
    Interseção ponderada por área: cada coluna do setor é distribuída
    proporcionalmente à fração de área que intersecta cada território Voronoi.
    """
    log.info("Spatial join setores → territórios Voronoi (ponderado por área)...")

    # Projetar para UTM 22S
    s_utm = setores.to_crs(CRS_UTM)
    t_utm = territorios.to_crs(CRS_UTM)

    s_utm["_area_setor"] = s_utm.geometry.area

    # Interseção
    inter = gpd.overlay(s_utm, t_utm, how="intersection")
    inter["_area_inter"] = inter.geometry.area

    # Mapa área original do setor
    area_map = s_utm.set_index("CD_SETOR")["_area_setor"]
    inter["_area_orig"] = inter["CD_SETOR"].map(area_map)
    inter["_frac"] = (inter["_area_inter"] / inter["_area_orig"].clip(lower=1e-10)).clip(upper=1.0)

    resultado: dict[str, pd.Series] = {}
    for col in colunas_soma:
        if col not in inter.columns:
            log.debug("  coluna %s não encontrada na interseção", col)
            continue
        inter[f"_w_{col}"] = pd.to_numeric(inter[col], errors="coerce") * inter["_frac"]
        resultado[col] = inter.groupby("id_ubs")[f"_w_{col}"].sum()

    df_res = pd.DataFrame(resultado)
    log.info("  Resultado: %d territórios × %d colunas", len(df_res), len(df_res.columns))
    return df_res


def calcular_indicadores(agg: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula indicadores percentuais a partir dos totais agregados.
    Retorna DataFrame com indicadores por UBS.
    """
    ind = pd.DataFrame(index=agg.index)

    def pct(num, denom, fill=np.nan):
        d = agg[denom].replace(0, np.nan) if denom in agg.columns else pd.Series(np.nan, index=agg.index)
        n = agg[num] if num in agg.columns else pd.Series(np.nan, index=agg.index)
        return (n / d * 100).fillna(fill) if fill is not np.nan else (n / d * 100)

    def col(c):
        return agg[c] if c in agg.columns else pd.Series(np.nan, index=agg.index)

    # D1 — Analfabetismo 15+
    ind["D1_analf"] = (
        col("V00901") / (col("V00900") + col("V00901")).replace(0, np.nan) * 100
    )

    # D1 — Negros (preta + parda) / total pop
    ind["D1_negros"] = (
        (col("V01318") + col("V01320")) / col("V01006").replace(0, np.nan) * 100
    )

    # D2 — Sem saneamento adequado (não rede + não fossa ligada à rede)
    sem_saneam = col("V00001") - col("V00309") - col("V00310")
    ind["D2_sem_saneam"] = sem_saneam / col("V00001").replace(0, np.nan) * 100

    # D2 — Sem coleta de lixo (não coletado + não em caçamba)
    sem_lixo = col("V00001") - col("V00397") - col("V00398")
    ind["D2_sem_lixo"] = sem_lixo / col("V00001").replace(0, np.nan) * 100

    # D5 — Crianças < 1 ano: proxy = (0-4 anos) / 5 / total pop
    ind["D5_menor1"] = (col("V01031") / 5) / col("V01006").replace(0, np.nan) * 100

    # D5 — Adolescentes (10-19 anos)
    ind["D5_adol"] = (
        (col("V01033") + col("V01034")) / col("V01006").replace(0, np.nan) * 100
    )

    # D5 — Mulheres em Idade Fértil (10-49 anos, apenas feminino)
    mif = (col("V01022") + col("V01023") + col("V01024") +
           col("V01025") + col("V01026") + col("V01027"))
    ind["D5_mif"] = mif / col("V01006").replace(0, np.nan) * 100

    # D5 — Idosos (60+)
    ind["D5_idosos"] = (
        (col("V01040") + col("V01041")) / col("V01006").replace(0, np.nan) * 100
    )

    # Totais brutos úteis
    ind["pop_total"] = col("pop_total")
    ind["dom_total"] = col("V00001")

    # Clamp: percentuais entre 0 e 100
    pct_cols = [c for c in ind.columns if c not in ("pop_total", "dom_total")]
    ind[pct_cols] = ind[pct_cols].clip(lower=0, upper=100)

    return ind


def main():
    # 1. Carregar Voronoi + normalizar colunas
    log.info("Carregando territórios Voronoi: %s", VORONOI.name)
    territorios = gpd.read_file(VORONOI)
    if territorios.crs is None:
        territorios = territorios.set_crs(CRS_GEO)
    elif territorios.crs.to_epsg() != 4674:
        territorios = territorios.to_crs(CRS_GEO)

    # Garantir coluna id_ubs
    if "id_ubs" not in territorios.columns:
        id_col = next((c for c in territorios.columns if c in ("cnes", "CNES", "co_cnes")), None)
        if id_col:
            territorios["id_ubs"] = territorios[id_col].astype(str)
        else:
            territorios["id_ubs"] = [f"UBS_{i:03d}" for i in range(1, len(territorios) + 1)]

    nome_col = next((c for c in territorios.columns if c in ("nome", "no_ubs", "nome_ubs")), None)
    if nome_col and nome_col != "no_ubs":
        territorios["no_ubs"] = territorios[nome_col]
    elif "no_ubs" not in territorios.columns:
        territorios["no_ubs"] = territorios["id_ubs"]

    log.info("  %d territórios Voronoi", len(territorios))

    # 2. Carregar setores + dados IBGE
    setores = carregar_setores_com_dados()

    # 3. Colunas a agregar (todas as colunas numéricas relevantes)
    cols_numericas = [
        "pop_total", "V00001",
        "V00309", "V00310", "V00397", "V00398",
        "V00900", "V00901",
        "V01006",
        "V01022", "V01023", "V01024", "V01025", "V01026", "V01027",
        "V01031", "V01033", "V01034", "V01040", "V01041",
        "V01318", "V01320",
    ]
    cols_presentes = [c for c in cols_numericas if c in setores.columns]
    log.info("  Colunas para agregar: %d/%d", len(cols_presentes), len(cols_numericas))

    # 4. Spatial join ponderado
    territorios_com_id = territorios[["id_ubs", "no_ubs", "geometry"]].copy()
    agg = agregar_por_voronoi(setores, territorios_com_id, cols_presentes)

    # 5. Calcular indicadores
    log.info("Calculando indicadores por UBS...")
    ind = calcular_indicadores(agg)

    # Adicionar metadados
    meta = territorios.set_index("id_ubs")[["no_ubs"]].copy()
    ind = meta.join(ind, how="right")

    # 6. Salvar IBGE bruto por UBS
    out_raw = PROC / "ibge_por_ubs.csv"
    agg_out = agg.copy()
    agg_out.insert(0, "no_ubs", meta["no_ubs"])
    agg_out.to_csv(out_raw)
    log.info("Totais IBGE por UBS salvos: %s (%d linhas)", out_raw.name, len(agg_out))

    # 7. Salvar indicadores
    out_ind = PROC / "ivs_pelotas.csv"
    ind.to_csv(out_ind)
    log.info("Indicadores IVS por UBS salvos: %s (%d linhas)", out_ind.name, len(ind))

    # 8. Resumo
    print("\n=== RESUMO DOS INDICADORES ===")
    pct_cols = [c for c in ind.columns if c.startswith(("D1_", "D2_", "D4_", "D5_"))]
    if pct_cols:
        print(ind[pct_cols].describe().round(2).to_string())

    return ind


if __name__ == "__main__":
    main()
