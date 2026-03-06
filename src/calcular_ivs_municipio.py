"""
calcular_ivs_municipio.py
=======================
Agrega dados do Censo IBGE 2022 (por setor censitário) para territórios
Voronoi de UBS do município configurado.
"""
from __future__ import annotations

import argparse
import json
import logging
import warnings
from pathlib import Path

import time

import geopandas as gpd
import numpy as np
import pandas as pd
try:
    from tqdm import tqdm as _tqdm
except ImportError:
    def _tqdm(it, **_):  # type: ignore[override]
        return it

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

DEFAULT_BASE = Path(__file__).resolve().parents[1] / "ivs_municipio"
DEFAULT_SLUG = "municipio"

BASE = DEFAULT_BASE
RAW = BASE / "data" / "raw"
PROC = BASE / "data" / "processed"
PROC.mkdir(parents=True, exist_ok=True)

IBGE_DIR = RAW / "ibge_universo"
SETOR_GEO = RAW / "ibge_setores" / "setores_municipio.geojson"
VORONOI = PROC / "territorios_voronoi_ubs.geojson"
OSC_FILE = RAW / "osc_municipio_osm.json"
ESCOLA_FILE: Path | None = None  # set by _configure_runtime
CNPJ_FILE: Path | None = None    # set by _configure_runtime (geocoded)
IVS_OUT_NAME = "ivs_municipio.csv"

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
              "V01031", "V01032", "V01033", "V01034", "V01040", "V01041"]
COR_COLS   = ["CD_setor", "V01318", "V01320"]  # preta + parda
BAS_COLS   = ["CD_SETOR", "v0001"]


def _configure_runtime(base_dir: Path, slug: str) -> None:
    global BASE, RAW, PROC, IBGE_DIR, SETOR_GEO, VORONOI, OSC_FILE, ESCOLA_FILE, CNPJ_FILE, IVS_OUT_NAME
    BASE = base_dir.resolve()
    RAW = BASE / "data" / "raw"
    PROC = BASE / "data" / "processed"
    PROC.mkdir(parents=True, exist_ok=True)
    IBGE_DIR = RAW / "ibge_universo"
    SETOR_GEO = RAW / "ibge_setores" / f"setores_{slug}.geojson"
    VORONOI = PROC / "territorios_voronoi_ubs.geojson"
    OSC_FILE = RAW / f"osc_{slug}_osm.json"
    ESCOLA_FILE = RAW / "censo_escolar" / f"{slug}_escolas_geo.csv"
    CNPJ_FILE = RAW / "cnpj" / f"{slug}_cnpj_osc_geo.csv"
    IVS_OUT_NAME = f"ivs_{slug}.csv"


def _resolve_existing_file(candidates: list[Path], label: str) -> Path:
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"{label} não encontrado. Candidatos: {', '.join(x.name for x in candidates)}")


def _resolve_optional_file(candidates: list[Path]) -> Path | None:
    for p in candidates:
        if p.exists():
            return p
    return None


def _read_ibge_optional(candidates: list[Path], cols: list[str], label: str) -> pd.DataFrame:
    path = _resolve_optional_file(candidates)
    if path is None:
        log.warning("  %s não encontrado; colunas serão NaN", label)
        return pd.DataFrame(columns=cols)
    return _read_ibge(path, cols)


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
    Carrega setores censitários do município alvo + junta os dados IBGE.
    Retorna GeoDataFrame com geometria e colunas numéricas.
    """
    setor_geo = _resolve_existing_file(
        [
            SETOR_GEO,
            RAW / "ibge_setores" / "setores_municipio.geojson",
            RAW / "ibge_setores" / "setores.geojson",
        ],
        "GeoJSON de setores",
    )
    log.info("Carregando setores censitários: %s", setor_geo.name)
    setores = gpd.read_file(setor_geo)
    if setores.empty:
        raise ValueError(
            f"Arquivo de setores está vazio ({setor_geo}). "
            "Rode o download IBGE para o município/UF corretos."
        )
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
    slug = IVS_OUT_NAME.removeprefix("ivs_").removesuffix(".csv")
    basico_path = _resolve_existing_file(
        [
            IBGE_DIR / f"{slug}_basico.csv",
            IBGE_DIR / "municipio_basico.csv",
            IBGE_DIR / "basico.csv",
        ],
        "IBGE basico",
    )
    dom1_path = _resolve_existing_file(
        [
            IBGE_DIR / f"{slug}_domicilio.csv",
            IBGE_DIR / "municipio_domicilio.csv",
            IBGE_DIR / "domicilio.csv",
        ],
        "IBGE domicilio",
    )
    pessoa01_path = _resolve_existing_file(
        [
            IBGE_DIR / f"{slug}_pessoa01.csv",
            IBGE_DIR / "municipio_pessoa01.csv",
            IBGE_DIR / "pessoa01.csv",
        ],
        "IBGE pessoa01",
    )
    alfa_path = _resolve_optional_file(
        [
            IBGE_DIR / f"{slug}_alfabetizacao.csv",
            IBGE_DIR / f"{slug}_pessoa02.csv",
            IBGE_DIR / "municipio_alfabetizacao.csv",
            IBGE_DIR / "municipio_pessoa02.csv",
            IBGE_DIR / "alfabetizacao.csv",
            IBGE_DIR / "pessoa02.csv",
        ]
    )
    dom2_path = _resolve_optional_file(
        [
            IBGE_DIR / f"{slug}_domicilio2.csv",
            IBGE_DIR / "municipio_domicilio2.csv",
            IBGE_DIR / "domicilio2.csv",
        ]
    )
    cor_path = _resolve_optional_file(
        [
            IBGE_DIR / f"{slug}_cor_raca.csv",
            IBGE_DIR / "municipio_cor_raca.csv",
            IBGE_DIR / "cor_raca.csv",
        ]
    )

    basico = _read_ibge(basico_path, BAS_COLS)
    basico = basico.rename(columns={"CD_setor": "CD_SETOR", "v0001": "pop_total"})
    basico["CD_SETOR"] = basico["CD_SETOR"].astype(str).str.strip()

    dom1 = _read_ibge(dom1_path, DOM1_COLS)
    dom2 = _read_ibge_optional(
        [dom2_path] if dom2_path else [],
        DOM2_COLS,
        "IBGE domicilio2",
    )
    alfa = _read_ibge_optional(
        [alfa_path] if alfa_path else [],
        ALFA_COLS,
        "IBGE alfabetizacao",
    )
    pes  = _read_ibge(pessoa01_path, PES_COLS)
    cor  = _read_ibge_optional(
        [cor_path] if cor_path else [],
        COR_COLS,
        "IBGE cor_raca",
    )

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
    log.info("  %d setores × %d territórios — executando overlay (pode demorar)...",
             len(setores), len(territorios))

    # Projetar para UTM 22S
    s_utm = setores.to_crs(CRS_UTM)
    t_utm = territorios.to_crs(CRS_UTM)

    s_utm["_area_setor"] = s_utm.geometry.area

    # Interseção
    t0 = time.monotonic()
    inter = gpd.overlay(s_utm, t_utm, how="intersection")
    log.info("  overlay concluído em %.1fs — %d fragmentos", time.monotonic() - t0, len(inter))
    inter["_area_inter"] = inter.geometry.area

    # Mapa área original do setor
    area_map = s_utm.set_index("CD_SETOR")["_area_setor"]
    inter["_area_orig"] = inter["CD_SETOR"].map(area_map)
    inter["_frac"] = (inter["_area_inter"] / inter["_area_orig"].clip(lower=1e-10)).clip(upper=1.0)

    resultado: dict[str, pd.Series] = {}
    for col in _tqdm(colunas_soma, desc="  agregando colunas", leave=False, unit="col"):
        if col not in inter.columns:
            log.debug("  coluna %s não encontrada na interseção", col)
            continue
        inter[f"_w_{col}"] = pd.to_numeric(inter[col], errors="coerce") * inter["_frac"]
        resultado[col] = inter.groupby("id_ubs")[f"_w_{col}"].sum(min_count=1)

    df_res = pd.DataFrame(resultado)
    log.info("  Resultado: %d territórios × %d colunas", len(df_res), len(df_res.columns))
    return df_res


def contar_osc_por_territorio(territorios: gpd.GeoDataFrame) -> pd.Series:
    """
    Conta entidades comunitárias (OpenStreetMap) por território Voronoi.
    Retorna Series indexada por id_ubs com contagem de OSC.
    """
    osc_path = OSC_FILE if OSC_FILE.exists() else RAW / "osc_municipio_osm.json"
    if not osc_path.exists():
        log.warning("OSC JSON não encontrado (%s) — D3 será neutro (0 OSC)", OSC_FILE.name)
        return pd.Series(0.0, index=territorios.set_index("id_ubs").index)

    with open(osc_path, encoding="utf-8") as f:
        records = json.load(f)

    osc_gdf = gpd.GeoDataFrame(
        records,
        geometry=gpd.points_from_xy(
            [r["lon"] for r in records],
            [r["lat"] for r in records],
        ),
        crs="EPSG:4326",
    ).to_crs(CRS_UTM)

    t_utm = territorios.to_crs(CRS_UTM)[["id_ubs", "geometry"]]
    joined = gpd.sjoin(osc_gdf, t_utm, how="left", predicate="within")
    counts = joined.groupby("id_ubs").size()

    all_ids = territorios["id_ubs"].values
    result = counts.reindex(all_ids).fillna(0)
    log.info("  D3 — %d entidades OSM distribuídas em %d territórios", len(records), (result > 0).sum())
    return result


def carregar_escola_por_territorio(territorios: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Carrega escolas geocodificadas e soma matrículas por território Voronoi.
    Retorna DataFrame indexado por id_ubs com colunas:
      n_escolas, QT_MAT_INF_CRE, QT_MAT_INF_PRE, QT_MAT_FUND_AI, QT_MAT_FUND_AF
    """
    escola_path = ESCOLA_FILE if (ESCOLA_FILE and ESCOLA_FILE.exists()) else None
    mat_cols = ["QT_MAT_INF_CRE", "QT_MAT_INF_PRE", "QT_MAT_FUND_AI", "QT_MAT_FUND_AF"]
    zero_result = pd.DataFrame(0.0, index=territorios["id_ubs"], columns=["n_escolas"] + mat_cols)

    if escola_path is None:
        log.warning("Escolas geocodificadas não encontradas (%s) — D2 escola será zero", ESCOLA_FILE)
        return zero_result

    try:
        df = pd.read_csv(escola_path, dtype=str)
        if df.empty or "LATITUDE" not in df.columns or "LONGITUDE" not in df.columns:
            log.warning("  escolas_geo.csv vazio ou sem coordenadas — D2 escola será zero")
            return zero_result

        df["LATITUDE"] = pd.to_numeric(df["LATITUDE"].str.replace(",", "."), errors="coerce")
        df["LONGITUDE"] = pd.to_numeric(df["LONGITUDE"].str.replace(",", "."), errors="coerce")
        df = df.dropna(subset=["LATITUDE", "LONGITUDE"])

        if df.empty:
            log.warning("  nenhuma escola com coordenadas válidas — D2 escola será zero")
            return zero_result

        for c in mat_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            else:
                df[c] = 0.0

        escola_gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["LONGITUDE"], df["LATITUDE"]),
            crs="EPSG:4326",
        ).to_crs(CRS_UTM)

        t_utm = territorios.to_crs(CRS_UTM)[["id_ubs", "geometry"]]
        joined = gpd.sjoin(escola_gdf, t_utm, how="left", predicate="within")

        agg_cols = {c: "sum" for c in mat_cols}
        agg_cols["geometry"] = "count"
        result = joined.groupby("id_ubs").agg(agg_cols).rename(columns={"geometry": "n_escolas"})
        result = result.reindex(territorios["id_ubs"]).fillna(0)

        n_com_escola = (result["n_escolas"] > 0).sum()
        log.info("  D2 escola — %d escolas geocodificadas; %d territórios com ao menos 1 escola",
                 len(df), n_com_escola)
        return result
    except Exception as e:  # noqa: BLE001
        log.warning("  erro ao carregar escolas: %s — D2 escola será zero", e)
        return zero_result


def contar_cnpj_osc_por_territorio(territorios: gpd.GeoDataFrame) -> pd.Series:
    """
    Conta entidades OSC do CNPJ (com CEPs geocodificados) por território Voronoi.
    Requer arquivo `{slug}_cnpj_osc_geo.csv` com colunas LATITUDE e LONGITUDE.
    Retorna Series indexada por id_ubs.
    """
    cnpj_path = CNPJ_FILE if (CNPJ_FILE and CNPJ_FILE.exists()) else None
    zero = pd.Series(0.0, index=territorios["id_ubs"])

    if cnpj_path is None:
        log.info("  CNPJ geocodificado não encontrado — D3 usará apenas OSM")
        return zero

    try:
        df = pd.read_csv(cnpj_path, dtype=str)
        if df.empty or "LATITUDE" not in df.columns or "LONGITUDE" not in df.columns:
            log.info("  CNPJ geo vazio ou sem coordenadas — D3 usará apenas OSM")
            return zero

        df["LATITUDE"] = pd.to_numeric(df["LATITUDE"].str.replace(",", "."), errors="coerce")
        df["LONGITUDE"] = pd.to_numeric(df["LONGITUDE"].str.replace(",", "."), errors="coerce")
        df = df.dropna(subset=["LATITUDE", "LONGITUDE"])

        cnpj_gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["LONGITUDE"], df["LATITUDE"]),
            crs="EPSG:4326",
        ).to_crs(CRS_UTM)

        t_utm = territorios.to_crs(CRS_UTM)[["id_ubs", "geometry"]]
        joined = gpd.sjoin(cnpj_gdf, t_utm, how="left", predicate="within")
        counts = joined.groupby("id_ubs").size().reindex(territorios["id_ubs"]).fillna(0)

        log.info("  D3 CNPJ — %d OSCs geocodificadas em %d territórios", len(df), (counts > 0).sum())
        return counts
    except Exception as e:  # noqa: BLE001
        log.warning("  erro ao carregar CNPJ geo: %s — D3 usará apenas OSM", e)
        return zero


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

    # D4 — Adolescentes femininas 10-19 anos (proxy mães adolescentes, SINASC sem CEP)
    ind["D4_adol_fem"] = (
        (col("V01022") + col("V01023")) / col("V01006").replace(0, np.nan) * 100
    )

    # D5 — Crianças < 1 ano: proxy = (0-4 anos) / 5 / total pop
    # V01032 = total 5-9 anos (também carregado para cálculo de D2_fora_fund)
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
    parser = argparse.ArgumentParser(description="Calcula IVS parcial por município")
    parser.add_argument(
        "--base-dir",
        default=str(DEFAULT_BASE),
        help="Diretório base de dados (ex.: ivs_betim)",
    )
    parser.add_argument("--slug", default=DEFAULT_SLUG, help="Slug dos arquivos filtrados (ex.: betim)")
    args = parser.parse_args()

    _configure_runtime(Path(args.base_dir), args.slug)
    log.info("Configuração ativa: base_dir=%s, slug=%s", BASE, args.slug)

    # 1. Carregar Voronoi + normalizar colunas
    log.info("Carregando territórios Voronoi: %s", VORONOI.name)
    if not VORONOI.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {VORONOI}. "
            "Rode o pipeline de download com --only voronoi antes do cálculo."
        )
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
        "V01031", "V01032", "V01033", "V01034", "V01040", "V01041",
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

    # D2 — Indicadores de cobertura escolar (Censo Escolar INEP)
    log.info("Calculando D2 escola (cobertura creche e fundamental)...")
    escola_agg = carregar_escola_por_territorio(territorios_com_id)
    pop_ubs = ind["pop_total"].replace(0, np.nan)
    escola_disponivel = escola_agg["n_escolas"].sum() > 0
    # D2_fora_creche: % crianças 0-3 sem vaga em creche (proxy: V01031/5 × 4 = 0-3 anos)
    if escola_disponivel:
        pop_0_3 = (agg["V01031"].reindex(ind.index) / 5 * 4).clip(lower=0).replace(0, np.nan) if "V01031" in agg.columns else pd.Series(np.nan, index=ind.index)
        vagas_creche = escola_agg["QT_MAT_INF_CRE"].reindex(ind.index).fillna(0)
        ind["D2_fora_creche"] = ((pop_0_3 - vagas_creche).clip(lower=0) / pop_0_3 * 100).clip(upper=100)
        # D2_fora_fund: % crianças 5-14 sem vaga no fundamental (V01032=5-9, V01033=10-14)
        pop_5_14 = (
            (agg["V01032"].reindex(ind.index) if "V01032" in agg.columns else pd.Series(0.0, index=ind.index))
            + (agg["V01033"].reindex(ind.index) if "V01033" in agg.columns else pd.Series(0.0, index=ind.index))
        ).clip(lower=0).replace(0, np.nan)
        vagas_fund = (
            escola_agg["QT_MAT_FUND_AI"].reindex(ind.index).fillna(0)
            + escola_agg["QT_MAT_FUND_AF"].reindex(ind.index).fillna(0)
        )
        ind["D2_fora_fund"] = ((pop_5_14 - vagas_fund).clip(lower=0) / pop_5_14 * 100).clip(upper=100)
    else:
        log.warning("  nenhuma escola mapeada nos territórios — D2_fora_creche e D2_fora_fund serão NaN")
        ind["D2_fora_creche"] = np.nan
        ind["D2_fora_fund"] = np.nan

    # D3 — Entidades comunitárias: OSM + CNPJ geocodificado
    log.info("Calculando D3 (entidades comunitárias OSM + CNPJ)...")
    osc_osm = contar_osc_por_territorio(territorios_com_id).reindex(ind.index).fillna(0)
    osc_cnpj = contar_cnpj_osc_por_territorio(territorios_com_id).reindex(ind.index).fillna(0)
    osc_total = osc_osm + osc_cnpj
    ind["D3_osc_per1k"] = (osc_total / pop_ubs * 1000)
    ind["D3_osc_osm"] = osc_osm
    ind["D3_osc_cnpj"] = osc_cnpj

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
    out_ind = PROC / IVS_OUT_NAME
    ind.to_csv(out_ind)
    log.info("Indicadores IVS por UBS salvos: %s (%d linhas)", out_ind.name, len(ind))

    # 8. Resumo
    print("\n=== RESUMO DOS INDICADORES ===")
    pct_cols = [c for c in ind.columns if c.startswith(("D1_", "D2_", "D3_", "D4_", "D5_"))]
    if pct_cols:
        print(ind[pct_cols].describe().round(2).to_string())

    return ind


if __name__ == "__main__":
    main()
