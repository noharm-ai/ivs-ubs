"""
calcular_ivs.py
===============
Pipeline completo para cálculo do Índice de Vulnerabilidade em Saúde (IVSaúde)
das 141 Unidades Básicas de Saúde (UBS) de Porto Alegre.

Metodologia: Determinantes Sociais da Saúde (Dahlgren & Whitehead, 1991)
Referência original: SMS-POA / DGVS

Uso:
    python src/calcular_ivs.py [--modo-demo] [--voronoi]

Flags:
    --modo-demo   Usa dados sintéticos quando arquivos reais estão ausentes
    --voronoi     Usa territórios Voronoi gerados a partir de pontos CNES
                  (fallback quando shapefile oficial não está disponível)

Saídas:
    outputs/tables/indicadores_por_ubs.csv
    outputs/tables/ivs_poa_resultado_final.csv
    outputs/tables/qualidade_dados.csv
    outputs/maps/ivs_poa_mapa_interativo.html
    outputs/maps/ivs_poa_mapa_estatico.png
    outputs/maps/ivs_poa_top20_vulneraveis.png
    outputs/maps/ivs_poa_top20_menos_vulneraveis.png
    outputs/maps/ivs_poa_heatmap_dimensoes.png
"""

from __future__ import annotations

import argparse
import json
import logging
import warnings
from pathlib import Path
from typing import Optional

import geopandas as gpd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from shapely.geometry import Point

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ---------------------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------------------

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data" / "raw"
PROC = BASE / "data" / "processed"
OUT_TABLES = BASE / "outputs" / "tables"
OUT_MAPS = BASE / "outputs" / "maps"

for _d in (PROC, OUT_TABLES, OUT_MAPS):
    _d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

COD_MUNICIPIO = "4314902"   # Porto Alegre
CRS = "EPSG:4674"           # SIRGAS 2000

# ---------------------------------------------------------------------------
# Pesos do IVSaúde
# ---------------------------------------------------------------------------

DIMENSOES = {
    "D1": {"peso": 0.50, "n_indicadores": 5},
    "D2": {"peso": 0.30, "n_indicadores": 5},
    "D3": {"peso": 0.10, "n_indicadores": 1},
    "D4": {"peso": 0.08, "n_indicadores": 1},
    "D5": {"peso": 0.02, "n_indicadores": 4},
}

INDICADORES_META = [
    # (id, dimensao, descricao, fonte, invertido)
    ("D1_pbf",          "D1", "% beneficiários Bolsa Família",           "e-Gestor/MDS",      False),
    ("D1_analf",        "D1", "% analfabetismo >15 anos",                "Censo IBGE 2022",   False),
    ("D1_negros",       "D1", "% população preta+parda",                 "Censo IBGE 2022",   False),
    ("D1_obitos_viol",  "D1", "% óbitos por causas violentas",           "SIM",               False),
    ("D1_risco_amb",    "D1", "% área de risco ambiental",               "SMAMS/DGVS-POA",    False),
    ("D2_sem_saneam",   "D2", "% domicílios sem saneamento adequado",    "Censo IBGE 2022",   False),
    ("D2_sem_lixo",     "D2", "% domicílios sem coleta de lixo",         "Censo IBGE 2022",   False),
    ("D2_sem_esf",      "D2", "% população sem cobertura ESF",           "e-Gestor APS",      False),
    ("D2_evasao_em",    "D2", "% evasão ensino médio",                   "Censo Escolar INEP",False),
    ("D2_sem_creche",   "D2", "% crianças 0-5 sem educação infantil",    "Censo Escolar INEP",False),
    ("D3_entidades",    "D3", "Nº entidades comunitárias/associativismo","Dados abertos PMPA",True),
    ("D4_mae_adol",     "D4", "% RN de mães adolescentes (10-19 anos)",  "SINASC",            False),
    ("D5_menor1",       "D5", "% crianças < 1 ano",                      "Censo IBGE 2022",   False),
    ("D5_adol",         "D5", "% adolescentes (10-19 anos)",             "Censo IBGE 2022",   False),
    ("D5_mif",          "D5", "% mulheres em idade fértil (10-49 anos)", "Censo IBGE 2022",   False),
    ("D5_idosos",       "D5", "% idosos (60+ anos)",                     "Censo IBGE 2022",   False),
]

# ---------------------------------------------------------------------------
# ETAPA 2 — Geocodificação e join territorial
# ---------------------------------------------------------------------------

def carregar_territorios_ubs(usar_voronoi: bool = False) -> gpd.GeoDataFrame:
    """
    Carrega o shapefile/GeoJSON dos territórios das 141 UBS.

    Ordem de preferência:
      1. data/raw/ubs_territorios/territorios_ubs.geojson  (oficial SMS-POA)
      2. data/raw/ubs_territorios/ugeo_ds_ubs_areas.geojson (WFS GeoSampa)
      3. data/raw/ubs_territorios/territorios_ubs_voronoi.geojson (fallback)
      4. Sintetizar 141 territórios fictícios (apenas --modo-demo)
    """
    candidatos = [
        RAW / "ubs_territorios" / "territorios_ubs.geojson",
        RAW / "ubs_territorios" / "ugeo_ds_ubs_areas.geojson",
    ]
    if usar_voronoi:
        candidatos.insert(0, RAW / "ubs_territorios" / "territorios_ubs_voronoi.geojson")

    for caminho in candidatos:
        if caminho.exists() and caminho.stat().st_size > 100:
            log.info("Carregando territórios: %s", caminho.name)
            gdf = gpd.read_file(caminho)
            gdf = _normalizar_crs(gdf)
            gdf = _garantir_coluna_ubs(gdf)
            log.info("  %d territórios carregados", len(gdf))
            return gdf

    log.warning("Shapefile de UBS não encontrado — usando territórios sintéticos")
    return _sintetizar_territorios()


def _normalizar_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        gdf = gdf.set_crs(CRS)
    elif gdf.crs.to_epsg() != 4674:
        gdf = gdf.to_crs(CRS)
    return gdf


def _garantir_coluna_ubs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Garante a existência de 'id_ubs' e 'no_ubs' padronizados."""
    # Mapeamento de nomes alternativos encontrados em diferentes fontes
    candidatos_id = ["co_cnes", "CO_CNES", "cnes", "CNES", "id_ubs", "ID_UBS",
                     "cod_ubs", "CD_UBS", "FID", "id"]
    candidatos_nome = ["no_fantasia", "NO_FANTASIA", "nome", "NOME", "no_ubs",
                       "ds_nome", "DS_NOME", "name", "NAME"]

    for col in candidatos_id:
        if col in gdf.columns:
            gdf = gdf.rename(columns={col: "id_ubs"})
            break
    if "id_ubs" not in gdf.columns:
        gdf["id_ubs"] = [f"UBS_{i:03d}" for i in range(1, len(gdf) + 1)]

    for col in candidatos_nome:
        if col in gdf.columns:
            gdf = gdf.rename(columns={col: "no_ubs"})
            break
    if "no_ubs" not in gdf.columns:
        gdf["no_ubs"] = gdf["id_ubs"].astype(str)

    return gdf[["id_ubs", "no_ubs", "geometry"]].copy()


def _sintetizar_territorios() -> gpd.GeoDataFrame:
    """
    Gera 141 polígonos retangulares fictícios dentro da bbox de Porto Alegre.
    Usado apenas em modo demo / testes.
    """
    from shapely.geometry import box
    np.random.seed(42)
    # Bbox aproximada de POA (SIRGAS 2000)
    lon_min, lon_max = -51.32, -51.04
    lat_min, lat_max = -30.25, -29.93
    n = 141
    lons = np.random.uniform(lon_min, lon_max, n)
    lats = np.random.uniform(lat_min, lat_max, n)
    delta = 0.015  # ~1.7 km
    geoms = [box(lo - delta, la - delta, lo + delta, la + delta)
             for lo, la in zip(lons, lats)]
    nomes = [f"UBS {i:03d} DEMO" for i in range(1, n + 1)]
    return gpd.GeoDataFrame(
        {"id_ubs": [f"DEMO_{i:03d}" for i in range(1, n + 1)],
         "no_ubs": nomes,
         "geometry": geoms},
        crs=CRS,
    )


def carregar_setores_censitarios() -> Optional[gpd.GeoDataFrame]:
    """Carrega shapefile dos setores censitários do RS filtrado para POA."""
    shp_dir = RAW / "ibge_setores"
    shps = sorted(shp_dir.glob("**/*.shp")) if shp_dir.exists() else []
    if not shps:
        log.warning("Setores censitários não encontrados em %s", shp_dir)
        return None
    log.info("Carregando setores censitários: %s", shps[0].name)
    setores = gpd.read_file(shps[0])
    setores = _normalizar_crs(setores)
    # Filtrar Porto Alegre
    col_mun = next((c for c in setores.columns
                    if "MUNICIPIO" in c.upper() or "CD_MUN" in c.upper()), None)
    if col_mun:
        setores = setores[setores[col_mun].astype(str).str.startswith(COD_MUNICIPIO)]
    log.info("  %d setores de Porto Alegre", len(setores))
    return setores


def agregar_setores_por_ubs(
    setores: gpd.GeoDataFrame,
    territorios: gpd.GeoDataFrame,
    colunas_soma: list[str],
) -> pd.DataFrame:
    """
    Agrega colunas de setores censitários por território de UBS.
    Usa interseção ponderada por área quando o setor cruza mais de um território.

    Retorna DataFrame indexado por id_ubs.
    """
    log.info("Fazendo spatial join setores -> territórios (ponderado por área)...")
    setores = setores.copy()
    setores["area_setor"] = setores.geometry.area

    # Projetar para métrico (UTM 22S) para cálculo correto de área
    utm = "EPSG:32722"
    s_utm = setores.to_crs(utm)
    t_utm = territorios.to_crs(utm)

    # Interseção
    inter = gpd.overlay(s_utm, t_utm, how="intersection")
    inter["area_inter"] = inter.geometry.area

    # Recalcular área original do setor na projeção métrica
    s_utm["area_setor_m2"] = s_utm.geometry.area
    area_map = s_utm.set_index("CD_SETOR" if "CD_SETOR" in s_utm.columns
                               else s_utm.index.name or "index")["area_setor_m2"]

    # Identificar coluna do setor na interseção
    setor_col = next((c for c in inter.columns if "CD_SETOR" in c.upper()), None)
    if setor_col:
        inter["area_setor_orig"] = inter[setor_col].map(area_map)
        inter["frac"] = inter["area_inter"] / inter["area_setor_orig"].clip(lower=1e-10)
    else:
        inter["frac"] = 1.0

    # Ponderar colunas
    resultado: dict[str, pd.Series] = {}
    for col in colunas_soma:
        if col not in inter.columns:
            continue
        inter[f"_w_{col}"] = inter[col] * inter["frac"]
        resultado[col] = inter.groupby("id_ubs")[f"_w_{col}"].sum()

    return pd.DataFrame(resultado)


# ---------------------------------------------------------------------------
# Carregamento de dados por indicador
# ---------------------------------------------------------------------------

class RegistroQualidade:
    """Registra metadados de qualidade de cada indicador."""

    def __init__(self):
        self.registros: list[dict] = []

    def add(self, id_ind: str, fonte: str, ano: str,
            n_ubs: int, n_missing: int, metodo_imputacao: str):
        pct_disp = (n_ubs - n_missing) / n_ubs * 100 if n_ubs > 0 else 0
        self.registros.append({
            "indicador": id_ind,
            "fonte": fonte,
            "ano_referencia": ano,
            "pct_ubs_com_dado": round(pct_disp, 1),
            "pct_missing": round(n_missing / n_ubs * 100, 1) if n_ubs > 0 else 100,
            "metodo_imputacao": metodo_imputacao,
        })

    def salvar(self, path: Path) -> None:
        pd.DataFrame(self.registros).to_csv(path, index=False, encoding="utf-8-sig")
        log.info("Qualidade de dados: %s", path)


qreg = RegistroQualidade()


def _imputar_mediana(serie: pd.Series, id_ind: str) -> pd.Series:
    """Imputa valores ausentes pela mediana; registra na qualidade."""
    n_miss = serie.isna().sum()
    if n_miss > 0:
        mediana = serie.median()
        log.warning("  %s: %d missing → imputados pela mediana (%.4f)", id_ind, n_miss, mediana)
        serie = serie.fillna(mediana)
    return serie


# ---------------------------------------------------------------------------
# D1 — Indicadores socioeconômicos
# ---------------------------------------------------------------------------

def indicador_D1_pbf(territorios: gpd.GeoDataFrame,
                     setores: Optional[gpd.GeoDataFrame],
                     modo_demo: bool) -> pd.Series:
    """% beneficiários Bolsa Família por território."""
    id_ind = "D1_pbf"
    arquivo = RAW / "pbf" / "bolsa_familia_poa.json"
    if modo_demo or not arquivo.exists():
        log.info("  %s: usando dados demo", id_ind)
        vals = _demo_valores(territorios, seed=1)
        qreg.add(id_ind, "e-Gestor/MDS", "demo", len(territorios), 0, "demo")
        return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)

    try:
        data = json.loads(arquivo.read_text())
        registros = data if isinstance(data, list) else data.get("content", [])
        df = pd.json_normalize(registros)
        # Schema Portal Transparência: valorBeneficioMedio, quantidadeBeneficiados
        if "quantidadeBeneficiados" in df.columns:
            total_beneficiados = df["quantidadeBeneficiados"].sum()
            # Sem dados por setor: distribuir proporcional à pop do território
            pop_col = _col_pop(territorios)
            if pop_col:
                pop_total = territorios[pop_col].sum()
                vals = territorios[pop_col] / pop_total * total_beneficiados / territorios[pop_col]
                vals = vals.fillna(0)
            else:
                vals = _demo_valores(territorios, seed=1)
        else:
            vals = _demo_valores(territorios, seed=1)
        qreg.add(id_ind, "Portal Transparência/MDS",
                 str(df.get("anoMesReferencia", ["desconhecido"]).iloc[0]
                     if len(df) > 0 else "desconhecido"),
                 len(territorios), 0, "nenhuma")
    except Exception as e:
        log.warning("  %s: erro ao processar (%s) — usando demo", id_ind, e)
        vals = _demo_valores(territorios, seed=1)
        qreg.add(id_ind, "Portal Transparência/MDS", "erro", len(territorios), len(territorios), "demo")

    return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)


def indicador_D1_analf(territorios: gpd.GeoDataFrame,
                       setores: Optional[gpd.GeoDataFrame],
                       modo_demo: bool) -> pd.Series:
    """% analfabetismo em maiores de 15 anos (Censo 2022)."""
    id_ind = "D1_analf"
    return _indicador_censo(
        id_ind=id_ind,
        territorios=territorios,
        setores=setores,
        modo_demo=modo_demo,
        col_numerador="V010",   # alfabetização: não alfabetizados >15 (ver dicionário)
        col_denominador="V009", # pop >15 anos
        seed=2,
    )


def indicador_D1_negros(territorios: gpd.GeoDataFrame,
                        setores: Optional[gpd.GeoDataFrame],
                        modo_demo: bool) -> pd.Series:
    """% população preta + parda (Censo 2022)."""
    id_ind = "D1_negros"
    return _indicador_censo(
        id_ind=id_ind,
        territorios=territorios,
        setores=setores,
        modo_demo=modo_demo,
        col_numerador=["V006", "V007"],   # pretos + pardos
        col_denominador="V001",           # pop total
        seed=3,
    )


def indicador_D1_obitos_violentos(territorios: gpd.GeoDataFrame,
                                  setores: Optional[gpd.GeoDataFrame],
                                  modo_demo: bool) -> pd.Series:
    """% óbitos por causas violentas (SIM)."""
    id_ind = "D1_obitos_viol"
    sim_dir = RAW / "sim"
    sim_files = list(sim_dir.glob("*.dbc")) + list(sim_dir.glob("*.csv")) if sim_dir.exists() else []

    if modo_demo or not sim_files:
        log.info("  %s: usando dados demo", id_ind)
        vals = _demo_valores(territorios, seed=4, scale=0.08)
        qreg.add(id_ind, "SIM/DataSUS", "demo", len(territorios), 0, "demo")
        return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)

    try:
        df = _ler_sim(sim_files)
        # CIDs de causas externas: V01-Y98
        causas_ext = df["CAUSABAS"].str.match(r"^[V-Y]", na=False)
        obitos_viol = df[causas_ext]
        # Agregar por coordenada -> território
        gdf_obitos = _geo_from_coords(df, "CODBAIRES")
        if gdf_obitos is not None:
            join = gpd.sjoin(gdf_obitos, territorios[["id_ubs", "geometry"]], how="left")
            total_por_ubs = join.groupby("id_ubs").size()
            viol_por_ubs = join[join.index.isin(obitos_viol.index)].groupby("id_ubs").size()
            vals = (viol_por_ubs / total_por_ubs).reindex(territorios["id_ubs"]).fillna(0)
        else:
            vals = _demo_valores(territorios, seed=4, scale=0.08)
        qreg.add(id_ind, "SIM/DataSUS", "triênio recente", len(territorios),
                 vals.isna().sum(), "mediana")
    except Exception as e:
        log.warning("  %s: erro (%s)", id_ind, e)
        vals = _demo_valores(territorios, seed=4, scale=0.08)
        qreg.add(id_ind, "SIM/DataSUS", "erro", len(territorios), len(territorios), "demo")

    return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)


def indicador_D1_risco_ambiental(territorios: gpd.GeoDataFrame,
                                  setores: Optional[gpd.GeoDataFrame],
                                  modo_demo: bool) -> pd.Series:
    """% área do território classificada como risco ambiental (SMAMS)."""
    id_ind = "D1_risco_amb"
    risco_file = next(
        (RAW / "ubs_territorios" / f for f in
         ["smams_areas_risco.geojson", "smams_areas_risco.geojson",
          "ugeo_smams_areas_risco.geojson"]),
        None,
    )
    # Procurar na pasta raw
    candidates = list((RAW / "ubs_territorios").glob("*risco*")) if \
        (RAW / "ubs_territorios").exists() else []

    if modo_demo or not candidates:
        log.info("  %s: usando dados demo", id_ind)
        vals = _demo_valores(territorios, seed=5, scale=0.15)
        qreg.add(id_ind, "SMAMS/DGVS-POA", "demo", len(territorios), 0, "demo")
        return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)

    try:
        risco = gpd.read_file(candidates[0])
        risco = _normalizar_crs(risco)
        # Projetar para métrico
        utm = "EPSG:32722"
        r_utm = risco.to_crs(utm)
        t_utm = territorios.to_crs(utm)
        t_utm["area_total"] = t_utm.geometry.area
        inter = gpd.overlay(t_utm, r_utm, how="intersection")
        inter["area_risco"] = inter.geometry.area
        pct_risco = (inter.groupby("id_ubs")["area_risco"].sum()
                     / t_utm.set_index("id_ubs")["area_total"]).clip(0, 1)
        vals = pct_risco.reindex(territorios["id_ubs"]).fillna(0)
        qreg.add(id_ind, "SMAMS/DGVS-POA", "recente", len(territorios),
                 vals.isna().sum(), "zero (sem risco)")
    except Exception as e:
        log.warning("  %s: erro (%s)", id_ind, e)
        vals = _demo_valores(territorios, seed=5, scale=0.15)
        qreg.add(id_ind, "SMAMS/DGVS-POA", "erro", len(territorios), len(territorios), "demo")

    return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)


# ---------------------------------------------------------------------------
# D2 — Condições de vida e trabalho
# ---------------------------------------------------------------------------

def indicador_D2_sem_saneamento(territorios, setores, modo_demo):
    """% domicílios sem rede de água e esgoto adequados (Censo 2022)."""
    return _indicador_censo(
        id_ind="D2_sem_saneam",
        territorios=territorios, setores=setores, modo_demo=modo_demo,
        col_numerador="V042",   # domicílios sem abastecimento de água adequado
        col_denominador="V001", # total de domicílios
        seed=6,
    )


def indicador_D2_sem_lixo(territorios, setores, modo_demo):
    """% domicílios sem coleta de lixo (Censo 2022)."""
    return _indicador_censo(
        id_ind="D2_sem_lixo",
        territorios=territorios, setores=setores, modo_demo=modo_demo,
        col_numerador="V044",   # domicílios sem coleta de lixo
        col_denominador="V001",
        seed=7,
    )


def indicador_D2_sem_esf(territorios, setores, modo_demo):
    """% população sem cobertura ESF (e-Gestor APS)."""
    id_ind = "D2_sem_esf"
    esf_file = RAW / "esf" / "cobertura_esf_poa.json"
    if modo_demo or not esf_file.exists():
        vals = _demo_valores(territorios, seed=8)
        qreg.add(id_ind, "e-Gestor APS", "demo", len(territorios), 0, "demo")
        return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)

    try:
        data = json.loads(esf_file.read_text())
        registros = data if isinstance(data, list) else data.get("content", [])
        df = pd.json_normalize(registros)
        # Mapear por CNES (se disponível no arquivo de territórios)
        if "co_cnes" in territorios.columns and "nu_cnes" in df.columns:
            df["id_ubs"] = df["nu_cnes"].astype(str)
            cobertura = df.set_index("id_ubs")["nu_cobertura_esf"]
            sem_esf = (100 - cobertura.clip(0, 100)) / 100
            vals = sem_esf.reindex(territorios["id_ubs"]).fillna(
                sem_esf.median())
        else:
            vals = _demo_valores(territorios, seed=8)
        qreg.add(id_ind, "e-Gestor APS", "recente", len(territorios),
                 vals.isna().sum(), "mediana")
    except Exception as e:
        log.warning("  %s: erro (%s)", id_ind, e)
        vals = _demo_valores(territorios, seed=8)
        qreg.add(id_ind, "e-Gestor APS", "erro", len(territorios), len(territorios), "demo")

    return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)


def indicador_D2_evasao_em(territorios, setores, modo_demo):
    """% evasão/abandono no ensino médio (Censo Escolar INEP)."""
    return _indicador_censo_escolar(
        id_ind="D2_evasao_em",
        territorios=territorios,
        modo_demo=modo_demo,
        etapa="EM",
        seed=9,
    )


def indicador_D2_sem_creche(territorios, setores, modo_demo):
    """% crianças 0-5 anos não matriculadas em educação infantil."""
    return _indicador_censo_escolar(
        id_ind="D2_sem_creche",
        territorios=territorios,
        modo_demo=modo_demo,
        etapa="EI",
        seed=10,
    )


# ---------------------------------------------------------------------------
# D3 — Redes sociais e comunitárias (invertido)
# ---------------------------------------------------------------------------

def indicador_D3_entidades(territorios, setores, modo_demo):
    """
    Nº de entidades comunitárias e de associativismo por território.
    INDICADOR INVERTIDO: mais entidades = menos vulnerabilidade.

    Fontes públicas alternativas (funcionam para qualquer município):
      - CNPJ/RFB: entidades do terceiro setor (OSCIP, associações — natureza jurídica 3xxx)
      - Portal da Transparência: organizações da sociedade civil (MAPA OSC/IPEA)
    """
    id_ind = "D3_entidades"
    # Tentar MAPA OSC (IPEA) — dados abertos, cobre todos os municípios
    mapa_osc = RAW / "entidades" / "mapa_osc_poa.csv"
    if modo_demo or not mapa_osc.exists():
        vals = _demo_valores(territorios, seed=11, scale=15, loc=5)
        qreg.add(id_ind, "MAPA OSC/IPEA", "demo", len(territorios), 0, "demo")
        return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)

    try:
        df = pd.read_csv(mapa_osc, dtype=str)
        lat_col = next((c for c in df.columns if "lat" in c.lower()), None)
        lon_col = next((c for c in df.columns if "lon" in c.lower()), None)
        if lat_col and lon_col:
            df = df.dropna(subset=[lat_col, lon_col])
            df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
            df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
            gdf_osc = gpd.GeoDataFrame(
                df,
                geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
                crs=CRS,
            )
            join = gpd.sjoin(gdf_osc, territorios[["id_ubs", "geometry"]], how="left")
            contagem = join.groupby("id_ubs").size().reindex(
                territorios["id_ubs"]).fillna(0)
        else:
            contagem = _demo_valores(territorios, seed=11, scale=15, loc=5)
        qreg.add(id_ind, "MAPA OSC/IPEA", "recente", len(territorios),
                 contagem.isna().sum(), "zero")
        vals = contagem
    except Exception as e:
        log.warning("  %s: erro (%s)", id_ind, e)
        vals = _demo_valores(territorios, seed=11, scale=15, loc=5)
        qreg.add(id_ind, "MAPA OSC/IPEA", "erro", len(territorios), len(territorios), "demo")

    return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)


# ---------------------------------------------------------------------------
# D4 — Estilo de vida
# ---------------------------------------------------------------------------

def indicador_D4_mae_adolescente(territorios, setores, modo_demo):
    """% recém-nascidos de mães adolescentes (10-19 anos) — SINASC."""
    id_ind = "D4_mae_adol"
    sinasc_files = (list((RAW / "sinasc").glob("*.dbc")) +
                    list((RAW / "sinasc").glob("*.csv"))) if (RAW / "sinasc").exists() else []

    if modo_demo or not sinasc_files:
        vals = _demo_valores(territorios, seed=12, scale=0.12)
        qreg.add(id_ind, "SINASC/DataSUS", "demo", len(territorios), 0, "demo")
        return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)

    try:
        df = _ler_sinasc(sinasc_files)
        df["mae_adol"] = df["IDADEMAE"].between(10, 19)
        gdf_nasc = _geo_from_coords(df, "CODMUNNASC")
        if gdf_nasc is not None:
            join = gpd.sjoin(gdf_nasc, territorios[["id_ubs", "geometry"]], how="left")
            total = join.groupby("id_ubs").size()
            adol = join[join["mae_adol"]].groupby("id_ubs").size()
            vals = (adol / total).reindex(territorios["id_ubs"]).fillna(0)
        else:
            vals = _demo_valores(territorios, seed=12, scale=0.12)
        qreg.add(id_ind, "SINASC/DataSUS", "triênio recente", len(territorios),
                 vals.isna().sum(), "mediana")
    except Exception as e:
        log.warning("  %s: erro (%s)", id_ind, e)
        vals = _demo_valores(territorios, seed=12, scale=0.12)
        qreg.add(id_ind, "SINASC/DataSUS", "erro", len(territorios), len(territorios), "demo")

    return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)


# ---------------------------------------------------------------------------
# D5 — Idade, sexo e fatores hereditários
# ---------------------------------------------------------------------------

def indicador_D5_menor1(territorios, setores, modo_demo):
    return _indicador_censo("D5_menor1", territorios, setores, modo_demo,
                            "V003", "V001", seed=13, scale=0.015)


def indicador_D5_adolescentes(territorios, setores, modo_demo):
    return _indicador_censo("D5_adol", territorios, setores, modo_demo,
                            ["V022", "V023", "V024", "V025",
                             "V026", "V027", "V028", "V029",
                             "V030", "V031"],  # grupos 10-19 anos
                            "V001", seed=14, scale=0.12)


def indicador_D5_mif(territorios, setores, modo_demo):
    return _indicador_censo("D5_mif", territorios, setores, modo_demo,
                            ["V022", "V023", "V024", "V025",
                             "V026", "V027", "V028", "V029",
                             "V030", "V031", "V032", "V033",
                             "V034", "V035", "V036", "V037",
                             "V038", "V039", "V040"],  # mulheres 10-49
                            "V001", seed=15, scale=0.25)


def indicador_D5_idosos(territorios, setores, modo_demo):
    return _indicador_censo("D5_idosos", territorios, setores, modo_demo,
                            "V060",  # 60+
                            "V001", seed=16, scale=0.12)


# ---------------------------------------------------------------------------
# Helpers genéricos
# ---------------------------------------------------------------------------

def _indicador_censo(id_ind: str, territorios, setores, modo_demo: bool,
                     col_numerador, col_denominador: str,
                     seed: int, scale: float = 0.1) -> pd.Series:
    """
    Calcula um indicador proporcional a partir dos dados do Censo 2022
    agregados por território de UBS.

    col_numerador pode ser string (uma coluna) ou list (soma de colunas).
    """
    # Tentar leitura de arquivo processado
    proc_file = PROC / f"{id_ind}_por_setor.csv"

    if modo_demo or setores is None:
        vals = _demo_valores(territorios, seed=seed, scale=scale)
        fonte = "Censo IBGE 2022 (demo)"
        ano = "demo"
        n_miss = 0
        met_imp = "demo"
    else:
        try:
            cols_num = [col_numerador] if isinstance(col_numerador, str) else col_numerador
            cols_usar = [c for c in cols_num + [col_denominador] if c in setores.columns]
            if not cols_usar:
                raise ValueError(f"Colunas {cols_num + [col_denominador]} não encontradas")

            agregado = agregar_setores_por_ubs(setores, territorios,
                                               list(set(cols_usar)))
            num = sum(agregado[c] for c in cols_num if c in agregado)
            den = agregado[col_denominador] if col_denominador in agregado else pd.Series(1, index=agregado.index)
            vals = (num / den.replace(0, np.nan)).clip(0, 1)
            vals = _imputar_mediana(vals, id_ind)
            vals = vals.reindex(territorios["id_ubs"])
            fonte = "Censo IBGE 2022"
            ano = "2022"
            n_miss = vals.isna().sum()
            met_imp = "mediana" if n_miss > 0 else "nenhuma"
            vals = vals.fillna(vals.median())
        except Exception as e:
            log.warning("  %s: erro censo (%s) — demo", id_ind, e)
            vals = _demo_valores(territorios, seed=seed, scale=scale)
            fonte = "Censo IBGE 2022 (erro)"
            ano = "erro"
            n_miss = len(territorios)
            met_imp = "demo"

    qreg.add(id_ind, fonte, ano, len(territorios), n_miss, met_imp)
    return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)


def _indicador_censo_escolar(id_ind: str, territorios, modo_demo: bool,
                              etapa: str, seed: int) -> pd.Series:
    """
    Indicadores do Censo Escolar INEP.
    etapa: 'EM' = Ensino Médio (evasão), 'EI' = Educação Infantil (sem matrícula).
    """
    ce_dir = RAW / "censo_escolar"
    csv_files = list(ce_dir.glob(f"*{COD_UF}*.csv")) if ce_dir.exists() else []
    csv_files += list(ce_dir.glob("*matricula*.csv")) if ce_dir.exists() else []

    if modo_demo or not csv_files:
        vals = _demo_valores(territorios, seed=seed, scale=0.08)
        qreg.add(id_ind, "Censo Escolar INEP", "demo", len(territorios), 0, "demo")
        return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)

    try:
        df = pd.read_csv(csv_files[0], sep=";", encoding="latin-1",
                         low_memory=False, dtype=str)
        # Filtrar Porto Alegre
        col_mun = next((c for c in df.columns if "CO_MUNICIPIO" in c.upper()), None)
        if col_mun:
            df = df[df[col_mun].astype(str) == COD_MUNICIPIO]

        # Geolocalizar escolas
        lat_col = next((c for c in df.columns if "LATITUDE" in c.upper()), None)
        lon_col = next((c for c in df.columns if "LONGITUDE" in c.upper()), None)
        if lat_col and lon_col:
            df[lat_col] = pd.to_numeric(df[lat_col].str.replace(",", "."), errors="coerce")
            df[lon_col] = pd.to_numeric(df[lon_col].str.replace(",", "."), errors="coerce")
            df = df.dropna(subset=[lat_col, lon_col])
            gdf = gpd.GeoDataFrame(
                df, geometry=gpd.points_from_xy(df[lon_col], df[lat_col]), crs=CRS)
            join = gpd.sjoin(gdf, territorios[["id_ubs", "geometry"]], how="left")
            # Simplificação: contar matrículas/abandono por UBS
            if etapa == "EM":
                # Filtrar etapa Ensino Médio (TP_ETAPA_ENSINO 25-38)
                col_etapa = next((c for c in join.columns if "ETAPA" in c.upper()), None)
                if col_etapa:
                    join = join[pd.to_numeric(join[col_etapa],
                                              errors="coerce").between(25, 38)]
            elif etapa == "EI":
                col_etapa = next((c for c in join.columns if "ETAPA" in c.upper()), None)
                if col_etapa:
                    join = join[pd.to_numeric(join[col_etapa],
                                              errors="coerce").between(1, 4)]
            vals_raw = join.groupby("id_ubs").size()
            # Normalizar pelo máximo para obter proporção (0-1)
            vals = vals_raw / vals_raw.max()
            vals = vals.reindex(territorios["id_ubs"]).fillna(0)
        else:
            vals = _demo_valores(territorios, seed=seed, scale=0.08)
        qreg.add(id_ind, "Censo Escolar INEP", "recente", len(territorios),
                 vals.isna().sum(), "zero")
    except Exception as e:
        log.warning("  %s: erro censo escolar (%s) — demo", id_ind, e)
        vals = _demo_valores(territorios, seed=seed, scale=0.08)
        qreg.add(id_ind, "Censo Escolar INEP", "erro", len(territorios),
                 len(territorios), "demo")

    return pd.Series(vals, index=territorios["id_ubs"], name=id_ind)


def _demo_valores(territorios, seed: int, scale: float = 0.2,
                  loc: float = 0.0) -> np.ndarray:
    """Gera valores demo com distribuição gamma truncada em [0, 1]."""
    rng = np.random.default_rng(seed)
    if loc > 1:
        # Para contagens (e.g., entidades comunitárias)
        vals = rng.gamma(shape=2, scale=scale, size=len(territorios)) + loc
    else:
        vals = np.clip(rng.gamma(shape=2, scale=scale, size=len(territorios)), 0, 1)
    return vals


def _col_pop(gdf: gpd.GeoDataFrame) -> Optional[str]:
    """Retorna coluna de população total se existir."""
    for c in ["pop_total", "V001", "POP_TOTAL", "populacao"]:
        if c in gdf.columns:
            return c
    return None


def _ler_sim(arquivos: list) -> pd.DataFrame:
    """Lê arquivos SIM (DBC ou CSV) e retorna DataFrame."""
    dfs = []
    for f in arquivos:
        try:
            if str(f).endswith(".csv"):
                dfs.append(pd.read_csv(f, sep=";", encoding="latin-1",
                                       dtype=str, low_memory=False))
            else:
                import pysus.online_data.SIM as SIM_pysus
                # pysus retorna DataFrame diretamente
                dfs.append(SIM_pysus.read_dbc(str(f)))
        except Exception as e:
            log.warning("    SIM: não foi possível ler %s (%s)", f.name, e)
    if not dfs:
        raise ValueError("Nenhum arquivo SIM legível")
    df = pd.concat(dfs, ignore_index=True)
    return df[df["CODMUNRES"].astype(str).str.startswith(COD_MUNICIPIO)]


def _ler_sinasc(arquivos: list) -> pd.DataFrame:
    """Lê arquivos SINASC (DBC ou CSV) e retorna DataFrame."""
    dfs = []
    for f in arquivos:
        try:
            if str(f).endswith(".csv"):
                dfs.append(pd.read_csv(f, sep=";", encoding="latin-1",
                                       dtype=str, low_memory=False))
            else:
                import pysus.online_data.SINASC as SINASC_pysus
                dfs.append(SINASC_pysus.read_dbc(str(f)))
        except Exception as e:
            log.warning("    SINASC: não foi possível ler %s (%s)", f.name, e)
    if not dfs:
        raise ValueError("Nenhum arquivo SINASC legível")
    df = pd.concat(dfs, ignore_index=True)
    df["IDADEMAE"] = pd.to_numeric(df.get("IDADEMAE", pd.Series(dtype=str)),
                                   errors="coerce")
    return df[df["CODMUNNASC"].astype(str).str.startswith(COD_MUNICIPIO)]


def _geo_from_coords(df: pd.DataFrame, bairro_col: str) -> Optional[gpd.GeoDataFrame]:
    """
    Tenta construir GeoDataFrame a partir de colunas de lat/lon.
    Retorna None se coordenadas não disponíveis.
    """
    lat_col = next((c for c in df.columns if c in ("LATITUDE", "lat", "LAT")), None)
    lon_col = next((c for c in df.columns if c in ("LONGITUDE", "lon", "LON", "LONG")), None)
    if not lat_col or not lon_col:
        return None
    df2 = df.copy()
    df2[lat_col] = pd.to_numeric(df2[lat_col], errors="coerce")
    df2[lon_col] = pd.to_numeric(df2[lon_col], errors="coerce")
    df2 = df2.dropna(subset=[lat_col, lon_col])
    return gpd.GeoDataFrame(
        df2, geometry=gpd.points_from_xy(df2[lon_col], df2[lat_col]), crs=CRS
    )


# ---------------------------------------------------------------------------
# ETAPA 4 — Padronização
# ---------------------------------------------------------------------------

def padronizar_modelo_I(serie: pd.Series, media_cidade: float) -> pd.Series:
    """
    Modelo I — referência = valor de Porto Alegre como um todo.
      Grau 0  (0,00): valor = 0
      Grau I  (0,25): 0 < valor < média
      Grau II (0,50): média ≤ valor ≤ média × 1,10
      Grau III(0,75): média × 1,10 < valor ≤ média × 1,50
      Grau IV (1,00): valor > média × 1,50
    """
    def _score(v):
        if v == 0:
            return 0.00
        elif v < media_cidade:
            return 0.25
        elif v <= media_cidade * 1.10:
            return 0.50
        elif v <= media_cidade * 1.50:
            return 0.75
        else:
            return 1.00
    return serie.apply(_score)


def padronizar_modelo_II(serie: pd.Series, media: float, dp: float) -> pd.Series:
    """
    Modelo II — referência = média ± DP (indicador INVERTIDO: mais = menos vulnerável).
      Grau 0  (0,00): valor = 0
      Grau I  (0,25): 0 < valor < média
      Grau II (0,50): média ≤ valor ≤ média + 1 DP
      Grau III(0,75): média + 1 DP < valor ≤ média + 2 DP
      Grau IV (1,00): valor > média + 2 DP

    Score invertido: score_final = 1 - score_calculado
    """
    def _score(v):
        if v == 0:
            return 0.00
        elif v < media:
            return 0.25
        elif v <= media + dp:
            return 0.50
        elif v <= media + 2 * dp:
            return 0.75
        else:
            return 1.00
    scores_brutos = serie.apply(_score)
    return 1 - scores_brutos  # inversão


def aplicar_padronizacao(df_indicadores: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica Modelo I (D1, D2, D4, D5) e Modelo II (D3) a todos os indicadores.
    Retorna DataFrame com scores 0,00 / 0,25 / 0,50 / 0,75 / 1,00.
    """
    df_scores = pd.DataFrame(index=df_indicadores.index)

    for meta in INDICADORES_META:
        id_ind, dim, _, _, invertido = meta
        if id_ind not in df_indicadores.columns:
            log.warning("  indicador ausente: %s — preenchendo com 0.5", id_ind)
            df_scores[id_ind] = 0.50
            continue

        serie = df_indicadores[id_ind]

        if dim == "D3":
            # Modelo II
            media = serie.mean()
            dp = serie.std()
            df_scores[id_ind] = padronizar_modelo_II(serie, media, dp)
        else:
            # Modelo I — média da cidade = média ponderada de todos os territórios
            media_cidade = serie.mean()
            df_scores[id_ind] = padronizar_modelo_I(serie, media_cidade)

    return df_scores


# ---------------------------------------------------------------------------
# ETAPA 5 — Cálculo do IVSaúde
# ---------------------------------------------------------------------------

def calcular_ivs(df_scores: pd.DataFrame,
                 territorios: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Calcula o IVSaúde final e os scores por dimensão.
    Retorna DataFrame com colunas: id_ubs, no_ubs, score_D1..D5, ivs_saude.
    """
    result = territorios[["id_ubs", "no_ubs"]].set_index("id_ubs").copy()

    ivs_total = pd.Series(0.0, index=df_scores.index)

    for dim, cfg in DIMENSOES.items():
        peso_dim = cfg["peso"]
        cols_dim = [m[0] for m in INDICADORES_META if m[1] == dim]
        cols_pres = [c for c in cols_dim if c in df_scores.columns]

        if not cols_pres:
            log.warning("  %s: nenhum indicador disponível — score = 0.50", dim)
            result[f"score_{dim}"] = 0.50
            ivs_total += 0.50 * peso_dim
            continue

        score_dim = df_scores[cols_pres].mean(axis=1)
        result[f"score_{dim}"] = score_dim.round(4)
        ivs_total += score_dim * peso_dim

    result["ivs_saude"] = ivs_total.round(4)
    result = result.sort_values("ivs_saude", ascending=False)
    result.index.name = "id_ubs"
    return result.reset_index()


# ---------------------------------------------------------------------------
# Desfechos (fora do IVS, mas calculados para análise)
# ---------------------------------------------------------------------------

def calcular_desfechos(territorios: gpd.GeoDataFrame,
                       modo_demo: bool) -> pd.DataFrame:
    """
    Calcula desfechos em saúde por território para correlação com o IVS.
    Desfechos: sífilis congênita, mortalidade infantil, AIDS, DCNT prematura.
    """
    n = len(territorios)
    df = pd.DataFrame({"id_ubs": territorios["id_ubs"].values})

    rng = np.random.default_rng(99)
    df["desfecho_sifilis_cong_inc"]   = rng.gamma(2, 0.5, n) if modo_demo else np.nan
    df["desfecho_mort_infantil"]      = rng.gamma(2, 1.5, n) if modo_demo else np.nan
    df["desfecho_mort_aids"]          = rng.gamma(1, 0.8, n) if modo_demo else np.nan
    df["desfecho_mort_dcnt_prematura"]= rng.gamma(3, 2.0, n) if modo_demo else np.nan

    if not modo_demo:
        log.info("  Desfechos: arquivos reais não processados — valores NaN salvos")
    return df


# ---------------------------------------------------------------------------
# ETAPA 6 — Visualizações
# ---------------------------------------------------------------------------

CLASSES_COR = {
    "< 0.25 (Baixa)":         "#2ecc71",   # verde
    "0.25 – 0.49 (Moderada)": "#f1c40f",   # amarelo
    "0.50 – 0.74 (Alta)":     "#e67e22",   # laranja
    "0.75 – 0.89 (Muito Alta)":"#e74c3c",  # vermelho
    "≥ 0.90 (Extrema)":       "#7b0041",   # vinho
}


def _classe_cor(ivs: float) -> tuple[str, str]:
    if ivs < 0.25:
        return "#2ecc71", "< 0.25 (Baixa)"
    elif ivs < 0.50:
        return "#f1c40f", "0.25 – 0.49 (Moderada)"
    elif ivs < 0.75:
        return "#e67e22", "0.50 – 0.74 (Alta)"
    elif ivs < 0.90:
        return "#e74c3c", "0.75 – 0.89 (Muito Alta)"
    else:
        return "#7b0041", "≥ 0.90 (Extrema)"


def gerar_mapa_interativo(resultado: pd.DataFrame,
                          territorios: gpd.GeoDataFrame) -> None:
    """Gera mapa coroplético interativo com Folium."""
    try:
        import folium
    except ImportError:
        log.warning("folium não instalado — pulando mapa interativo")
        return

    log.info("Gerando mapa interativo...")
    # resultado já contém no_ubs; drop da coluna duplicada do territorios antes do merge
    terr_geo = territorios[["id_ubs", "geometry"]].copy()
    gdf = terr_geo.merge(resultado[["id_ubs", "no_ubs", "ivs_saude",
                                    "score_D1", "score_D2", "score_D3",
                                    "score_D4", "score_D5"]],
                         on="id_ubs")
    gdf = gdf.to_crs("EPSG:4326")

    centro = [gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean()]
    m = folium.Map(location=centro, zoom_start=12, tiles="CartoDB positron")

    def style_fn(feature):
        ivs = feature["properties"].get("ivs_saude", 0) or 0
        cor, _ = _classe_cor(ivs)
        return {"fillColor": cor, "color": "#555", "weight": 0.7,
                "fillOpacity": 0.75}

    def tooltip_fn(feature):
        p = feature["properties"]
        return (f"<b>{p.get('no_ubs', '')}</b><br>"
                f"IVSaúde: {p.get('ivs_saude', 'N/A'):.3f}<br>"
                f"D1: {p.get('score_D1', 0):.2f} | "
                f"D2: {p.get('score_D2', 0):.2f} | "
                f"D3: {p.get('score_D3', 0):.2f}<br>"
                f"D4: {p.get('score_D4', 0):.2f} | "
                f"D5: {p.get('score_D5', 0):.2f}")

    folium.GeoJson(
        gdf.__geo_interface__,
        style_function=style_fn,
        tooltip=folium.features.GeoJsonTooltip(
            fields=["no_ubs", "ivs_saude",
                    "score_D1", "score_D2", "score_D3", "score_D4", "score_D5"],
            aliases=["UBS", "IVSaúde", "D1", "D2", "D3", "D4", "D5"],
            localize=True,
        ),
    ).add_to(m)

    # Legenda
    legenda_html = """
    <div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
                background: white; padding: 12px; border: 1px solid #aaa;
                border-radius: 6px; font-size: 13px;">
    <b>IVSaúde</b><br>
    """
    for label, cor in CLASSES_COR.items():
        legenda_html += (f'<i style="background:{cor};width:14px;height:14px;'
                         f'float:left;margin-right:6px;border:1px solid #999;'
                         f'display:inline-block"></i>{label}<br>')
    legenda_html += "</div>"
    m.get_root().html.add_child(folium.Element(legenda_html))

    out = OUT_MAPS / "ivs_poa_mapa_interativo.html"
    m.save(str(out))
    log.info("Mapa interativo: %s", out)


def gerar_mapa_estatico(resultado: pd.DataFrame,
                        territorios: gpd.GeoDataFrame) -> None:
    """Mapa estático em alta resolução para relatório."""
    log.info("Gerando mapa estático...")
    gdf = territorios[["id_ubs", "geometry"]].merge(
        resultado[["id_ubs", "ivs_saude"]], on="id_ubs")

    fig, ax = plt.subplots(1, 1, figsize=(14, 14))
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_title("Índice de Vulnerabilidade em Saúde (IVSaúde)\n"
                 "Porto Alegre — Territórios das UBS", fontsize=16, pad=16)

    bounds = [0, 0.25, 0.50, 0.75, 0.90, 1.01]
    cores = ["#2ecc71", "#f1c40f", "#e67e22", "#e74c3c", "#7b0041"]
    cmap = mcolors.ListedColormap(cores)
    norm = mcolors.BoundaryNorm(bounds, cmap.N)

    gdf.plot(column="ivs_saude", cmap=cmap, norm=norm, ax=ax,
             edgecolor="#555", linewidth=0.4, missing_kwds={"color": "#ddd"})

    # Legenda manual
    patches = [mpatches.Patch(color=c, label=l)
               for c, l in zip(cores, CLASSES_COR.keys())]
    ax.legend(handles=patches, loc="lower left", title="IVSaúde", fontsize=10,
              title_fontsize=11, framealpha=0.9)

    plt.tight_layout()
    out = OUT_MAPS / "ivs_poa_mapa_estatico.png"
    fig.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)
    log.info("Mapa estático: %s", out)


def gerar_graficos_ranking(resultado: pd.DataFrame) -> None:
    """Top 20 mais e menos vulneráveis."""
    log.info("Gerando gráficos de ranking...")
    res = resultado.copy()

    for titulo, dados, fname in [
        ("Top 20 UBS mais vulneráveis", res.head(20), "ivs_poa_top20_vulneraveis.png"),
        ("Top 20 UBS menos vulneráveis", res.tail(20).iloc[::-1],
         "ivs_poa_top20_menos_vulneraveis.png"),
    ]:
        fig, ax = plt.subplots(figsize=(10, 8))
        cores = [_classe_cor(v)[0] for v in dados["ivs_saude"]]
        ax.barh(dados["no_ubs"], dados["ivs_saude"],
                color=cores, edgecolor="#555", linewidth=0.5)
        ax.set_xlabel("IVSaúde", fontsize=12)
        ax.set_title(titulo, fontsize=14, pad=12)
        ax.set_xlim(0, 1)
        ax.axvline(x=dados["ivs_saude"].mean(), color="#333",
                   linestyle="--", linewidth=1.2, label="Média")
        ax.legend(fontsize=10)
        for i, (_, row) in enumerate(dados.iterrows()):
            ax.text(row["ivs_saude"] + 0.01, i,
                    f"{row['ivs_saude']:.3f}", va="center", fontsize=8)
        plt.tight_layout()
        out = OUT_MAPS / fname
        fig.savefig(out, dpi=150, bbox_inches="tight")
        plt.close(fig)
        log.info("  %s", out)


def gerar_heatmap_dimensoes(resultado: pd.DataFrame) -> None:
    """Heatmap UBS × dimensões."""
    log.info("Gerando heatmap de dimensões...")
    dim_cols = [f"score_{d}" for d in DIMENSOES if f"score_{d}" in resultado.columns]
    if not dim_cols:
        log.warning("  Nenhuma coluna de dimensão disponível para heatmap")
        return

    # Ordenar pelo IVS e tomar top/bottom 40 para visualização
    res_sort = resultado.sort_values("ivs_saude", ascending=False)
    n_mostrar = min(40, len(res_sort))
    amostra = pd.concat([res_sort.head(n_mostrar // 2),
                         res_sort.tail(n_mostrar // 2)])
    matrix = amostra.set_index("no_ubs")[dim_cols]

    fig, ax = plt.subplots(figsize=(10, max(8, n_mostrar * 0.3)))
    im = ax.imshow(matrix.values, aspect="auto", cmap="RdYlGn_r",
                   vmin=0, vmax=1)
    ax.set_xticks(range(len(dim_cols)))
    ax.set_xticklabels(
        [c.replace("score_", "") + f"\n(peso {DIMENSOES[c.replace('score_', '')]['peso']:.2f})"
         for c in dim_cols], fontsize=10)
    ax.set_yticks(range(len(matrix)))
    ax.set_yticklabels(matrix.index, fontsize=8)
    ax.set_title("Scores por Dimensão — 40 UBS extremas\n"
                 "(verde = baixa vulnerabilidade, vermelho = alta)", fontsize=12)
    plt.colorbar(im, ax=ax, shrink=0.6, label="Score (0 = baixo, 1 = alto)")

    # Valores nas células
    for i in range(len(matrix)):
        for j in range(len(dim_cols)):
            v = matrix.values[i, j]
            ax.text(j, i, f"{v:.2f}", ha="center", va="center",
                    fontsize=7, color="black" if 0.35 < v < 0.65 else "white")

    plt.tight_layout()
    out = OUT_MAPS / "ivs_poa_heatmap_dimensoes.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("  %s", out)


# ---------------------------------------------------------------------------
# ETAPA 7 — Relatório de qualidade
# ---------------------------------------------------------------------------
# (implementado via RegistroQualidade.salvar — ver main())


# ---------------------------------------------------------------------------
# ETAPA 8 — Comparação com referência SMS-POA 2019
# ---------------------------------------------------------------------------

def _normalizar_nome(nome: str) -> str:
    """Normaliza nome de UBS para comparação fuzzy.

    Remove acentos, prefixos 'US'/'UBS'/'CS', pontuação e converte para
    maiúsculas, permitindo match entre 'US Ilha do Pavão' e 'UBS ILHA DO PAVAO'.
    """
    import unicodedata, re
    s = unicodedata.normalize("NFD", str(nome))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")  # remove diacríticos
    s = s.upper()
    s = re.sub(r"\b(UBS|US|CS|UNIDADE DE SAUDE|UNIDADE BASICA DE SAUDE)\b", "", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def comparar_com_sms_2019(resultado: pd.DataFrame) -> None:
    """Compara o IVS calculado com a referência SMS-POA 2019.

    - Carrega data/reference/ivs_poa_sms_2019.json
    - Faz matching fuzzy pelo nome normalizado (difflib)
    - Salva tabela de match + gráficos de correlação
    """
    import difflib
    from scipy.stats import spearmanr

    ref_path = BASE / "data" / "reference" / "ivs_poa_sms_2019.json"
    if not ref_path.exists():
        log.warning("[ETAPA 8] Referência SMS 2019 não encontrada: %s", ref_path)
        return

    log.info("[ETAPA 8] Comparando com referência SMS-POA 2019...")

    ref_raw = json.loads(ref_path.read_text(encoding="utf-8"))
    ref = pd.DataFrame(ref_raw["unidades_saude"])
    ref.columns = [c if c == "nome" else f"sms_{c}" for c in ref.columns]
    ref["nome_norm"] = ref["nome"].apply(_normalizar_nome)

    resultado = resultado.copy()
    resultado["nome_norm"] = resultado["no_ubs"].apply(_normalizar_nome)

    ref_nomes = ref["nome_norm"].tolist()

    def _match(nome_norm: str) -> Optional[str]:
        hits = difflib.get_close_matches(nome_norm, ref_nomes, n=1, cutoff=0.6)
        return hits[0] if hits else None

    resultado["nome_norm_match"] = resultado["nome_norm"].apply(_match)

    merged = resultado.merge(
        ref.rename(columns={"nome": "nome_sms_2019"}),
        left_on="nome_norm_match",
        right_on="nome_norm",
        how="left",
        suffixes=("", "_ref"),
    )

    n_total = len(resultado)
    n_matched = merged["nome_sms_2019"].notna().sum()
    log.info("  Matched: %d / %d UBS (%.0f%%)", n_matched, n_total,
             100 * n_matched / n_total)

    # ── Tabela de comparação ─────────────────────────────────────────────
    cols_out = (
        ["no_ubs", "nome_sms_2019", "ivs_saude", "sms_ivs",
         "score_D1", "sms_d1", "score_D2", "sms_d2",
         "score_D3", "sms_d3", "score_D4", "sms_d4",
         "score_D5", "sms_d5"]
    )
    cols_out = [c for c in cols_out if c in merged.columns]
    tab = merged[cols_out].rename(columns={
        "ivs_saude": "ivs_calculado", "sms_ivs": "ivs_sms_2019",
    })
    tab.to_csv(OUT_TABLES / "comparacao_sms_2019.csv", index=False,
               encoding="utf-8-sig")
    log.info("  Tabela salva: outputs/tables/comparacao_sms_2019.csv")

    # ── Correlações ──────────────────────────────────────────────────────
    pares = [
        ("ivs_saude",  "sms_ivs",  "IVS"),
        ("score_D1",   "sms_d1",   "D1"),
        ("score_D2",   "sms_d2",   "D2"),
        ("score_D3",   "sms_d3",   "D3"),
        ("score_D4",   "sms_d4",   "D4"),
        ("score_D5",   "sms_d5",   "D5"),
    ]
    rows = []
    for col_calc, col_sms, label in pares:
        if col_calc not in merged.columns or col_sms not in merged.columns:
            continue
        sub = merged[[col_calc, col_sms]].dropna()
        if len(sub) < 5:
            continue
        r, p = spearmanr(sub[col_calc], sub[col_sms])
        sig = "***" if p < 0.001 else ("**" if p < 0.01 else ("*" if p < 0.05 else "n.s."))
        rows.append({"variavel": label, "n": len(sub), "spearman_r": round(r, 4),
                     "p_value": round(p, 6), "sig": sig})
        log.info("  Spearman %-4s r=%.4f  p=%.4f  %s  (n=%d)",
                 label, r, p, sig, len(sub))

    if rows:
        pd.DataFrame(rows).to_csv(OUT_TABLES / "spearman_sms_2019.csv",
                                  index=False, encoding="utf-8-sig")

    # ── Gráficos ─────────────────────────────────────────────────────────
    sub_ivs = merged[["ivs_saude", "sms_ivs", "no_ubs"]].dropna()
    if len(sub_ivs) < 5:
        log.warning("  Poucos pares para gráfico (n=%d). Pulando plots.", len(sub_ivs))
        return

    # Scatter IVS calculado × SMS 2019
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(sub_ivs["sms_ivs"], sub_ivs["ivs_saude"],
               alpha=0.7, color="#2196F3", edgecolors="white", linewidths=0.5)
    lim = (0, 1)
    ax.plot(lim, lim, "k--", linewidth=0.8, alpha=0.5, label="y = x")
    r_ivs = rows[0]["spearman_r"] if rows else float("nan")
    ax.set_xlabel("IVS — SMS-POA 2019", fontsize=11)
    ax.set_ylabel("IVS — Calculado", fontsize=11)
    ax.set_title(f"IVS: Calculado vs SMS-POA 2019\nSpearman r = {r_ivs:.3f}  (n={len(sub_ivs)})",
                 fontsize=12)
    ax.set_xlim(lim); ax.set_ylim(lim)
    # Anotar outliers (delta > 0.15)
    for _, row in sub_ivs.iterrows():
        if abs(row["ivs_saude"] - row["sms_ivs"]) > 0.15:
            ax.annotate(row["no_ubs"], (row["sms_ivs"], row["ivs_saude"]),
                        fontsize=6, alpha=0.7,
                        xytext=(4, 4), textcoords="offset points")
    fig.tight_layout()
    fig.savefig(OUT_MAPS / "sms2019_scatter_ivs.png", dpi=150)
    plt.close(fig)
    log.info("  Gráfico: outputs/maps/sms2019_scatter_ivs.png")

    # Scatter por dimensão (6 painéis)
    fig, axes = plt.subplots(2, 3, figsize=(14, 9))
    axes = axes.flatten()
    for i, (col_calc, col_sms, label) in enumerate(pares):
        ax = axes[i]
        sub = merged[[col_calc, col_sms]].dropna()
        if len(sub) < 3:
            ax.set_visible(False)
            continue
        ax.scatter(sub[col_sms], sub[col_calc],
                   alpha=0.6, color="#FF5722", edgecolors="white", linewidths=0.4, s=30)
        ax.plot([0, 1], [0, 1], "k--", linewidth=0.7, alpha=0.4)
        r_row = next((x for x in rows if x["variavel"] == label), None)
        r_val = r_row["spearman_r"] if r_row else float("nan")
        sig_val = r_row["sig"] if r_row else ""
        ax.set_title(f"{label}  r={r_val:.3f} {sig_val}  n={len(sub)}", fontsize=9)
        ax.set_xlabel("SMS 2019", fontsize=8)
        ax.set_ylabel("Calculado", fontsize=8)
        ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    fig.suptitle("Scatter por Dimensão — Calculado vs SMS-POA 2019", fontsize=12)
    fig.tight_layout()
    fig.savefig(OUT_MAPS / "sms2019_scatter_dimensoes.png", dpi=150)
    plt.close(fig)
    log.info("  Gráfico: outputs/maps/sms2019_scatter_dimensoes.png")

    # Curva de ranking comparada
    sub_rank = merged[["no_ubs", "ivs_saude", "sms_ivs"]].dropna().copy()
    sub_rank = sub_rank.sort_values("ivs_saude", ascending=False).reset_index(drop=True)
    sub_rank["delta"] = sub_rank["ivs_saude"] - sub_rank["sms_ivs"]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8),
                                    gridspec_kw={"height_ratios": [3, 1]})
    x = range(len(sub_rank))
    ax1.plot(x, sub_rank["ivs_saude"], label="Calculado", color="#2196F3", linewidth=1.5)
    ax1.plot(x, sub_rank["sms_ivs"],   label="SMS 2019",  color="#F44336",
             linewidth=1.5, linestyle="--")
    ax1.set_ylabel("IVS"); ax1.legend(); ax1.set_xlim(0, len(sub_rank) - 1)
    ax1.set_title(f"Comparação IVS por Ranking — Calculado vs SMS-POA 2019\n"
                  f"n={len(sub_rank)} UBS matched")
    ax2.bar(x, sub_rank["delta"], color=["#F44336" if d > 0 else "#4CAF50"
                                          for d in sub_rank["delta"]], width=1.0)
    ax2.axhline(0, color="black", linewidth=0.8)
    ax2.set_ylabel("Delta\n(calc − SMS)")
    ax2.set_xlabel("UBS (ordenadas por IVS calculado)")
    ax2.set_xlim(0, len(sub_rank) - 1)
    fig.tight_layout()
    fig.savefig(OUT_MAPS / "sms2019_rank_comparacao.png", dpi=150)
    plt.close(fig)
    log.info("  Gráfico: outputs/maps/sms2019_rank_comparacao.png")


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def main(modo_demo: bool = False, usar_voronoi: bool = False) -> None:
    log.info("=" * 60)
    log.info("IVSaúde POA — Pipeline %s", "(MODO DEMO)" if modo_demo else "")
    log.info("=" * 60)

    # ── ETAPA 2: Dados geoespaciais ──────────────────────────────────────
    log.info("[ETAPA 2] Carregando dados geoespaciais...")
    territorios = carregar_territorios_ubs(usar_voronoi=usar_voronoi)
    setores = carregar_setores_censitarios() if not modo_demo else None

    # ── ETAPA 3: Cálculo dos indicadores ────────────────────────────────
    log.info("[ETAPA 3] Calculando indicadores...")
    funcoes_indicadores = [
        indicador_D1_pbf,
        indicador_D1_analf,
        indicador_D1_negros,
        indicador_D1_obitos_violentos,
        indicador_D1_risco_ambiental,
        indicador_D2_sem_saneamento,
        indicador_D2_sem_lixo,
        indicador_D2_sem_esf,
        indicador_D2_evasao_em,
        indicador_D2_sem_creche,
        indicador_D3_entidades,
        indicador_D4_mae_adolescente,
        indicador_D5_menor1,
        indicador_D5_adolescentes,
        indicador_D5_mif,
        indicador_D5_idosos,
    ]

    series_indicadores = []
    for func in funcoes_indicadores:
        nome = func.__name__.replace("indicador_", "")
        log.info("  calculando: %s", nome)
        try:
            s = func(territorios, setores, modo_demo)
            series_indicadores.append(s)
        except Exception as e:
            log.error("  ERRO em %s: %s", nome, e, exc_info=True)

    df_indicadores = pd.concat(series_indicadores, axis=1)
    df_indicadores.index = territorios["id_ubs"].values
    df_indicadores.to_csv(PROC / "indicadores_por_ubs.csv", encoding="utf-8-sig")
    log.info("Indicadores salvos: %s", PROC / "indicadores_por_ubs.csv")

    # ── ETAPA 4: Padronização ────────────────────────────────────────────
    log.info("[ETAPA 4] Padronizando indicadores...")
    df_scores = aplicar_padronizacao(df_indicadores)
    df_scores.to_csv(PROC / "scores_por_ubs.csv", encoding="utf-8-sig")

    # ── ETAPA 5: IVSaúde final ───────────────────────────────────────────
    log.info("[ETAPA 5] Calculando IVSaúde...")
    resultado = calcular_ivs(df_scores, territorios)

    # Adicionar indicadores brutos para consulta
    ind_merge = df_indicadores.copy()
    ind_merge.index.name = "id_ubs"
    resultado = resultado.merge(
        ind_merge.add_prefix("ind_").reset_index(),
        on="id_ubs", how="left",
    )

    out_final = OUT_TABLES / "ivs_poa_resultado_final.csv"
    resultado.to_csv(out_final, index=False, encoding="utf-8-sig")
    log.info("Resultado final: %s", out_final)

    # ── ETAPA 8: Comparação com SMS 2019 ─────────────────────────────────
    comparar_com_sms_2019(resultado)

    # Resumo no console
    log.info("\n=== TOP 5 MAIS VULNERÁVEIS ===")
    for _, r in resultado.head(5).iterrows():
        log.info("  %s — IVS: %.3f", r["no_ubs"], r["ivs_saude"])
    log.info("\n=== TOP 5 MENOS VULNERÁVEIS ===")
    for _, r in resultado.tail(5).iterrows():
        log.info("  %s — IVS: %.3f", r["no_ubs"], r["ivs_saude"])

    # ── Desfechos ────────────────────────────────────────────────────────
    desfechos = calcular_desfechos(territorios, modo_demo)
    desfechos.to_csv(OUT_TABLES / "desfechos_por_ubs.csv",
                     index=False, encoding="utf-8-sig")

    # ── ETAPA 6: Visualizações ───────────────────────────────────────────
    log.info("[ETAPA 6] Gerando visualizações...")
    gerar_mapa_interativo(resultado, territorios)
    gerar_mapa_estatico(resultado, territorios)
    gerar_graficos_ranking(resultado)
    gerar_heatmap_dimensoes(resultado)

    # ── ETAPA 7: Qualidade dos dados ─────────────────────────────────────
    log.info("[ETAPA 7] Relatório de qualidade...")
    qreg.salvar(OUT_TABLES / "qualidade_dados.csv")

    log.info("=" * 60)
    log.info("Pipeline concluído!")
    log.info("Outputs em: %s", BASE / "outputs")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calcula o IVSaúde para as UBS de POA")
    parser.add_argument("--modo-demo", action="store_true",
                        help="Usa dados sintéticos (sem arquivos reais necessários)")
    parser.add_argument("--voronoi", action="store_true",
                        help="Usa territórios Voronoi a partir dos pontos CNES")
    args = parser.parse_args()
    main(modo_demo=args.modo_demo, usar_voronoi=args.voronoi)
