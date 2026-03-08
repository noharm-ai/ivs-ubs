"""
Compara o IVS calculado (IVSaúde por território de UBS, média municipal)
com o IVS IPEA (Atlas do Desenvolvimento Humano, nível municipal).

Testa três variantes de cálculo:
  A) ivs_medio original (normalização por município, pesos SMS-POA)
  B) normalização global entre municípios, pesos SMS-POA
  C) normalização global, dimensões IPEA (3 × 1/3):
       Infra     = mean(D2_sem_saneam, D2_sem_lixo)
       CapHum    = mean(D1_analf, D2_fora_creche*, D2_fora_fund*, D4_adol_fem)
       Social    = mean(1-D3_osc_per1k, D5_idosos)   (* omitido se ausente)

Uso:
    python src/comparar_ivs_ipea.py [--ano 2010]

Saída: tabela no terminal + CSV em data/reference/ivs_comparacao_ipea.csv
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr

ROOT = Path(__file__).resolve().parents[1]
IPEA_FILE = ROOT / "data" / "basecompletamunicipal" / "atlasivs_dadosbrutos_pt_v2.xlsx"
MANIFEST   = ROOT / "docs" / "data" / "municipios.json.gz"
DOCS_DATA  = ROOT / "docs" / "data"

# Pesos SMS-POA originais
DIM_PESOS_SMS = {"D1": 0.50, "D2": 0.30, "D3": 0.10, "D4": 0.08, "D5": 0.02}

# Mapeamento de indicadores brutos → dimensão IPEA
# Infra     : D2_sem_saneam, D2_sem_lixo
# CapHum    : D1_analf, D2_fora_creche, D2_fora_fund, D4_adol_fem
# Social    : D3_osc_per1k (INVERSO), D5_idosos
IPEA_DIMS = {
    "infra":    ["D2_sem_saneam", "D2_sem_lixo"],
    "cap_hum":  ["D1_analf", "D2_fora_creche", "D2_fora_fund", "D4_adol_fem"],
    "social":   ["D3_osc_per1k", "D5_idosos"],
}
INDICADORES_INVERSOS = {"D3_osc_per1k"}


# ── helpers ────────────────────────────────────────────────────────────────────

def sig(p: float) -> str:
    if p < 0.001: return "***"
    if p < 0.01:  return "**"
    if p < 0.05:  return "*"
    return "ns"

def interpret(r: float) -> str:
    a = abs(r)
    if a >= 0.80: return "muito forte"
    if a >= 0.60: return "forte"
    if a >= 0.40: return "moderada"
    if a >= 0.20: return "fraca"
    return "muito fraca"

def to_float(v) -> float | None:
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError):
        return None

def corr_row(label: str, x: pd.Series, y: pd.Series) -> str:
    valid = pd.DataFrame({"x": x, "y": y}).dropna()
    n = len(valid)
    if n < 3:
        return f"  {label:<30}  N={n} insuficiente"
    rp, pp = pearsonr(valid["x"], valid["y"])
    rs, ps = spearmanr(valid["x"], valid["y"])
    return (f"  {label:<30}  N={n:>4}  "
            f"Pearson r={rp:+.4f} {sig(pp):<3}  "
            f"Spearman ρ={rs:+.4f} {sig(ps):<3}  → {interpret(rs)}")


# ── carregamento ───────────────────────────────────────────────────────────────

def load_manifest() -> pd.DataFrame:
    data = json.loads(gzip.decompress(MANIFEST.read_bytes()))
    df = pd.DataFrame(data)
    df["ibge7"] = df["ibge"].astype(str).str.zfill(7)
    return df[["slug", "nome", "uf", "ibge7", "ivs_medio", "n_ubs"]]


def load_ubs_indicators() -> pd.DataFrame:
    """Carrega indicadores brutos por UBS de todos os JSONs em docs/data/."""
    rows = []
    for gz in DOCS_DATA.glob("**/*.json.gz"):
        if gz.name == "municipios.json.gz":
            continue
        try:
            d = json.loads(gzip.decompress(gz.read_bytes()))
        except Exception:
            continue
        ibge7 = str(d["meta"].get("ibge", "")).zfill(7)
        for ubs in d.get("tabela", []):
            row = {"ibge7": ibge7}
            for ind in ["D1_analf", "D1_negros", "D2_sem_saneam", "D2_sem_lixo",
                        "D2_fora_creche", "D2_fora_fund", "D3_osc_per1k",
                        "D4_adol_fem", "D5_menor1", "D5_adol", "D5_mif", "D5_idosos"]:
                row[ind] = ubs.get(ind)
            rows.append(row)
    return pd.DataFrame(rows)


def load_ipea(ano: int) -> pd.DataFrame:
    print(f"Lendo IPEA ({IPEA_FILE.name}) — pode levar alguns segundos...")
    raw = pd.read_excel(IPEA_FILE)
    mun = raw[raw["nivel"] == "regiao,uf,rm,municipio"].copy()
    mun = mun[mun["ano"] == ano].copy()
    mun = mun[
        (mun["label_cor"]     == "Total Cor") &
        (mun["label_sexo"]    == "Total Sexo") &
        (mun["label_sit_dom"] == "Total Situação de Domicílio")
    ].copy()
    for col in ["ivs", "ivs_infraestrutura_urbana", "ivs_capital_humano", "ivs_renda_e_trabalho"]:
        mun[col] = mun[col].apply(to_float)
    mun["ibge7"] = mun["municipio"].astype(str).str.zfill(7)
    return mun[["ibge7", "nome_municipio_uf", "ivs",
                "ivs_infraestrutura_urbana", "ivs_capital_humano",
                "ivs_renda_e_trabalho"]].rename(columns={
        "ivs": "ivs_ipea",
        "ivs_infraestrutura_urbana": "infra_ipea",
        "ivs_capital_humano":        "cap_hum_ipea",
        "ivs_renda_e_trabalho":      "renda_ipea",
    })


# ── cálculo de IVS alternativo (normalização global) ──────────────────────────

def normalizar_global(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Min-max global (todos os municípios juntos) para as colunas brutas."""
    out = df.copy()
    for col in cols:
        s = pd.to_numeric(out[col], errors="coerce")
        mn, mx = s.min(), s.max()
        if mx > mn:
            out[col] = (s - mn) / (mx - mn)
        else:
            out[col] = 0.5
        if col in INDICADORES_INVERSOS:
            out[col] = 1 - out[col]
    return out


def calcular_ivs_global(ubs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna DataFrame com ibge7 + ivs_global_sms + ivs_global_ipea,
    usando normalização global entre todos os municípios.
    """
    all_ind = ["D1_analf", "D1_negros", "D2_sem_saneam", "D2_sem_lixo",
               "D2_fora_creche", "D2_fora_fund", "D3_osc_per1k",
               "D4_adol_fem", "D5_menor1", "D5_adol", "D5_mif", "D5_idosos"]

    df = normalizar_global(ubs_df, all_ind)

    # ── Variante B: pesos SMS-POA sobre normalização global ──────────────────
    d1_ind = [c for c in ["D1_analf", "D1_negros"] if df[c].notna().any()]
    d2_ind = [c for c in ["D2_sem_saneam", "D2_sem_lixo",
                          "D2_fora_creche", "D2_fora_fund"] if df[c].notna().any()]
    d3_ind = [c for c in ["D3_osc_per1k"] if df[c].notna().any()]
    d4_ind = [c for c in ["D4_adol_fem"] if df[c].notna().any()]
    d5_ind = [c for c in ["D5_menor1", "D5_adol", "D5_mif", "D5_idosos"] if df[c].notna().any()]

    for label, inds in [("D1g", d1_ind), ("D2g", d2_ind), ("D3g", d3_ind),
                        ("D4g", d4_ind), ("D5g", d5_ind)]:
        df[label] = df[inds].mean(axis=1) if inds else np.nan

    peso_total = sum(DIM_PESOS_SMS[d] for d in ["D1", "D2", "D3", "D4", "D5"]
                     if f"{d}g" in df.columns and df[f"{d}g"].notna().any())
    df["ivs_global_sms"] = sum(
        df[f"{d}g"] * DIM_PESOS_SMS[d]
        for d in ["D1", "D2", "D3", "D4", "D5"]
        if f"{d}g" in df.columns and df[f"{d}g"].notna().any()
    ) / peso_total

    # ── Variante C: 3 dimensões iguais (estilo IPEA) ─────────────────────────
    infra_ind    = [c for c in IPEA_DIMS["infra"]    if df[c].notna().any()]
    cap_hum_ind  = [c for c in IPEA_DIMS["cap_hum"]  if df[c].notna().any()]
    social_ind   = [c for c in IPEA_DIMS["social"]   if df[c].notna().any()]

    df["_infra"]   = df[infra_ind].mean(axis=1)   if infra_ind   else np.nan
    df["_cap_hum"] = df[cap_hum_ind].mean(axis=1) if cap_hum_ind else np.nan
    df["_social"]  = df[social_ind].mean(axis=1)  if social_ind  else np.nan

    dims_presentes = [c for c in ["_infra", "_cap_hum", "_social"]
                      if df[c].notna().any()]
    df["ivs_global_ipea"] = df[dims_presentes].mean(axis=1)

    return (df.groupby("ibge7")[["ivs_global_sms", "ivs_global_ipea"]]
              .mean().reset_index())


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ano", type=int, default=2010,
                        choices=[2000, 2010], help="Ano do Atlas IPEA (padrão: 2010)")
    args = parser.parse_args()

    our = load_manifest()
    print(f"\nMunicípios no manifesto : {len(our)}")
    if our.empty:
        sys.exit("Nenhum município encontrado em docs/data/municipios.json.gz")

    print("Carregando indicadores brutos por UBS...")
    ubs_raw = load_ubs_indicators()
    print(f"  {len(ubs_raw)} UBSs em {ubs_raw['ibge7'].nunique()} municípios")

    ivs_global = calcular_ivs_global(ubs_raw)

    ipea = load_ipea(args.ano)
    print(f"Municípios IPEA ({args.ano}): {len(ipea)}")

    # join
    merged = our.merge(ivs_global, on="ibge7", how="left")
    merged = merged.merge(ipea, on="ibge7", how="inner")
    merged = merged.dropna(subset=["ivs_medio", "ivs_ipea"])

    print(f"\nPares encontrados       : {len(merged)}")
    if len(merged) < 3:
        sys.exit("Pares insuficientes para calcular correlação (< 3).")

    # ── Correlações comparativas ─────────────────────────────────────────────
    print("\n" + "=" * 72)
    print(f"CORRELAÇÕES vs. IPEA {args.ano}  (Pearson r / Spearman ρ)")
    print("=" * 72)
    print(corr_row("A) ivs_medio (por-mun, SMS-POA)",  merged["ivs_medio"],       merged["ivs_ipea"]))
    print(corr_row("B) ivs_global_sms (global, SMS)",   merged["ivs_global_sms"],  merged["ivs_ipea"]))
    print(corr_row("C) ivs_global_ipea (global, 3dim)", merged["ivs_global_ipea"], merged["ivs_ipea"]))

    # ── Correlação das dimensões IPEA com nossas dimensões globais ───────────
    print("\n" + "=" * 72)
    print("CORRELAÇÃO DAS DIMENSÕES IPEA vs. NOSSAS (global)")
    print("=" * 72)
    ubs_norm = normalizar_global(ubs_raw, ["D2_sem_saneam", "D2_sem_lixo",
                                           "D1_analf", "D2_fora_creche", "D2_fora_fund", "D4_adol_fem",
                                           "D3_osc_per1k", "D5_idosos"])
    ubs_norm["_infra"]   = ubs_norm[["D2_sem_saneam", "D2_sem_lixo"]].mean(axis=1)
    ubs_norm["_cap_hum"] = ubs_norm[["D1_analf", "D2_fora_creche", "D2_fora_fund", "D4_adol_fem"]].mean(axis=1)
    ubs_norm["_social"]  = ubs_norm[["D3_osc_per1k", "D5_idosos"]].mean(axis=1)
    dim_mun = ubs_norm.groupby("ibge7")[["_infra", "_cap_hum", "_social"]].mean().reset_index()
    m2 = merged.merge(dim_mun, on="ibge7", how="left")
    print(corr_row("Infra nosso   × Infra IPEA",   m2["_infra"],   m2["infra_ipea"]))
    print(corr_row("CapHum nosso  × CapHum IPEA",  m2["_cap_hum"], m2["cap_hum_ipea"]))
    print(corr_row("Social nosso  × Renda IPEA",   m2["_social"],  m2["renda_ipea"]))

    # ── Estatísticas descritivas ─────────────────────────────────────────────
    print("\n" + "=" * 72)
    print("ESTATÍSTICAS DESCRITIVAS")
    print("=" * 72)
    for label, col in [
        ("A ivs_medio",       "ivs_medio"),
        ("B ivs_global_sms",  "ivs_global_sms"),
        ("C ivs_global_ipea", "ivs_global_ipea"),
        (f"IPEA {args.ano}",  "ivs_ipea"),
    ]:
        s = merged[col].dropna()
        print(f"  {label:<22}: min={s.min():.3f}  max={s.max():.3f}  "
              f"média={s.mean():.3f}  dp={s.std():.3f}")

    # ── Salvar CSV ──────────────────────────────────────────────────────────
    out = ROOT / "data" / "reference" / f"ivs_comparacao_ipea_{args.ano}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    merged.sort_values("ivs_ipea", ascending=False).to_csv(out, index=False, float_format="%.4f")
    print(f"\nCSV salvo em: {out}")


if __name__ == "__main__":
    main()
