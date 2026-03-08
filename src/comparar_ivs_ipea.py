"""
Compara o IVS calculado (IVSaúde por território de UBS, média municipal)
com o IVS IPEA (Atlas do Desenvolvimento Humano, nível municipal).

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

import pandas as pd
from scipy.stats import pearsonr, spearmanr

ROOT = Path(__file__).resolve().parents[1]
IPEA_FILE = ROOT / "data" / "basecompletamunicipal" / "atlasivs_dadosbrutos_pt_v2.xlsx"
MANIFEST   = ROOT / "docs" / "data" / "municipios.json.gz"


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
    """Converte valor IPEA (pode ter vírgula decimal) para float."""
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError):
        return None


# ── carregamento ───────────────────────────────────────────────────────────────

def load_manifest() -> pd.DataFrame:
    data = json.loads(gzip.decompress(MANIFEST.read_bytes()))
    df = pd.DataFrame(data)
    # normaliza código IBGE para 7 dígitos (string)
    df["ibge7"] = df["ibge"].astype(str).str.zfill(7)
    return df[["slug", "nome", "uf", "ibge7", "ivs_medio", "n_ubs"]]


def load_ipea(ano: int) -> pd.DataFrame:
    print(f"Lendo IPEA ({IPEA_FILE.name}) — pode levar alguns segundos...")
    raw = pd.read_excel(IPEA_FILE)
    mun = raw[raw["nivel"] == "regiao,uf,rm,municipio"].copy()
    mun = mun[mun["ano"] == ano].copy()
    # filtrar apenas o agregado total (sem desagregação por cor/sexo/situação de domicílio)
    mun = mun[
        (mun["label_cor"]     == "Total Cor") &
        (mun["label_sexo"]    == "Total Sexo") &
        (mun["label_sit_dom"] == "Total Situação de Domicílio")
    ].copy()

    # converte colunas numéricas
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

    ipea = load_ipea(args.ano)
    print(f"Municípios IPEA ({args.ano}): {len(ipea)}")

    # join por código IBGE de 7 dígitos
    merged = our.merge(ipea, on="ibge7", how="inner")
    merged = merged.dropna(subset=["ivs_medio", "ivs_ipea"])

    print(f"\nPares encontrados       : {len(merged)}")
    if len(merged) < 3:
        sys.exit("Pares insuficientes para calcular correlação (< 3).")

    # ── Correlação IVS geral ────────────────────────────────────────────────
    print("\n" + "=" * 62)
    print(f"CORRELAÇÃO IVS GERAL  (nosso ivs_medio  ×  IPEA ivs {args.ano})")
    print("=" * 62)

    rp, pp = pearsonr(merged["ivs_medio"], merged["ivs_ipea"])
    rs, ps = spearmanr(merged["ivs_medio"], merged["ivs_ipea"])
    n = len(merged)

    print(f"  N pares   : {n}")
    print(f"  Pearson  r = {rp:+.4f}   p = {pp:.4g}  ({sig(pp)})  → {interpret(rp)}")
    print(f"  Spearman ρ = {rs:+.4f}   p = {ps:.4g}  ({sig(ps)})  → {interpret(rs)}")

    # ── Tabela individual ────────────────────────────────────────────────────
    print("\n" + "=" * 62)
    print("DETALHE POR MUNICÍPIO")
    print("=" * 62)
    merged["diff"] = (merged["ivs_medio"] - merged["ivs_ipea"]).round(3)
    merged["rank_nosso"] = merged["ivs_medio"].rank(ascending=False).astype(int)
    merged["rank_ipea"]  = merged["ivs_ipea"].rank(ascending=False).astype(int)
    cols_show = ["nome", "uf", "n_ubs", "ivs_medio", "ivs_ipea", "diff",
                 "rank_nosso", "rank_ipea"]
    print(merged[cols_show].sort_values("ivs_ipea", ascending=False).to_string(index=False))

    # ── Estatísticas descritivas ─────────────────────────────────────────────
    print("\n" + "=" * 62)
    print("ESTATÍSTICAS DESCRITIVAS")
    print("=" * 62)
    for label, col in [("IVS nosso (ivs_medio)", "ivs_medio"), (f"IVS IPEA {args.ano}", "ivs_ipea")]:
        s = merged[col]
        print(f"  {label}:")
        print(f"    min={s.min():.3f}  max={s.max():.3f}  média={s.mean():.3f}  dp={s.std():.3f}")

    # ── Salvar CSV ──────────────────────────────────────────────────────────
    out = ROOT / "data" / "reference" / f"ivs_comparacao_ipea_{args.ano}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    merged.sort_values("ivs_ipea", ascending=False).to_csv(out, index=False, float_format="%.4f")
    print(f"\nCSV salvo em: {out}")


if __name__ == "__main__":
    main()
