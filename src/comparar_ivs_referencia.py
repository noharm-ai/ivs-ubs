"""
Compara o IVS calculado (Censo 2022 + INEP) com a referência SMS-POA 2019.
Calcula correlações de Pearson e Spearman entre os scores de IVS e por dimensão.

Uso:
    python src/comparar_ivs_referencia.py
"""

import json
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
from scipy.stats import pearsonr, spearmanr

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
IVS_FILE = ROOT / "ivs_poa" / "data" / "processed" / "ivs_poa.csv"
REF_FILE = ROOT / "data" / "reference" / "ivs_poa_sms_2019.json"

# ── Replicar lógica de gerar_pagina_municipio.py ───────────────────────────────
INDICADORES = {
    "D1_analf":       ("D1",),
    "D1_negros":      ("D1",),
    "D2_sem_saneam":  ("D2",),
    "D2_sem_lixo":    ("D2",),
    "D2_fora_creche": ("D2",),
    "D2_fora_fund":   ("D2",),
    "D3_osc_per1k":   ("D3",),
    "D4_adol_fem":    ("D4",),
    "D5_menor1":      ("D5",),
    "D5_adol":        ("D5",),
    "D5_mif":         ("D5",),
    "D5_idosos":      ("D5",),
}
INDICADORES_INVERSOS = {"D3_osc_per1k"}
DIM_PESOS = {"D1": 0.50, "D2": 0.30, "D3": 0.10, "D4": 0.08, "D5": 0.02}


def normalizar_mm(serie: pd.Series) -> pd.Series:
    mn, mx = serie.min(), serie.max()
    if mx == mn:
        return pd.Series(0.5, index=serie.index)
    return (serie - mn) / (mx - mn)


def calcular_dimensoes(df: pd.DataFrame) -> pd.DataFrame:
    dim_scores: dict[str, list[pd.Series]] = {}
    for col, (dim, *_) in INDICADORES.items():
        if col not in df.columns:
            continue
        norm = normalizar_mm(df[col].fillna(df[col].median()))
        if col in INDICADORES_INVERSOS:
            norm = 1 - norm
        dim_scores.setdefault(dim, []).append(norm)

    result = pd.DataFrame(index=df.index)
    for d in ("D1", "D2", "D3", "D4", "D5"):
        if d in dim_scores:
            vs = dim_scores[d]
            result[d] = sum(vs) / len(vs)
        else:
            result[d] = float("nan")

    total_peso = sum(DIM_PESOS[d] for d in dim_scores)
    result["ivs_parcial"] = (
        sum(result[d] * DIM_PESOS[d] for d in dim_scores if d in DIM_PESOS)
        / total_peso
    )
    return result


# ── Matching de nomes ──────────────────────────────────────────────────────────
def normalizar_nome(nome: str) -> str:
    """Remove prefixos comuns e normaliza para comparação."""
    nome = nome.upper()
    for prefix in [
        "CLINICA DA FAMILIA ", "CLINICA DA FAMÍLIA ", "CSF ", "US ", "UBS ",
        "UNIDADE DE SAUDE ", "UNIDADE DE SAÚDE ", "POSTO DE SAUDE ",
        "POSTO DE SAÚDE ",
    ]:
        if nome.startswith(prefix):
            nome = nome[len(prefix):]
            break
    transl = str.maketrans("ÁÀÃÂÉÊÍÓÔÕÚÜÇ", "AAAAEEIOOOUUC")
    return nome.translate(transl).strip()


def melhor_match(nome_ref: str, candidatos: list[str], threshold: float = 0.55):
    """Retorna (índice, score) do melhor match por similaridade de string."""
    nr = normalizar_nome(nome_ref)
    melhor_idx, melhor_score = -1, 0.0
    for i, c in enumerate(candidatos):
        score = SequenceMatcher(None, nr, normalizar_nome(c)).ratio()
        if score > melhor_score:
            melhor_score, melhor_idx = score, i
    if melhor_score >= threshold:
        return melhor_idx, melhor_score
    return -1, melhor_score


def interpret(r: float) -> str:
    ar = abs(r)
    if ar >= 0.80:
        return "muito forte"
    if ar >= 0.60:
        return "forte"
    if ar >= 0.40:
        return "moderada"
    if ar >= 0.20:
        return "fraca"
    return "muito fraca"


def sig_label(p: float) -> str:
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    return "ns"


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    # 1. Carregar e calcular IVS
    df_raw = pd.read_csv(IVS_FILE)
    scores = calcular_dimensoes(df_raw)
    df_calc = pd.concat([df_raw[["id_ubs", "no_ubs"]], scores], axis=1)

    # 2. Carregar referência 2019
    with open(REF_FILE) as f:
        ref_data = json.load(f)
    df_ref = pd.DataFrame(ref_data["unidades_saude"])
    df_ref.rename(
        columns={"d1": "D1_ref", "d2": "D2_ref", "d3": "D3_ref",
                 "d4": "D4_ref", "d5": "D5_ref", "ivs": "ivs_ref"},
        inplace=True,
    )

    print(f"\nIVS calculado : {len(df_calc)} UBS")
    print(f"IVS ref. 2019 : {len(df_ref)} UBS\n")

    # 3. Match por similaridade de nome
    nomes_calc = df_calc["no_ubs"].tolist()
    matches = []
    sem_match = []

    for _, row_ref in df_ref.iterrows():
        idx, score = melhor_match(row_ref["nome"], nomes_calc)
        if idx >= 0:
            calc_row = df_calc.iloc[idx]
            matches.append({
                "nome_ref":    row_ref["nome"],
                "nome_calc":   calc_row["no_ubs"],
                "score_match": round(score, 3),
                "id_ubs":      calc_row["id_ubs"],
                "ivs_calc":    calc_row["ivs_parcial"],
                "D1_calc": calc_row["D1"], "D2_calc": calc_row["D2"],
                "D3_calc": calc_row["D3"], "D4_calc": calc_row["D4"],
                "D5_calc": calc_row["D5"],
                "ivs_ref":     row_ref["ivs_ref"],
                "D1_ref": row_ref["D1_ref"], "D2_ref": row_ref["D2_ref"],
                "D3_ref": row_ref["D3_ref"], "D4_ref": row_ref["D4_ref"],
                "D5_ref": row_ref["D5_ref"],
            })
        else:
            sem_match.append((row_ref["nome"], round(score, 3)))

    df_m = pd.DataFrame(matches)

    print(f"UBS com match     : {len(df_m)}")
    print(f"UBS sem match (<0.55): {len(sem_match)}")
    if sem_match:
        print("  Sem match:")
        for nome, sc in sorted(sem_match, key=lambda x: x[1], reverse=True):
            print(f"    [{sc:.2f}] {nome}")

    # 4. Correlações — IVS geral
    print("\n" + "=" * 62)
    print("CORRELAÇÃO — IVS GERAL")
    print("=" * 62)
    valid = df_m[["ivs_calc", "ivs_ref"]].dropna()
    n = len(valid)

    rp, pp = pearsonr(valid["ivs_calc"], valid["ivs_ref"])
    rs, ps = spearmanr(valid["ivs_calc"], valid["ivs_ref"])

    print(f"  N pares   : {n}")
    print(f"  Pearson  r = {rp:+.4f}   p = {pp:.4g}  ({sig_label(pp)})  → {interpret(rp)}")
    print(f"  Spearman ρ = {rs:+.4f}   p = {ps:.4g}  ({sig_label(ps)})  → {interpret(rs)}")

    # 5. Correlações por dimensão
    print("\n" + "=" * 62)
    print("CORRELAÇÃO POR DIMENSÃO")
    print("=" * 62)
    print(f"  {'Dim':<4}  {'Pearson r':>10}  {'p':>8}       {'Spearman ρ':>10}  {'p':>8}     N")
    print(f"  {'-'*4}  {'-'*10}  {'-'*8}       {'-'*10}  {'-'*8}  ----")
    for d in ("D1", "D2", "D3", "D4", "D5"):
        sub = df_m[[f"{d}_calc", f"{d}_ref"]].dropna()
        if len(sub) < 5:
            print(f"  {d:<4}  {'—':>10}  {'—':>8}       {'—':>10}  {'—':>8}  {len(sub):>4}")
            continue
        rp_, pp_ = pearsonr(sub[f"{d}_calc"], sub[f"{d}_ref"])
        rs_, ps_ = spearmanr(sub[f"{d}_calc"], sub[f"{d}_ref"])
        print(
            f"  {d:<4}  {rp_:+10.4f}  {pp_:>8.4g} {sig_label(pp_):<3}     "
            f"{rs_:+10.4f}  {ps_:>8.4g} {sig_label(ps_):<3}  {len(sub):>4}"
        )

    # 6. Maiores discrepâncias de IVS
    df_m["diff_ivs"]  = (df_m["ivs_calc"] - df_m["ivs_ref"]).abs()
    df_m["rank_calc"] = df_m["ivs_calc"].rank(ascending=False).astype(int)
    df_m["rank_ref"]  = df_m["ivs_ref"].rank(ascending=False).astype(int)
    df_m["rank_diff"] = (df_m["rank_calc"] - df_m["rank_ref"]).abs()

    print("\n" + "=" * 62)
    print("MAIORES DISCREPÂNCIAS (|IVS_calc − IVS_ref|)")
    print("=" * 62)
    for _, r in df_m.nlargest(10, "diff_ivs").iterrows():
        print(
            f"  {r['nome_ref'][:42]:<42}  "
            f"calc={r['ivs_calc']:.3f}  ref={r['ivs_ref']:.3f}  "
            f"Δ={r['diff_ivs']:.3f}  "
            f"rank {r['rank_calc']}→{r['rank_ref']}"
        )

    print("\n" + "=" * 62)
    print("MAIS CONSISTENTES (menor discrepância)")
    print("=" * 62)
    for _, r in df_m.nsmallest(10, "diff_ivs").iterrows():
        print(
            f"  {r['nome_ref'][:42]:<42}  "
            f"calc={r['ivs_calc']:.3f}  ref={r['ivs_ref']:.3f}  "
            f"Δ={r['diff_ivs']:.3f}  "
            f"rank {r['rank_calc']}→{r['rank_ref']}"
        )

    # 7. Salvar CSV
    out = ROOT / "data" / "reference" / "ivs_poa_comparacao.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df_m.sort_values("ivs_ref", ascending=False).to_csv(out, index=False, float_format="%.4f")
    print(f"\nComparação completa salva em: {out}")


if __name__ == "__main__":
    main()
