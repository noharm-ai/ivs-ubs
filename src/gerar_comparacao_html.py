"""
Gera HTML de comparação entre o IVS calculado (Censo 2022) e a referência SMS-POA 2019.
Inclui scatter plot interativo, tabela detalhada e estatísticas de correlação.

Uso:
    python src/gerar_comparacao_html.py
"""

import json
from difflib import SequenceMatcher
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr

ROOT = Path(__file__).resolve().parents[1]
IVS_FILE = ROOT / "ivs_poa" / "data" / "processed" / "ivs_poa.csv"
REF_FILE = ROOT / "data" / "reference" / "ivs_poa_sms_2019.json"
OUT_HTML = ROOT / "comparacao-ivs-poa.html"

INDICADORES = {
    "D1_analf": "D1", "D1_negros": "D1",
    "D2_sem_saneam": "D2", "D2_sem_lixo": "D2",
    "D2_fora_creche": "D2", "D2_fora_fund": "D2",
    "D3_osc_per1k": "D3",
    "D4_adol_fem": "D4",
    "D5_menor1": "D5", "D5_adol": "D5", "D5_mif": "D5", "D5_idosos": "D5",
}
INDICADORES_INVERSOS = {"D3_osc_per1k"}
DIM_PESOS = {"D1": 0.50, "D2": 0.30, "D3": 0.10, "D4": 0.08, "D5": 0.02}
DIM_LABELS = {
    "D1": "D1 — Condição Socioeconômica",
    "D2": "D2 — Habitação e Saneamento",
    "D3": "D3 — Capital Social",
    "D4": "D4 — Saúde do Adolescente",
    "D5": "D5 — Perfil Demográfico",
}


def normalizar_mm(serie: pd.Series) -> pd.Series:
    mn, mx = serie.min(), serie.max()
    if mx == mn:
        return pd.Series(0.5, index=serie.index)
    return (serie - mn) / (mx - mn)


def calcular_dimensoes(df: pd.DataFrame) -> pd.DataFrame:
    dim_scores: dict = {}
    for col, dim in INDICADORES.items():
        if col not in df.columns:
            continue
        norm = normalizar_mm(df[col].fillna(df[col].median()))
        if col in INDICADORES_INVERSOS:
            norm = 1 - norm
        dim_scores.setdefault(dim, []).append(norm)

    result = pd.DataFrame(index=df.index)
    for d in ("D1", "D2", "D3", "D4", "D5"):
        result[d] = sum(dim_scores[d]) / len(dim_scores[d]) if d in dim_scores else float("nan")

    total_peso = sum(DIM_PESOS[d] for d in dim_scores)
    result["ivs_parcial"] = (
        sum(result[d] * DIM_PESOS[d] for d in dim_scores if d in DIM_PESOS) / total_peso
    )
    return result


def normalizar_nome(nome: str) -> str:
    nome = nome.upper()
    for p in ["CLINICA DA FAMILIA ", "CLINICA DA FAMÍLIA ", "CSF ", "US ", "UBS ",
              "UNIDADE DE SAUDE ", "UNIDADE DE SAÚDE ", "POSTO DE SAUDE ", "POSTO DE SAÚDE "]:
        if nome.startswith(p):
            nome = nome[len(p):]
            break
    return nome.translate(str.maketrans("ÁÀÃÂÉÊÍÓÔÕÚÜÇ", "AAAAEEIOOOUUC")).strip()


def melhor_match(nome_ref, candidatos, threshold=0.55):
    nr = normalizar_nome(nome_ref)
    best_idx, best_score = -1, 0.0
    for i, c in enumerate(candidatos):
        s = SequenceMatcher(None, nr, normalizar_nome(c)).ratio()
        if s > best_score:
            best_score, best_idx = s, i
    return (best_idx, best_score) if best_score >= threshold else (-1, best_score)


def sig_label(p):
    if p < 0.001: return "***"
    if p < 0.01:  return "**"
    if p < 0.05:  return "*"
    return "ns"


def interpret(r):
    a = abs(r)
    if a >= 0.80: return "muito forte"
    if a >= 0.60: return "forte"
    if a >= 0.40: return "moderada"
    if a >= 0.20: return "fraca"
    return "muito fraca"


def cor_ivs(v):
    stops = [(0.0, (39,174,96)), (0.33, (241,196,15)), (0.66, (230,126,34)), (1.0, (192,57,43))]
    for i in range(len(stops) - 1):
        v0, c0 = stops[i]
        v1, c1 = stops[i + 1]
        if v <= v1:
            t = (v - v0) / (v1 - v0) if v1 > v0 else 0
            r = int(c0[0] + t * (c1[0] - c0[0]))
            g = int(c0[1] + t * (c1[1] - c0[1]))
            b = int(c0[2] + t * (c1[2] - c0[2]))
            return f"#{r:02x}{g:02x}{b:02x}"
    return "#c0392b"


def build_html(df_m: pd.DataFrame, corr_geral: dict, corr_dim: dict) -> str:
    # ── scatter data ───────────────────────────────────────────────────────────
    scatter_points = []
    for _, r in df_m.iterrows():
        scatter_points.append({
            "x": round(float(r["ivs_ref"]), 4),
            "y": round(float(r["ivs_calc"]), 4),
            "nome": r["nome_ref"],
            "rank_calc": int(r["rank_calc"]),
            "rank_ref": int(r["rank_ref"]),
            "diff": round(float(r["diff_ivs"]), 4),
            "color": cor_ivs(float(r["ivs_ref"])),
        })

    scatter_json = json.dumps(scatter_points)

    # ── dim scatter data ───────────────────────────────────────────────────────
    dim_scatter = {}
    for d in ("D1", "D2", "D3", "D4", "D5"):
        pts = []
        sub = df_m[[f"{d}_calc", f"{d}_ref", "nome_ref"]].dropna()
        for _, r in sub.iterrows():
            pts.append({
                "x": round(float(r[f"{d}_ref"]), 4),
                "y": round(float(r[f"{d}_calc"]), 4),
                "nome": r["nome_ref"],
            })
        dim_scatter[d] = pts

    dim_scatter_json = json.dumps(dim_scatter)

    # ── tabela rows ────────────────────────────────────────────────────────────
    def bar(v, cor=None):
        if pd.isna(v): return '<span style="color:#aaa">—</span>'
        c = cor or cor_ivs(float(v))
        pct = int(float(v) * 100)
        return (f'<div class="bar-cell">'
                f'<div class="bar-bg"><div class="bar-fill" style="width:{pct}%;background:{c}"></div></div>'
                f'<span>{float(v):.3f}</span></div>')

    def delta_badge(d):
        if pd.isna(d): return "—"
        d = float(d)
        color = "#e74c3c" if d > 0.2 else ("#e67e22" if d > 0.1 else "#27ae60")
        return f'<span style="color:{color};font-weight:600">Δ {d:.3f}</span>'

    rows = ""
    for _, r in df_m.sort_values("ivs_ref", ascending=False).iterrows():
        rd = int(r["rank_diff"])
        rank_arrow = "↑" if r["rank_calc"] < r["rank_ref"] else ("↓" if r["rank_calc"] > r["rank_ref"] else "=")
        rank_col = f'<span style="font-size:.8em;color:#666">{rank_arrow} {int(r["rank_ref"])}→{int(r["rank_calc"])}</span>'
        nome_curto = r["nome_ref"].replace("US ", "").replace("CSF ", "").title()
        rows += f"""
        <tr>
          <td style="font-size:.85em">{nome_curto}</td>
          <td>{bar(r['ivs_ref'])}</td>
          <td>{bar(r['ivs_calc'])}</td>
          <td>{delta_badge(r['diff_ivs'])}</td>
          <td>{rank_col}</td>
          <td style="font-size:.8em;color:#888">{bar(r['D1_ref'], '#7f8c8d')}</td>
          <td style="font-size:.8em;color:#888">{bar(r['D1_calc'], '#7f8c8d')}</td>
          <td style="font-size:.8em;color:#888">{bar(r['D2_ref'], '#7f8c8d')}</td>
          <td style="font-size:.8em;color:#888">{bar(r['D2_calc'], '#7f8c8d')}</td>
        </tr>"""

    # ── stat cards correlação por dim ──────────────────────────────────────────
    dim_cards = ""
    for d, label in DIM_LABELS.items():
        info = corr_dim.get(d, {})
        rp = info.get("pearson_r")
        rs = info.get("spearman_r")
        pp = info.get("pearson_p")
        ps = info.get("spearman_p")
        n = info.get("n", 0)

        if rp is None or np.isnan(rp):
            content = '<p style="color:#aaa;font-size:.85em">Dados constantes na referência — correlação indefinida</p>'
            border = "#bdc3c7"
        else:
            strength = interpret(rp)
            border = "#27ae60" if abs(rp) >= 0.6 else ("#f39c12" if abs(rp) >= 0.4 else "#e74c3c")
            content = f"""
              <div class="corr-row">
                <span class="corr-method">Pearson</span>
                <span class="corr-val" style="color:{border}">r = {rp:+.3f}</span>
                <span class="corr-p">p = {pp:.3g} {sig_label(pp)}</span>
              </div>
              <div class="corr-row">
                <span class="corr-method">Spearman</span>
                <span class="corr-val" style="color:{border}">ρ = {rs:+.3f}</span>
                <span class="corr-p">p = {ps:.3g} {sig_label(ps)}</span>
              </div>
              <p class="corr-interp">Correlação <strong>{strength}</strong> · N={n}</p>"""

        dim_cards += f"""
        <div class="dim-card" style="border-top:3px solid {border}">
          <div class="dim-card-title">{label}</div>
          {content}
        </div>"""

    rp_g  = corr_geral["pearson_r"]
    rs_g  = corr_geral["spearman_r"]
    pp_g  = corr_geral["pearson_p"]
    ps_g  = corr_geral["spearman_p"]
    n_g   = corr_geral["n"]
    interp_g = interpret(rp_g)
    border_g = "#27ae60" if abs(rp_g) >= 0.6 else "#f39c12"

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Comparação IVSaúde — Calculado vs SMS-POA 2019</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         background: #f0f2f5; color: #2c3e50; font-size: 14px; }}
  .header {{ background: linear-gradient(135deg, #1a5276 0%, #2980b9 100%);
             color: #fff; padding: 32px 40px; }}
  .header h1 {{ font-size: 1.7em; margin-bottom: 6px; }}
  .header p {{ opacity: .85; font-size: .95em; }}
  .container {{ max-width: 1300px; margin: 0 auto; padding: 28px 20px; }}
  .section-title {{ font-size: 1.05em; font-weight: 700; color: #1a5276;
                    border-left: 4px solid #2980b9; padding-left: 10px;
                    margin: 32px 0 16px; text-transform: uppercase; letter-spacing: .04em; }}
  /* stat cards geral */
  .cards-row {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 8px; }}
  .card {{ background: #fff; border-radius: 8px; padding: 20px 24px;
           box-shadow: 0 1px 4px rgba(0,0,0,.08); flex: 1; min-width: 180px; }}
  .card-label {{ font-size: .78em; color: #7f8c8d; text-transform: uppercase;
                 letter-spacing: .05em; margin-bottom: 6px; }}
  .card-value {{ font-size: 2em; font-weight: 700; color: {border_g}; }}
  .card-sub {{ font-size: .8em; color: #95a5a6; margin-top: 4px; }}
  /* scatter */
  .chart-box {{ background: #fff; border-radius: 8px; padding: 20px;
                box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-bottom: 24px; }}
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
  @media (max-width: 900px) {{ .chart-grid {{ grid-template-columns: 1fr; }} }}
  /* dim cards */
  .dim-cards-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
                     gap: 16px; margin-bottom: 24px; }}
  .dim-card {{ background: #fff; border-radius: 8px; padding: 16px 18px;
               box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
  .dim-card-title {{ font-size: .82em; font-weight: 700; color: #2c3e50;
                     margin-bottom: 10px; }}
  .corr-row {{ display: flex; align-items: center; gap: 8px; margin-bottom: 5px; }}
  .corr-method {{ font-size: .78em; color: #7f8c8d; width: 60px; }}
  .corr-val {{ font-size: 1em; font-weight: 700; }}
  .corr-p {{ font-size: .78em; color: #7f8c8d; }}
  .corr-interp {{ font-size: .78em; color: #555; margin-top: 8px; }}
  /* dim scatter tabs */
  .tab-bar {{ display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 12px; }}
  .tab {{ cursor: pointer; padding: 5px 14px; border-radius: 20px; font-size: .82em;
          border: 1px solid #bdc3c7; background: #fff; color: #555; transition: all .15s; }}
  .tab.active {{ background: #2980b9; color: #fff; border-color: #2980b9; font-weight: 600; }}
  /* tabela */
  .table-wrap {{ overflow-x: auto; background: #fff; border-radius: 8px;
                 box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
  table {{ width: 100%; border-collapse: collapse; font-size: .83em; }}
  thead th {{ background: #1a5276; color: #fff; padding: 10px 10px; text-align: left;
              font-size: .8em; font-weight: 600; white-space: nowrap; position: sticky; top: 0; }}
  tbody tr:nth-child(even) {{ background: #f8f9fa; }}
  tbody tr:hover {{ background: #eaf3fb; }}
  td {{ padding: 7px 10px; border-bottom: 1px solid #ecf0f1; vertical-align: middle; }}
  .bar-cell {{ display: flex; align-items: center; gap: 6px; min-width: 90px; }}
  .bar-bg {{ flex: 1; height: 5px; background: #ecf0f1; border-radius: 3px; }}
  .bar-fill {{ height: 100%; border-radius: 3px; }}
  .bar-cell span {{ font-size: .82em; min-width: 34px; text-align: right; font-weight: 600; }}
  .note {{ font-size: .8em; color: #7f8c8d; margin-top: 8px; line-height: 1.5; }}
</style>
</head>
<body>

<div class="header">
  <h1>Validação do IVSaúde — Porto Alegre</h1>
  <p>Comparação entre o IVS calculado (Censo IBGE 2022 + INEP) e a referência SMS-POA 2019 &nbsp;·&nbsp; {n_g} UBS pareadas</p>
</div>

<div class="container">

  <!-- Cards de correlação geral -->
  <div class="section-title">Correlação Global — IVS</div>
  <div class="cards-row">
    <div class="card">
      <div class="card-label">Pearson r</div>
      <div class="card-value">{rp_g:+.3f}</div>
      <div class="card-sub">p = {pp_g:.2g} {sig_label(pp_g)} &nbsp;·&nbsp; {interp_g}</div>
    </div>
    <div class="card">
      <div class="card-label">Spearman ρ</div>
      <div class="card-value">{rs_g:+.3f}</div>
      <div class="card-sub">p = {ps_g:.2g} {sig_label(ps_g)} &nbsp;·&nbsp; {interpret(rs_g)}</div>
    </div>
    <div class="card">
      <div class="card-label">UBS pareadas</div>
      <div class="card-value" style="color:#2c3e50">{n_g}</div>
      <div class="card-sub">de 157 calculadas / 140 na referência</div>
    </div>
    <div class="card">
      <div class="card-label">Δ médio (|calc − ref|)</div>
      <div class="card-value" style="color:#e67e22">{df_m['diff_ivs'].mean():.3f}</div>
      <div class="card-sub">mediana {df_m['diff_ivs'].median():.3f} &nbsp;·&nbsp; máx {df_m['diff_ivs'].max():.3f}</div>
    </div>
  </div>

  <!-- Scatter principal -->
  <div class="section-title">Scatter — IVS Calculado vs. Referência 2019</div>
  <div class="chart-box">
    <canvas id="scatterMain" height="380"></canvas>
    <p class="note">Cada ponto é uma UBS. Linha diagonal = concordância perfeita. Pontos acima da diagonal = IVS calculado maior que a referência; abaixo = calculado menor.</p>
  </div>

  <!-- Correlação por dimensão -->
  <div class="section-title">Correlação por Dimensão</div>
  <div class="dim-cards-grid">{dim_cards}</div>

  <!-- Scatter por dimensão -->
  <div class="chart-box">
    <div class="tab-bar">
      <div class="tab active" onclick="showDim('D1',this)">D1 — Socioeconômico</div>
      <div class="tab" onclick="showDim('D2',this)">D2 — Habitação</div>
      <div class="tab" onclick="showDim('D3',this)">D3 — Capital Social</div>
      <div class="tab" onclick="showDim('D4',this)">D4 — Adolescente</div>
      <div class="tab" onclick="showDim('D5',this)">D5 — Demográfico</div>
    </div>
    <canvas id="scatterDim" height="340"></canvas>
  </div>

  <!-- Tabela -->
  <div class="section-title">Tabela Comparativa por UBS</div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>UBS (referência)</th>
          <th>IVS ref. 2019</th>
          <th>IVS calc. 2022</th>
          <th>Diferença</th>
          <th>Rank ref→calc</th>
          <th>D1 ref</th>
          <th>D1 calc</th>
          <th>D2 ref</th>
          <th>D2 calc</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
  <p class="note" style="margin-top:12px">
    <strong>Notas:</strong>
    Pesos: D1=50%, D2=30%, D3=10%, D4=8%, D5=2%.
    D2 calc. inclui D2_fora_creche e D2_fora_fund (Censo Escolar INEP 2025), não disponíveis na referência 2019.
    D3 da referência 2019 usa valores discretos (0.25/0.50/0.75/1.00), tornando a correlação indefinida.
    Pareamento por similaridade de nome (threshold ≥ 0.55).
  </p>

</div>

<script>
const POINTS = {scatter_json};
const DIM_POINTS = {dim_scatter_json};

// ── Scatter principal ──────────────────────────────────────────────────────
const ctxMain = document.getElementById('scatterMain').getContext('2d');
const scatterMain = new Chart(ctxMain, {{
  type: 'scatter',
  data: {{
    datasets: [{{
      label: 'UBS',
      data: POINTS.map(p => ({{x: p.x, y: p.y, meta: p}})),
      backgroundColor: POINTS.map(p => p.color + 'cc'),
      borderColor: POINTS.map(p => p.color),
      borderWidth: 1,
      pointRadius: 5,
      pointHoverRadius: 8,
    }},
    {{
      label: 'Diagonal (concordância perfeita)',
      data: [{{x:0.2,y:0.2}},{{x:1.0,y:1.0}}],
      type: 'line',
      borderColor: '#bdc3c7',
      borderDash: [6,4],
      borderWidth: 1.5,
      pointRadius: 0,
      fill: false,
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        callbacks: {{
          label: ctx => {{
            if (!ctx.raw.meta) return '';
            const m = ctx.raw.meta;
            return [
              m.nome,
              `IVS ref 2019: ${{m.x.toFixed(3)}}`,
              `IVS calc 2022: ${{m.y.toFixed(3)}}`,
              `Δ = ${{m.diff.toFixed(3)}}`,
              `Rank: ref ${{m.rank_ref}} → calc ${{m.rank_calc}}`,
            ];
          }}
        }}
      }}
    }},
    scales: {{
      x: {{ title: {{display:true, text:'IVS Referência SMS-POA 2019'}}, min:0.1, max:1.0 }},
      y: {{ title: {{display:true, text:'IVS Calculado (Censo 2022)'}}, min:0.1, max:1.0 }},
    }}
  }}
}});

// ── Scatter por dimensão ───────────────────────────────────────────────────
const ctxDim = document.getElementById('scatterDim').getContext('2d');
let scatterDim = null;

function showDim(d, tab) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  tab.classList.add('active');
  const pts = DIM_POINTS[d] || [];
  const data = pts.map(p => ({{x: p.x, y: p.y, nome: p.nome}}));
  if (scatterDim) scatterDim.destroy();
  scatterDim = new Chart(ctxDim, {{
    type: 'scatter',
    data: {{
      datasets: [{{
        label: d,
        data: data,
        backgroundColor: '#2980b9aa',
        borderColor: '#2980b9',
        borderWidth: 1,
        pointRadius: 4,
        pointHoverRadius: 7,
      }},
      {{
        label: 'Diagonal',
        data: [{{x:0,y:0}},{{x:1,y:1}}],
        type: 'line',
        borderColor: '#bdc3c7',
        borderDash: [6,4],
        borderWidth: 1.5,
        pointRadius: 0,
        fill: false,
      }}]
    }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{display:false}},
        tooltip: {{
          callbacks: {{
            label: ctx => ctx.raw.nome
              ? [ctx.raw.nome, `ref: ${{ctx.raw.x.toFixed(3)}}`, `calc: ${{ctx.raw.y.toFixed(3)}}`]
              : ''
          }}
        }}
      }},
      scales: {{
        x: {{ title: {{display:true, text:`${{d}} — Referência 2019`}}, min:0, max:1 }},
        y: {{ title: {{display:true, text:`${{d}} — Calculado 2022`}}, min:0, max:1 }},
      }}
    }}
  }});
}}
showDim('D1', document.querySelector('.tab.active'));
</script>
</body>
</html>"""


def main():
    df_raw = pd.read_csv(IVS_FILE)
    scores = calcular_dimensoes(df_raw)
    df_calc = pd.concat([df_raw[["id_ubs", "no_ubs"]], scores], axis=1)

    with open(REF_FILE) as f:
        ref_data = json.load(f)
    df_ref = pd.DataFrame(ref_data["unidades_saude"])
    df_ref.rename(columns={"d1":"D1_ref","d2":"D2_ref","d3":"D3_ref",
                            "d4":"D4_ref","d5":"D5_ref","ivs":"ivs_ref"}, inplace=True)

    nomes_calc = df_calc["no_ubs"].tolist()
    matches = []
    for _, row_ref in df_ref.iterrows():
        idx, score = melhor_match(row_ref["nome"], nomes_calc)
        if idx >= 0:
            cr = df_calc.iloc[idx]
            matches.append({
                "nome_ref": row_ref["nome"], "nome_calc": cr["no_ubs"],
                "score_match": round(score, 3), "id_ubs": cr["id_ubs"],
                "ivs_calc": cr["ivs_parcial"],
                "D1_calc": cr["D1"], "D2_calc": cr["D2"], "D3_calc": cr["D3"],
                "D4_calc": cr["D4"], "D5_calc": cr["D5"],
                "ivs_ref": row_ref["ivs_ref"],
                "D1_ref": row_ref["D1_ref"], "D2_ref": row_ref["D2_ref"],
                "D3_ref": row_ref["D3_ref"], "D4_ref": row_ref["D4_ref"],
                "D5_ref": row_ref["D5_ref"],
            })

    df_m = pd.DataFrame(matches)
    df_m["diff_ivs"]  = (df_m["ivs_calc"] - df_m["ivs_ref"]).abs()
    df_m["rank_calc"] = df_m["ivs_calc"].rank(ascending=False).astype(int)
    df_m["rank_ref"]  = df_m["ivs_ref"].rank(ascending=False).astype(int)
    df_m["rank_diff"] = (df_m["rank_calc"] - df_m["rank_ref"]).abs()

    # Correlação geral
    valid = df_m[["ivs_calc", "ivs_ref"]].dropna()
    rp, pp = pearsonr(valid["ivs_calc"], valid["ivs_ref"])
    rs, ps = spearmanr(valid["ivs_calc"], valid["ivs_ref"])
    corr_geral = {"pearson_r": rp, "pearson_p": pp, "spearman_r": rs, "spearman_p": ps, "n": len(valid)}

    # Correlação por dimensão
    corr_dim = {}
    for d in ("D1", "D2", "D3", "D4", "D5"):
        sub = df_m[[f"{d}_calc", f"{d}_ref"]].dropna()
        if len(sub) < 5:
            corr_dim[d] = {"pearson_r": float("nan"), "n": len(sub)}
            continue
        try:
            rp_, pp_ = pearsonr(sub[f"{d}_calc"], sub[f"{d}_ref"])
            rs_, ps_ = spearmanr(sub[f"{d}_calc"], sub[f"{d}_ref"])
            corr_dim[d] = {"pearson_r": rp_, "pearson_p": pp_,
                           "spearman_r": rs_, "spearman_p": ps_, "n": len(sub)}
        except Exception:
            corr_dim[d] = {"pearson_r": float("nan"), "n": len(sub)}

    html = build_html(df_m, corr_geral, corr_dim)
    OUT_HTML.write_text(html, encoding="utf-8")
    print(f"HTML gerado: {OUT_HTML}  ({OUT_HTML.stat().st_size/1024:.1f}KB)")


if __name__ == "__main__":
    main()
