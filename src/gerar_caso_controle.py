"""
Gera página HTML interativa para design de estudo caso-controle.

Segmenta UBS de Porto Alegre, Pelotas e Betim em grupos caso/controle
com base no IVS, pareando dentro de cada cidade por score ponderado
de população e IVS.

Uso:
    python src/gerar_caso_controle.py
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT_HTML = ROOT / "caso-controle.html"

MUNICIPIOS = [
    {"slug": "poa",     "cidade": "Porto Alegre", "cor": "#2980b9"},
    {"slug": "pelotas", "cidade": "Pelotas",       "cor": "#8e44ad"},
    {"slug": "betim",   "cidade": "Betim",         "cor": "#16a085"},
]

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


def normalizar_mm(serie: pd.Series) -> pd.Series:
    mn, mx = serie.min(), serie.max()
    if mx == mn:
        return pd.Series(0.5, index=serie.index)
    return (serie - mn) / (mx - mn)


def calcular_ivs(df: pd.DataFrame) -> pd.DataFrame:
    dim_scores: dict = {}
    for col, dim in INDICADORES.items():
        if col not in df.columns:
            continue
        series = df[col]
        med = series.median()
        if pd.isna(med):
            continue  # coluna toda NaN — ignorar
        norm = normalizar_mm(series.fillna(med))
        if col in INDICADORES_INVERSOS:
            norm = 1 - norm
        dim_scores.setdefault(dim, []).append(norm)

    result = df[["id_ubs", "no_ubs", "pop_total"]].copy()
    for d in ("D1", "D2", "D3", "D4", "D5"):
        result[d] = sum(dim_scores[d]) / len(dim_scores[d]) if d in dim_scores else float("nan")

    # IVS ponderado apenas pelas dimensões com dados (normaliza pelo peso disponível)
    dims_disponiveis = [d for d in dim_scores if d in DIM_PESOS]
    if dims_disponiveis:
        peso_total = sum(DIM_PESOS[d] for d in dims_disponiveis)
        result["ivs"] = sum(result[d] * DIM_PESOS[d] for d in dims_disponiveis) / peso_total
    else:
        result["ivs"] = float("nan")
    return result


def load_all() -> list[dict]:
    records = []
    for m in MUNICIPIOS:
        slug = m["slug"]
        csv_path = ROOT / f"ivs_{slug}" / "data" / "processed" / f"ivs_{slug}.csv"
        if not csv_path.exists():
            print(f"[AVISO] Não encontrado: {csv_path}")
            continue
        df = pd.read_csv(csv_path)
        scored = calcular_ivs(df)
        for _, r in scored.iterrows():
            if pd.isna(r["ivs"]) or pd.isna(r["pop_total"]):
                continue
            records.append({
                "id":      str(r["id_ubs"]),
                "nome":    r["no_ubs"].title().replace("Ubs ", "UBS ").replace("Csf ", "CSF "),
                "cidade":  m["cidade"],
                "cor":     m["cor"],
                "pop":     round(float(r["pop_total"])),
                "ivs":     round(float(r["ivs"]), 4),
                "D1":      round(float(r["D1"]), 4) if pd.notna(r["D1"]) else None,
                "D2":      round(float(r["D2"]), 4) if pd.notna(r["D2"]) else None,
                "D3":      round(float(r["D3"]), 4) if pd.notna(r["D3"]) else None,
                "D4":      round(float(r["D4"]), 4) if pd.notna(r["D4"]) else None,
                "D5":      round(float(r["D5"]), 4) if pd.notna(r["D5"]) else None,
            })
    return records


def main():
    records = load_all()
    data_json = json.dumps(records, ensure_ascii=False)

    cidades = list(dict.fromkeys(r["cidade"] for r in records))
    totais = {c: sum(1 for r in records if r["cidade"] == c) for c in cidades}
    print(f"UBS carregadas: {', '.join(f'{c}={totais[c]}' for c in cidades)}")

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Estudo Caso-Controle — IVSaúde UBS</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: #f0f2f5; color: #2c3e50; font-size: 14px; }}

.header {{ background: linear-gradient(135deg, #1a5276 0%, #2980b9 100%);
           color: #fff; padding: 28px 40px; }}
.header h1 {{ font-size: 1.6em; margin-bottom: 5px; }}
.header p {{ opacity: .8; font-size: .9em; }}

.layout {{ display: flex; gap: 0; min-height: calc(100vh - 120px); }}

/* ── Painel de controle ── */
.sidebar {{ width: 300px; flex-shrink: 0; background: #fff;
            border-right: 1px solid #dde3ea;
            padding: 20px 18px; overflow-y: auto; }}
.sidebar h2 {{ font-size: .9em; font-weight: 700; color: #1a5276;
               text-transform: uppercase; letter-spacing: .05em;
               border-left: 3px solid #2980b9; padding-left: 8px;
               margin: 20px 0 12px; }}
.sidebar h2:first-child {{ margin-top: 0; }}

.control-group {{ margin-bottom: 14px; }}
.control-group label {{ display: block; font-size: .8em; color: #555;
                         margin-bottom: 4px; font-weight: 600; }}
.control-group input[type=range] {{ width: 100%; accent-color: #2980b9; }}
.control-group .range-val {{ font-size: .85em; color: #2980b9; font-weight: 700;
                              text-align: right; margin-top: 2px; }}
.control-group select, .control-group input[type=number] {{
  width: 100%; padding: 6px 8px; border: 1px solid #d0d7de;
  border-radius: 5px; font-size: .85em; background: #f8f9fa; }}

.btn {{ width: 100%; padding: 10px; border: none; border-radius: 6px;
        font-size: .9em; font-weight: 700; cursor: pointer; margin-top: 6px;
        transition: opacity .15s; }}
.btn:hover {{ opacity: .85; }}
.btn-primary {{ background: #2980b9; color: #fff; }}
.btn-secondary {{ background: #ecf0f1; color: #2c3e50; }}

.legend-box {{ display: flex; gap: 8px; flex-direction: column;
               font-size: .8em; margin-top: 10px; }}
.legend-item {{ display: flex; align-items: center; gap: 6px; }}
.legend-dot {{ width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0; }}

/* ── Conteúdo principal ── */
.main {{ flex: 1; padding: 20px; overflow-y: auto; }}

.stats-row {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }}
.stat-card {{ background: #fff; border-radius: 8px; padding: 14px 18px;
              box-shadow: 0 1px 3px rgba(0,0,0,.08); flex: 1; min-width: 140px; }}
.stat-card .label {{ font-size: .72em; color: #7f8c8d; text-transform: uppercase;
                     letter-spacing: .05em; margin-bottom: 4px; }}
.stat-card .value {{ font-size: 1.6em; font-weight: 700; }}
.stat-card .sub {{ font-size: .75em; color: #95a5a6; margin-top: 2px; }}

.section-title {{ font-size: .95em; font-weight: 700; color: #1a5276;
                  border-left: 4px solid #2980b9; padding-left: 10px;
                  margin: 24px 0 12px; text-transform: uppercase; letter-spacing: .04em; }}

/* ── Tabela ── */
.table-wrap {{ overflow-x: auto; background: #fff; border-radius: 8px;
               box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 20px; }}
table {{ width: 100%; border-collapse: collapse; font-size: .82em; }}
thead th {{ background: #1a5276; color: #fff; padding: 9px 10px; text-align: left;
            font-size: .78em; font-weight: 600; white-space: nowrap;
            position: sticky; top: 0; z-index: 1; }}
tbody tr:nth-child(even) {{ background: #f8f9fa; }}
tbody tr:hover {{ background: #eaf3fb; }}
td {{ padding: 7px 10px; border-bottom: 1px solid #ecf0f1; vertical-align: middle; }}
td.pair-sep {{ background: #dde3ea !important; height: 2px; padding: 0; }}

.badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px;
          font-size: .75em; font-weight: 700; color: #fff; white-space: nowrap; }}
.badge-caso     {{ background: #e74c3c; }}
.badge-controle {{ background: #2980b9; }}
.badge-cidade   {{ padding: 2px 6px; border-radius: 3px; font-size: .72em; font-weight: 600; color: #fff; }}

.ivs-bar {{ display: flex; align-items: center; gap: 6px; }}
.ivs-bg  {{ flex: 1; height: 5px; background: #ecf0f1; border-radius: 3px; min-width: 50px; }}
.ivs-fill {{ height: 100%; border-radius: 3px; }}
.ivs-val {{ font-size: .8em; font-weight: 600; min-width: 32px; text-align: right; }}

.dim-mini {{ display: flex; gap: 3px; }}
.dim-pill {{ font-size: .68em; padding: 1px 5px; border-radius: 3px;
             background: #ecf0f1; color: #555; white-space: nowrap; }}

.pop-val {{ font-size: .82em; color: #555; }}

/* ── Não pareadas ── */
.unpaired-list {{ display: flex; flex-wrap: wrap; gap: 8px; }}
.unpaired-chip {{ background: #fef9e7; border: 1px solid #f39c12;
                  border-radius: 5px; padding: 5px 10px; font-size: .78em; }}

.empty-msg {{ color: #aaa; font-style: italic; font-size: .9em; padding: 16px; }}

/* ── Export ── */
.export-bar {{ display: flex; gap: 10px; margin-bottom: 16px; }}

@media (max-width: 800px) {{
  .layout {{ flex-direction: column; }}
  .sidebar {{ width: 100%; border-right: none; border-bottom: 1px solid #dde3ea; }}
}}
</style>
</head>
<body>

<div class="header">
  <h1>Estudo Caso-Controle — IVSaúde UBS</h1>
  <p>Pareamento aleatório de unidades por vulnerabilidade, população e IVS · Porto Alegre · Pelotas · Betim</p>
</div>

<div class="layout">

<!-- ── Sidebar ── -->
<div class="sidebar">
  <h2>Definição de grupos</h2>

  <div class="control-group">
    <label>Limiar IVS — <strong>Caso</strong> (vulnerabilidade ≥)</label>
    <input type="range" id="limiarCaso" min="0.30" max="0.90" step="0.01" value="0.55"
           oninput="document.getElementById('vCaso').textContent=parseFloat(this.value).toFixed(2)">
    <div class="range-val" id="vCaso">0.55</div>
  </div>

  <div class="control-group">
    <label>Limiar IVS — <strong>Controle</strong> (vulnerabilidade ≤)</label>
    <input type="range" id="limiarControle" min="0.10" max="0.70" step="0.01" value="0.40"
           oninput="document.getElementById('vControle').textContent=parseFloat(this.value).toFixed(2)">
    <div class="range-val" id="vControle">0.40</div>
  </div>

  <div class="control-group">
    <label>Razão caso:controle</label>
    <select id="ratio">
      <option value="1">1:1</option>
      <option value="2" selected>1:2</option>
      <option value="3">1:3</option>
      <option value="4">1:4</option>
    </select>
  </div>

  <div class="control-group">
    <label>Tolerância populacional (±%)</label>
    <input type="range" id="tolPop" min="5" max="100" step="5" value="50"
           oninput="document.getElementById('vTol').textContent=this.value+'%'">
    <div class="range-val" id="vTol">50%</div>
  </div>

  <div class="control-group">
    <label>Peso do IVS no pareamento</label>
    <input type="range" id="pesoIVS" min="0" max="100" step="10" value="50"
           oninput="document.getElementById('vPesoIVS').textContent=this.value+'%'">
    <div class="range-val" id="vPesoIVS">50%</div>
    <div style="display:flex;justify-content:space-between;font-size:.7em;color:#aaa;margin-top:2px">
      <span>só população</span><span>só IVS</span>
    </div>
  </div>

  <div class="control-group">
    <label>Semente aleatória</label>
    <input type="number" id="seed" value="42" min="1" max="9999">
  </div>

  <button class="btn btn-primary" onclick="run()">▶ Gerar pareamento</button>
  <button class="btn btn-secondary" onclick="randomSeed()">⟳ Nova semente aleatória</button>

  <h2>Legenda</h2>
  <div class="legend-box">
    <div class="legend-item"><div class="legend-dot" style="background:#e74c3c"></div> Caso (grupo de referência)</div>
    <div class="legend-item"><div class="legend-dot" style="background:#2980b9"></div> Controle (pareado por IVS+Pop semelhantes)</div>
    <div class="legend-item"><div class="legend-dot" style="background:#2980b9"></div> Porto Alegre</div>
    <div class="legend-item"><div class="legend-dot" style="background:#8e44ad"></div> Pelotas</div>
    <div class="legend-item"><div class="legend-dot" style="background:#16a085"></div> Betim</div>
  </div>

  <h2>Metodologia</h2>
  <p style="font-size:.78em;color:#555;line-height:1.5">
    UBS são classificadas como <em>caso</em> ou <em>controle</em> pelo IVS calculado
    (Censo IBGE 2022 · INEP · OSM). Pareamento 1:N dentro da mesma cidade,
    por score ponderado de proximidade populacional e proximidade de IVS
    (com tolerância populacional configurável e fallback quando necessário).
    Seleção aleatória com semente reproduzível.
  </p>
</div>

<!-- ── Main ── -->
<div class="main">
  <div id="statsRow" class="stats-row"></div>
  <div class="export-bar">
    <button class="btn btn-secondary" style="width:auto;padding:7px 16px"
            onclick="exportCSV()">⬇ Exportar CSV</button>
  </div>
  <div class="section-title">Pares caso-controle por cidade</div>
  <div id="tablesArea"></div>
  <div id="unpairedArea"></div>
</div>

</div><!-- layout -->

<script>
const UBS_DATA = {data_json};

// ── utilidades ──────────────────────────────────────────────────────────────
function seededRng(seed) {{
  let s = seed;
  return () => {{
    s = (s * 1664525 + 1013904223) & 0xffffffff;
    return (s >>> 0) / 0xffffffff;
  }};
}}

function shuffle(arr, rng) {{
  for (let i = arr.length - 1; i > 0; i--) {{
    const j = Math.floor(rng() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }}
  return arr;
}}

function ivsColor(v) {{
  const stops = [[0,'#27ae60'],[0.33,'#f1c40f'],[0.66,'#e67e22'],[1,'#c0392b']];
  for (let i = 0; i < stops.length - 1; i++) {{
    const [v0, c0] = stops[i], [v1, c1] = stops[i+1];
    if (v <= v1) {{
      const t = (v - v0) / (v1 - v0);
      const hex = (c) => parseInt(c.slice(1,3),16)*256*256 + parseInt(c.slice(3,5),16)*256 + parseInt(c.slice(5,7),16);
      const lerp = (a,b,t) => Math.round(a + t*(b-a));
      const n0 = hex(c0), n1 = hex(c1);
      const r = lerp(n0>>16, n1>>16, t);
      const g = lerp((n0>>8)&0xff, (n1>>8)&0xff, t);
      const b = lerp(n0&0xff, n1&0xff, t);
      return `rgb(${{r}},${{g}},${{b}})`;
    }}
  }}
  return '#c0392b';
}}

function ivsBar(v) {{
  const c = ivsColor(v);
  const pct = Math.round(v * 100);
  return `<div class="ivs-bar">
    <div class="ivs-bg"><div class="ivs-fill" style="width:${{pct}}%;background:${{c}}"></div></div>
    <span class="ivs-val" style="color:${{c}}">${{v.toFixed(3)}}</span>
  </div>`;
}}

function dimMini(u) {{
  return ['D1','D2','D3','D4','D5'].map(d => {{
    const v = u[d];
    if (v === null || v === undefined) return '';
    const c = ivsColor(v);
    return `<span class="dim-pill" style="border-left:3px solid ${{c}}">${{d}} ${{v.toFixed(2)}}</span>`;
  }}).join('');
}}

function fmtPop(p) {{
  return p >= 1000 ? (p/1000).toFixed(1)+'k' : String(p);
}}

// ── Matching ────────────────────────────────────────────────────────────────
function match(cidades, casos, controles, ratio, tolPop, pesoIVS, rng) {{
  const result = {{}};  // cidade -> {{pairs: [], unpairedCasos: [], unpairedControles: []}}
  const wIVS = Math.max(0, Math.min(1, pesoIVS));
  const wPop = 1 - wIVS;

  const popDiffRel = (a, b) => {{
    if (!a.pop || !b.pop) return 1;
    return Math.abs(a.pop - b.pop) / a.pop;
  }};

  const matchScore = (caso, ctrl) => {{
    const dPop = popDiffRel(caso, ctrl);
    const dIvs = Math.abs((caso.ivs ?? 0) - (ctrl.ivs ?? 0));
    return (wPop * dPop) + (wIVS * dIvs);
  }};

  for (const cidade of cidades) {{
    const casosC = shuffle(casos.filter(u => u.cidade === cidade), rng);
    const controlesC = controles.filter(u => u.cidade === cidade);
    const used = new Set();
    const pairs = [];
    const unpairedCasos = [];

    for (const caso of casosC) {{
      // Candidatos: controles não usados dentro da tolerância populacional
      let candidatos = controlesC.filter(c => {{
        if (used.has(c.id)) return false;
        if (c.id === caso.id) return false;
        if (caso.pop === 0 || c.pop === 0) return true;
        const diff = Math.abs(c.pop - caso.pop) / caso.pop;
        return diff <= tolPop / 100;
      }});

      // Ordenar por score ponderado (população + IVS)
      candidatos.sort((a, b) =>
        matchScore(caso, a) - matchScore(caso, b)
      );

      // Adicionar aleatoriedade entre os melhores candidatos (top 2× ratio)
      const pool = candidatos.slice(0, ratio * 2);
      shuffle(pool, rng);
      const escolhidos = pool
        .sort((a, b) => matchScore(caso, a) - matchScore(caso, b))
        .slice(0, ratio);

      if (escolhidos.length === 0) {{
        // Sem candidato na tolerância populacional: buscar no conjunto global.
        const fallback = controlesC
          .filter(c => !used.has(c.id) && c.id !== caso.id)
          .sort((a, b) => matchScore(caso, a) - matchScore(caso, b))
          .slice(0, ratio);
        if (fallback.length === 0) {{
          unpairedCasos.push(caso);
          continue;
        }}
        fallback.forEach(c => used.add(c.id));
        pairs.push({{ caso, controles: fallback, fallback: true }});
      }} else {{
        escolhidos.forEach(c => used.add(c.id));
        pairs.push({{ caso, controles: escolhidos, fallback: false }});
      }}
    }}

    const unpairedControles = controlesC.filter(c => !used.has(c.id));
    result[cidade] = {{ pairs, unpairedCasos, unpairedControles }};
  }}
  return result;
}}

// ── Render ──────────────────────────────────────────────────────────────────
let lastResult = null;

function run() {{
  const limCaso     = parseFloat(document.getElementById('limiarCaso').value);
  const limControle = parseFloat(document.getElementById('limiarControle').value);
  const ratio       = parseInt(document.getElementById('ratio').value);
  const tolPop      = parseFloat(document.getElementById('tolPop').value);
  const pesoIVS     = parseFloat(document.getElementById('pesoIVS').value) / 100;
  const seed        = parseInt(document.getElementById('seed').value) || 42;
  const rng         = seededRng(seed);

  const casos     = UBS_DATA.filter(u => u.ivs >= limCaso);
  const controles = UBS_DATA.filter(u => u.ivs <= limControle);
  const cidades   = [...new Set(UBS_DATA.map(u => u.cidade))];

  const resultado = match(cidades, casos, controles, ratio, tolPop, pesoIVS, rng);
  lastResult = {{ resultado, limCaso, limControle, ratio, tolPop, pesoIVS, seed }};

  // Stats globais
  let totalPares = 0, totalCasos = 0, totalCont = 0, totalNPar = 0;
  for (const c of cidades) {{
    const r = resultado[c];
    totalPares += r.pairs.length;
    totalCasos += r.pairs.length + r.unpairedCasos.length;
    totalCont  += r.pairs.reduce((s,p) => s + p.controles.length, 0);
    totalNPar  += r.unpairedCasos.length;
  }}

  document.getElementById('statsRow').innerHTML = `
    <div class="stat-card">
      <div class="label">Casos elegíveis</div>
      <div class="value" style="color:#e74c3c">${{casos.length}}</div>
      <div class="sub">IVS ≥ ${{limCaso.toFixed(2)}}</div>
    </div>
    <div class="stat-card">
      <div class="label">Controles elegíveis</div>
      <div class="value" style="color:#2980b9">${{controles.length}}</div>
      <div class="sub">IVS ≤ ${{limControle.toFixed(2)}}</div>
    </div>
    <div class="stat-card">
      <div class="label">Pares formados</div>
      <div class="value" style="color:#27ae60">${{totalPares}}</div>
      <div class="sub">Razão 1:${{ratio}} · IVS ${{Math.round(pesoIVS*100)}}% · semente ${{seed}}</div>
    </div>
    <div class="stat-card">
      <div class="label">Controles alocados</div>
      <div class="value" style="color:#27ae60">${{totalCont}}</div>
      <div class="sub">${{totalNPar}} casos sem par</div>
    </div>`;

  // Tabelas por cidade
  let html = '';
  for (const cidade of cidades) {{
    const r = resultado[cidade];
    const corCidade = UBS_DATA.find(u => u.cidade === cidade)?.cor || '#555';
    const nCasos = r.pairs.length + r.unpairedCasos.length;
    const nCont  = r.pairs.reduce((s,p) => s + p.controles.length, 0);
    const nPar   = r.pairs.length;

    html += `
    <div class="section-title" style="border-color:${{corCidade}}">
      <span class="badge-cidade badge" style="background:${{corCidade}}">${{cidade}}</span>
      &nbsp; ${{nPar}} pares · ${{nCasos}} casos · ${{nCont}} controles alocados
    </div>
    <div class="table-wrap" style="margin-bottom:20px">
      <table>
        <thead>
          <tr>
            <th>Grupo</th>
            <th>Par #</th>
            <th>UBS</th>
            <th>IVS</th>
            <th>Dimensões</th>
            <th>Pop. estimada</th>
          </tr>
        </thead>
        <tbody>`;

    r.pairs.forEach((p, i) => {{
      const nota = p.fallback ? ' <span title="Fora da tolerância populacional" style="color:#f39c12">⚠</span>' : '';
      html += renderRow(p.caso, 'caso', i+1);
      p.controles.forEach(ctrl => {{
        html += renderRow(ctrl, 'controle', i+1, nota);
      }});
      if (i < r.pairs.length - 1) html += `<tr><td class="pair-sep" colspan="6"></td></tr>`;
    }});

    html += '</tbody></table></div>';

    if (r.unpairedCasos.length > 0) {{
      html += `<p style="font-size:.8em;color:#e74c3c;margin:-12px 0 16px;padding-left:4px">
        ⚠ ${{r.unpairedCasos.length}} caso(s) sem controle disponível em ${{cidade}}:
        ${{r.unpairedCasos.map(u=>'<strong>'+u.nome+'</strong>').join(', ')}}
      </p>`;
    }}
  }}

  document.getElementById('tablesArea').innerHTML = html || '<p class="empty-msg">Nenhum par formado com os critérios atuais.</p>';

  // Controles não usados
  let unHtml = '<div class="section-title">Controles não alocados</div><div class="unpaired-list">';
  let anyUn = false;
  for (const cidade of cidades) {{
    const corCidade = UBS_DATA.find(u => u.cidade === cidade)?.cor || '#555';
    resultado[cidade].unpairedControles.forEach(u => {{
      anyUn = true;
      unHtml += `<div class="unpaired-chip">
        <span class="badge-cidade badge" style="background:${{corCidade}};font-size:.68em">${{cidade}}</span>
        ${{u.nome}} <span style="color:#7f8c8d">(IVS ${{u.ivs.toFixed(3)}})</span>
      </div>`;
    }});
  }}
  unHtml += '</div>';
  document.getElementById('unpairedArea').innerHTML = anyUn ? unHtml : '';
}}

function renderRow(u, grupo, par, nota='') {{
  const badge = grupo === 'caso'
    ? '<span class="badge badge-caso">Caso</span>'
    : '<span class="badge badge-controle">Controle</span>';
  return `<tr>
    <td>${{badge}}</td>
    <td style="color:#7f8c8d;font-size:.8em">Par ${{par}}${{nota}}</td>
    <td style="font-weight:500">${{u.nome}}</td>
    <td>${{ivsBar(u.ivs)}}</td>
    <td><div class="dim-mini">${{dimMini(u)}}</div></td>
    <td class="pop-val">~${{fmtPop(u.pop)}} hab</td>
  </tr>`;
}}

function randomSeed() {{
  document.getElementById('seed').value = Math.floor(Math.random() * 9000) + 1;
  run();
}}

function exportCSV() {{
  if (!lastResult) return;
  const rows = [['Cidade','Par','Grupo','ID_UBS','Nome_UBS','IVS','D1','D2','D3','D4','D5','Pop_estimada']];
  const cidades = [...new Set(UBS_DATA.map(u => u.cidade))];
  for (const cidade of cidades) {{
    const r = lastResult.resultado[cidade];
    r.pairs.forEach((p, i) => {{
      const addRow = (u, grupo) => rows.push([
        cidade, i+1, grupo, u.id, u.nome,
        u.ivs, u.D1??'', u.D2??'', u.D3??'', u.D4??'', u.D5??'', u.pop
      ]);
      addRow(p.caso, 'caso');
      p.controles.forEach(c => addRow(c, 'controle'));
    }});
    r.unpairedCasos.forEach(u => rows.push([
      cidade, 'sem_par', 'caso', u.id, u.nome,
      u.ivs, u.D1??'', u.D2??'', u.D3??'', u.D4??'', u.D5??'', u.pop
    ]));
  }}
  const csv = rows.map(r => r.map(v => `"${{String(v).replace(/"/g,'""')}}"`).join(',')).join('\\n');
  const blob = new Blob([csv], {{type:'text/csv;charset=utf-8;'}});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `caso_controle_ivs_semente${{lastResult.seed}}.csv`;
  a.click();
}}

// Executar ao carregar
run();
</script>
</body>
</html>"""

    OUT_HTML.write_text(html, encoding="utf-8")
    size = OUT_HTML.stat().st_size / 1024
    print(f"HTML gerado: {OUT_HTML}  ({size:.1f}KB)")


if __name__ == "__main__":
    main()
