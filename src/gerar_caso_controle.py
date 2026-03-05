"""
Gera pagina HTML interativa para design de estudo cluster randomizado estratificado.

Unidades (UBS) sao estratificadas por faixas de IVS e faixas de populacao adscrita.
Dentro de cada estrato, as UBS sao randomizadas para intervencao/controle em proporcao
aproximada de 1:1 (exata quando estrato tem tamanho par).

Uso:
    python src/gerar_caso_controle.py
"""

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT_HTML = ROOT / "caso-controle.html"

MUNICIPIOS = [
    {"slug": "poa", "cidade": "Porto Alegre", "cor": "#d4ac0d"},
    {"slug": "pelotas", "cidade": "Pelotas", "cor": "#8e44ad"},
    {"slug": "betim", "cidade": "Betim", "cor": "#16a085"},
]

INDICADORES = {
    "D1_analf": "D1",
    "D1_negros": "D1",
    "D2_sem_saneam": "D2",
    "D2_sem_lixo": "D2",
    "D2_fora_creche": "D2",
    "D2_fora_fund": "D2",
    "D3_osc_per1k": "D3",
    "D4_adol_fem": "D4",
    "D5_menor1": "D5",
    "D5_adol": "D5",
    "D5_mif": "D5",
    "D5_idosos": "D5",
}
INDICADORES_INVERSOS = {"D3_osc_per1k"}
DIM_PESOS = {"D1": 0.50, "D2": 0.30, "D3": 0.10, "D4": 0.08, "D5": 0.02}


def normalizar_mm(serie: pd.Series) -> pd.Series:
    mn, mx = serie.min(), serie.max()
    if mx == mn:
        return pd.Series(0.5, index=serie.index)
    return (serie - mn) / (mx - mn)


def calcular_ivs(df: pd.DataFrame) -> pd.DataFrame:
    dim_scores: dict[str, list[pd.Series]] = {}
    for col, dim in INDICADORES.items():
        if col not in df.columns:
            continue
        series = df[col]
        series_valid = series.dropna()
        if series_valid.empty:
            continue
        med = series_valid.median()
        norm = normalizar_mm(series.fillna(med))
        if col in INDICADORES_INVERSOS:
            norm = 1 - norm
        dim_scores.setdefault(dim, []).append(norm)

    result = df[["id_ubs", "no_ubs", "pop_total"]].copy()
    for d in ("D1", "D2", "D3", "D4", "D5"):
        result[d] = sum(dim_scores[d]) / len(dim_scores[d]) if d in dim_scores else float("nan")

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
            print(f"[AVISO] Nao encontrado: {csv_path}")
            continue

        df = pd.read_csv(csv_path)
        scored = calcular_ivs(df)
        for _, r in scored.iterrows():
            if pd.isna(r["ivs"]) or pd.isna(r["pop_total"]):
                continue
            records.append(
                {
                    "id": str(r["id_ubs"]),
                    "nome": r["no_ubs"].title().replace("Ubs ", "UBS ").replace("Csf ", "CSF "),
                    "cidade": m["cidade"],
                    "cor": m["cor"],
                    "pop": round(float(r["pop_total"])),
                    "ivs": round(float(r["ivs"]), 4),
                    "D1": round(float(r["D1"]), 4) if pd.notna(r["D1"]) else None,
                    "D2": round(float(r["D2"]), 4) if pd.notna(r["D2"]) else None,
                    "D3": round(float(r["D3"]), 4) if pd.notna(r["D3"]) else None,
                    "D4": round(float(r["D4"]), 4) if pd.notna(r["D4"]) else None,
                    "D5": round(float(r["D5"]), 4) if pd.notna(r["D5"]) else None,
                }
            )
    return records


def main() -> None:
    records = load_all()
    data_json = json.dumps(records, ensure_ascii=False)

    cidades = list(dict.fromkeys(r["cidade"] for r in records))
    totais = {c: sum(1 for r in records if r["cidade"] == c) for c in cidades}
    if cidades:
        print(f"UBS carregadas: {', '.join(f'{c}={totais[c]}' for c in cidades)}")
    else:
        print("[AVISO] Nenhuma UBS carregada.")

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cluster Randomized Trial Estratificado — UBS</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: #f0f2f5;
  color: #2c3e50;
  font-size: 14px;
}}

.header {{
  background: linear-gradient(135deg, #1a5276 0%, #2980b9 100%);
  color: #fff;
  padding: 28px 40px;
}}
.header h1 {{ font-size: 1.55em; margin-bottom: 6px; }}
.header p {{ opacity: .85; font-size: .9em; }}

.layout {{ display: flex; min-height: calc(100vh - 120px); }}

.sidebar {{
  width: 320px;
  flex-shrink: 0;
  background: #fff;
  border-right: 1px solid #dde3ea;
  padding: 20px 18px;
  overflow-y: auto;
}}

.sidebar h2 {{
  font-size: .9em;
  font-weight: 700;
  color: #1a5276;
  text-transform: uppercase;
  letter-spacing: .05em;
  border-left: 3px solid #2980b9;
  padding-left: 8px;
  margin: 20px 0 12px;
}}
.sidebar h2:first-child {{ margin-top: 0; }}

.control-group {{ margin-bottom: 14px; }}
.control-group label {{
  display: block;
  font-size: .8em;
  color: #555;
  margin-bottom: 4px;
  font-weight: 600;
}}
.control-group select,
.control-group input[type=number] {{
  width: 100%;
  padding: 6px 8px;
  border: 1px solid #d0d7de;
  border-radius: 5px;
  font-size: .85em;
  background: #f8f9fa;
}}
.control-group input[type=checkbox] {{ transform: scale(1.05); margin-right: 6px; }}

.btn {{
  width: 100%;
  padding: 10px;
  border: none;
  border-radius: 6px;
  font-size: .9em;
  font-weight: 700;
  cursor: pointer;
  margin-top: 6px;
  transition: opacity .15s;
}}
.btn:hover {{ opacity: .85; }}
.btn-primary {{ background: #2980b9; color: #fff; }}
.btn-secondary {{ background: #ecf0f1; color: #2c3e50; }}

.legend-box {{ display: flex; gap: 8px; flex-direction: column; font-size: .8em; margin-top: 10px; }}
.legend-item {{ display: flex; align-items: center; gap: 6px; }}
.legend-dot {{ width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0; }}

.main {{ flex: 1; padding: 20px; overflow-y: auto; }}

.stats-row {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }}
.stat-card {{
  background: #fff;
  border-radius: 8px;
  padding: 14px 18px;
  box-shadow: 0 1px 3px rgba(0,0,0,.08);
  flex: 1;
  min-width: 150px;
}}
.stat-card .label {{
  font-size: .72em;
  color: #7f8c8d;
  text-transform: uppercase;
  letter-spacing: .05em;
  margin-bottom: 4px;
}}
.stat-card .value {{ font-size: 1.55em; font-weight: 700; }}
.stat-card .sub {{ font-size: .75em; color: #95a5a6; margin-top: 2px; }}

.section-title {{
  font-size: .95em;
  font-weight: 700;
  color: #1a5276;
  border-left: 4px solid #2980b9;
  padding-left: 10px;
  margin: 24px 0 12px;
  text-transform: uppercase;
  letter-spacing: .04em;
}}

.table-wrap {{
  overflow-x: auto;
  background: #fff;
  border-radius: 8px;
  box-shadow: 0 1px 3px rgba(0,0,0,.08);
  margin-bottom: 18px;
}}

table {{ width: 100%; border-collapse: collapse; font-size: .82em; }}
thead th {{
  background: #1a5276;
  color: #fff;
  padding: 9px 10px;
  text-align: left;
  font-size: .78em;
  font-weight: 600;
  white-space: nowrap;
  position: sticky;
  top: 0;
  z-index: 1;
}}
tbody tr:nth-child(even) {{ background: #f8f9fa; }}
tbody tr:hover {{ background: #eaf3fb; }}
td {{ padding: 7px 10px; border-bottom: 1px solid #ecf0f1; vertical-align: middle; }}

.badge {{
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: .75em;
  font-weight: 700;
  color: #fff;
  white-space: nowrap;
}}
.badge-int {{ background: #e74c3c; }}
.badge-ctrl {{ background: #2980b9; }}
.badge-cidade {{
  padding: 2px 6px;
  border-radius: 3px;
  font-size: .72em;
  font-weight: 600;
  color: #fff;
}}

.ivs-bar {{ display: flex; align-items: center; gap: 6px; }}
.ivs-bg {{ flex: 1; height: 5px; background: #ecf0f1; border-radius: 3px; min-width: 50px; }}
.ivs-fill {{ height: 100%; border-radius: 3px; }}
.ivs-val {{ font-size: .8em; font-weight: 600; min-width: 32px; text-align: right; }}

.dim-mini {{ display: flex; gap: 3px; flex-wrap: wrap; }}
.dim-pill {{
  font-size: .68em;
  padding: 1px 5px;
  border-radius: 3px;
  background: #ecf0f1;
  color: #555;
  white-space: nowrap;
}}

.pop-val {{ font-size: .82em; color: #555; }}
.estrato-cell {{ font-size: .8em; color: #34495e; font-weight: 600; }}

.method-note {{ font-size: .78em; color: #555; line-height: 1.5; }}

@media (max-width: 900px) {{
  .layout {{ flex-direction: column; }}
  .sidebar {{ width: 100%; border-right: none; border-bottom: 1px solid #dde3ea; }}
}}
</style>
</head>
<body>

<div class="header">
  <h1>Cluster Randomized Trial Estratificado — UBS</h1>
  <p>Randomizacao estratificada por IVS e populacao adscrita para alocacao em intervencao e controle</p>
</div>

<div class="layout">

<div class="sidebar">
  <h2>Parametros</h2>

  <div class="control-group">
    <label>Estratos de IVS</label>
    <select id="binsIVS">
      <option value="2">2 faixas</option>
      <option value="3">3 faixas</option>
      <option value="4" selected>4 faixas</option>
      <option value="5">5 faixas</option>
      <option value="6">6 faixas</option>
    </select>
  </div>

  <div class="control-group">
    <label>Estratos de populacao</label>
    <select id="binsPop">
      <option value="2">2 faixas</option>
      <option value="3">3 faixas</option>
      <option value="4" selected>4 faixas</option>
      <option value="5">5 faixas</option>
      <option value="6">6 faixas</option>
    </select>
  </div>

  <div class="control-group">
    <label style="display:flex;align-items:center">
      <input type="checkbox" id="porCidade" checked>
      Estratificar separadamente por cidade
    </label>
  </div>

  <div class="control-group">
    <label>Semente aleatoria</label>
    <input type="number" id="seed" value="42" min="1" max="999999">
  </div>

  <button class="btn btn-primary" onclick="run()">Gerar randomizacao estratificada</button>
  <button class="btn btn-secondary" onclick="randomSeed()">Nova semente</button>

  <h2>Legenda</h2>
  <div class="legend-box">
    <div class="legend-item"><div class="legend-dot" style="background:#e74c3c"></div> Grupo intervencao</div>
    <div class="legend-item"><div class="legend-dot" style="background:#2980b9"></div> Grupo controle</div>
    <div class="legend-item"><div class="legend-dot" style="background:#d4ac0d"></div> Porto Alegre</div>
    <div class="legend-item"><div class="legend-dot" style="background:#8e44ad"></div> Pelotas</div>
    <div class="legend-item"><div class="legend-dot" style="background:#16a085"></div> Betim</div>
  </div>

  <h2>Metodo</h2>
  <p class="method-note">
    1) As UBS sao estratificadas por faixas de IVS e faixas de populacao adscrita.<br>
    2) Dentro de cada estrato, a alocacao e aleatoria com proporcao 1:1 entre intervencao e controle.<br>
    3) Em estratos com numero impar de UBS, a unidade excedente e sorteada entre os grupos.<br>
    4) A semente garante reprodutibilidade da randomizacao.
  </p>
</div>

<div class="main">
  <div id="statsRow" class="stats-row"></div>

  <div class="section-title">Resumo por estrato</div>
  <div id="strataArea"></div>

  <div class="section-title">Alocacao por cidade</div>
  <div class="table-wrap" style="margin-bottom:16px;padding:10px 12px">
    <button class="btn btn-secondary" style="width:auto;padding:7px 16px" onclick="exportCSV()">Exportar CSV</button>
  </div>
  <div id="tablesArea"></div>
</div>

</div>

<script>
const UBS_DATA = {data_json};

function seededRng(seed) {{
  let s = seed >>> 0;
  return () => {{
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 4294967295;
  }};
}}

function shuffle(arr, rng) {{
  for (let i = arr.length - 1; i > 0; i--) {{
    const j = Math.floor(rng() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }}
  return arr;
}}

function groupBy(arr, keyFn) {{
  const out = {{}};
  for (const item of arr) {{
    const k = keyFn(item);
    if (!out[k]) out[k] = [];
    out[k].push(item);
  }}
  return out;
}}

function mean(arr, field) {{
  if (!arr.length) return NaN;
  const vals = arr.map(x => x[field]).filter(v => Number.isFinite(v));
  if (!vals.length) return NaN;
  return vals.reduce((s, v) => s + v, 0) / vals.length;
}}

function ivsColor(v) {{
  const stops = [[0, '#27ae60'], [0.33, '#f1c40f'], [0.66, '#e67e22'], [1, '#c0392b']];
  for (let i = 0; i < stops.length - 1; i++) {{
    const [v0, c0] = stops[i], [v1, c1] = stops[i + 1];
    if (v <= v1) {{
      const t = (v - v0) / (v1 - v0);
      const hex = c => parseInt(c.slice(1, 3), 16) * 65536 + parseInt(c.slice(3, 5), 16) * 256 + parseInt(c.slice(5, 7), 16);
      const lerp = (a, b, x) => Math.round(a + x * (b - a));
      const n0 = hex(c0), n1 = hex(c1);
      const r = lerp(n0 >> 16, n1 >> 16, t);
      const g = lerp((n0 >> 8) & 255, (n1 >> 8) & 255, t);
      const b = lerp(n0 & 255, n1 & 255, t);
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
  return ['D1', 'D2', 'D3', 'D4', 'D5'].map(d => {{
    const v = u[d];
    if (v === null || v === undefined || Number.isNaN(v)) return '';
    const c = ivsColor(v);
    return `<span class="dim-pill" style="border-left:3px solid ${{c}}">${{d}} ${{v.toFixed(2)}}</span>`;
  }}).join('');
}}

function fmtPop(p) {{
  if (!Number.isFinite(p)) return '-';
  return p >= 1000 ? (p / 1000).toFixed(1) + 'k' : String(Math.round(p));
}}

function quantileBreaks(values, bins) {{
  const vals = values.filter(v => Number.isFinite(v)).sort((a, b) => a - b);
  if (!vals.length || bins <= 1) return [];

  const cuts = [];
  for (let i = 1; i < bins; i++) {{
    const pos = ((vals.length - 1) * i) / bins;
    const lo = Math.floor(pos);
    const hi = Math.ceil(pos);
    const q = vals[lo] + (vals[hi] - vals[lo]) * (pos - lo);
    cuts.push(q);
  }}

  // Remover cortes duplicados (quando distribuicao tem muitos empates).
  return cuts.filter((v, i, arr) => i === 0 || Math.abs(v - arr[i - 1]) > 1e-9);
}}

function binIndex(value, cuts) {{
  if (!Number.isFinite(value)) return 1;
  let idx = 0;
  while (idx < cuts.length && value > cuts[idx]) idx++;
  return idx + 1;
}}

function estratificar(units, binsIVS, binsPop, porCidade) {{
  const saida = units.map(u => ({{ ...u }}));
  const escopos = porCidade ? groupBy(saida, u => u.cidade) : {{ TODAS: saida }};

  for (const lista of Object.values(escopos)) {{
    const cutsIVS = quantileBreaks(lista.map(u => u.ivs), binsIVS);
    const cutsPop = quantileBreaks(lista.map(u => u.pop), binsPop);

    for (const u of lista) {{
      const bIvs = binIndex(u.ivs, cutsIVS);
      const bPop = binIndex(u.pop, cutsPop);
      u.estrato = `IVS${{bIvs}}-POP${{bPop}}`;
      u.estrato_key = porCidade ? `${{u.cidade}} · ${{u.estrato}}` : u.estrato;
    }}
  }}
  return saida;
}}

function randomizacaoEstratificada(units, binsIVS, binsPop, porCidade, seed) {{
  const rng = seededRng(seed);
  const estratificados = estratificar(units, binsIVS, binsPop, porCidade);
  const porEstrato = groupBy(estratificados, u => u.estrato_key);

  const alocados = [];
  const strataSummary = [];

  for (const [estratoKey, listaOrig] of Object.entries(porEstrato)) {{
    const lista = shuffle([...listaOrig], rng);
    const n = lista.length;

    let nInterv = Math.floor(n / 2);
    let nCtrl = Math.floor(n / 2);

    if (n % 2 === 1) {{
      if (rng() < 0.5) nInterv += 1;
      else nCtrl += 1;
    }}

    lista.forEach((u, idx) => {{
      u.grupo = idx < nInterv ? 'intervencao' : 'controle';
      alocados.push(u);
    }});

    strataSummary.push({{
      estrato: estratoKey,
      total: n,
      intervencao: nInterv,
      controle: nCtrl,
      diff: Math.abs(nInterv - nCtrl),
    }});
  }}

  const interv = alocados.filter(u => u.grupo === 'intervencao');
  const ctrl = alocados.filter(u => u.grupo === 'controle');

  const meanIvsInt = mean(interv, 'ivs');
  const meanIvsCtrl = mean(ctrl, 'ivs');
  const meanPopInt = mean(interv, 'pop');
  const meanPopCtrl = mean(ctrl, 'pop');

  const diffIvs = Math.abs(meanIvsInt - meanIvsCtrl);
  const diffPopAbs = Math.abs(meanPopInt - meanPopCtrl);
  const popRef = (meanPopInt + meanPopCtrl) / 2;
  const diffPopPct = popRef > 0 ? (diffPopAbs / popRef) * 100 : NaN;

  return {{
    rows: alocados,
    strataSummary: strataSummary.sort((a, b) => a.estrato.localeCompare(b.estrato)),
    stats: {{
      total: alocados.length,
      intervencao: interv.length,
      controle: ctrl.length,
      meanIvsInt,
      meanIvsCtrl,
      meanPopInt,
      meanPopCtrl,
      diffIvs,
      diffPopAbs,
      diffPopPct,
      estratos: strataSummary.length,
      estratosBalanceados: strataSummary.filter(s => s.diff <= 1).length,
    }}
  }};
}}

let lastResult = null;

function renderStrataTable(strataSummary) {{
  if (!strataSummary.length) return '<p style="color:#999">Nenhum estrato formado.</p>';

  let html = '<div class="table-wrap"><table><thead><tr>' +
    '<th>Estrato</th><th>Total UBS</th><th>Intervencao</th><th>Controle</th><th>Diferenca</th>' +
    '</tr></thead><tbody>';

  for (const s of strataSummary) {{
    const okColor = s.diff <= 1 ? '#27ae60' : '#e67e22';
    html += `<tr>
      <td class="estrato-cell">${{s.estrato}}</td>
      <td>${{s.total}}</td>
      <td><span class="badge badge-int">${{s.intervencao}}</span></td>
      <td><span class="badge badge-ctrl">${{s.controle}}</span></td>
      <td style="font-weight:700;color:${{okColor}}">${{s.diff}}</td>
    </tr>`;
  }}

  html += '</tbody></table></div>';
  return html;
}}

function renderRowsByCity(rows) {{
  const cidades = [...new Set(rows.map(r => r.cidade))];
  let html = '';

  for (const cidade of cidades) {{
    const rowsCidade = rows
      .filter(r => r.cidade === cidade)
      .sort((a, b) => a.estrato_key.localeCompare(b.estrato_key) || a.grupo.localeCompare(b.grupo) || a.ivs - b.ivs);

    const corCidade = rowsCidade[0]?.cor || '#555';
    const nInt = rowsCidade.filter(r => r.grupo === 'intervencao').length;
    const nCtrl = rowsCidade.filter(r => r.grupo === 'controle').length;

    html += `
      <div class="section-title" style="border-color:${{corCidade}}">
        <span class="badge-cidade badge" style="background:${{corCidade}}">${{cidade}}</span>
        &nbsp; ${{rowsCidade.length}} UBS · Intervencao: ${{nInt}} · Controle: ${{nCtrl}}
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Grupo</th>
              <th>Estrato</th>
              <th>UBS</th>
              <th>IVS</th>
              <th>Dimensoes</th>
              <th>Pop. estimada</th>
            </tr>
          </thead>
          <tbody>`;

    for (const u of rowsCidade) {{
      const badge = u.grupo === 'intervencao'
        ? '<span class="badge badge-int">Intervencao</span>'
        : '<span class="badge badge-ctrl">Controle</span>';
      html += `<tr>
        <td>${{badge}}</td>
        <td class="estrato-cell">${{u.estrato}}</td>
        <td style="font-weight:500">${{u.nome}}</td>
        <td>${{ivsBar(u.ivs)}}</td>
        <td><div class="dim-mini">${{dimMini(u)}}</div></td>
        <td class="pop-val">~${{fmtPop(u.pop)}} hab</td>
      </tr>`;
    }}

    html += '</tbody></table></div>';
  }}

  return html;
}}

function run() {{
  const binsIVS = parseInt(document.getElementById('binsIVS').value, 10);
  const binsPop = parseInt(document.getElementById('binsPop').value, 10);
  const porCidade = document.getElementById('porCidade').checked;
  const seed = parseInt(document.getElementById('seed').value, 10) || 42;

  const result = randomizacaoEstratificada(UBS_DATA, binsIVS, binsPop, porCidade, seed);
  lastResult = {{ ...result, params: {{ binsIVS, binsPop, porCidade, seed }} }};

  const s = result.stats;
  const diffIvsStr = Number.isFinite(s.diffIvs) ? s.diffIvs.toFixed(3) : '-';
  const diffPopStr = Number.isFinite(s.diffPopAbs) ? Math.round(s.diffPopAbs).toLocaleString('pt-BR') : '-';
  const diffPopPctStr = Number.isFinite(s.diffPopPct) ? s.diffPopPct.toFixed(1) + '%' : '-';

  document.getElementById('statsRow').innerHTML = `
    <div class="stat-card">
      <div class="label">Total de UBS</div>
      <div class="value" style="color:#34495e">${{s.total}}</div>
      <div class="sub">Estratos: ${{s.estratos}} (IVS=${{binsIVS}}, Pop=${{binsPop}})</div>
    </div>
    <div class="stat-card">
      <div class="label">Intervencao</div>
      <div class="value" style="color:#e74c3c">${{s.intervencao}}</div>
      <div class="sub">Media IVS: ${{Number.isFinite(s.meanIvsInt) ? s.meanIvsInt.toFixed(3) : '-'}} · Pop media: ${{Number.isFinite(s.meanPopInt) ? Math.round(s.meanPopInt).toLocaleString('pt-BR') : '-'}} </div>
    </div>
    <div class="stat-card">
      <div class="label">Controle</div>
      <div class="value" style="color:#2980b9">${{s.controle}}</div>
      <div class="sub">Media IVS: ${{Number.isFinite(s.meanIvsCtrl) ? s.meanIvsCtrl.toFixed(3) : '-'}} · Pop media: ${{Number.isFinite(s.meanPopCtrl) ? Math.round(s.meanPopCtrl).toLocaleString('pt-BR') : '-'}} </div>
    </div>
    <div class="stat-card">
      <div class="label">Equilibrio global</div>
      <div class="value" style="color:#27ae60">ΔIVS ${{diffIvsStr}}</div>
      <div class="sub">ΔPop ${{diffPopStr}} (${{diffPopPctStr}}) · Estratos balanceados: ${{s.estratosBalanceados}}/${{s.estratos}}</div>
    </div>`;

  document.getElementById('strataArea').innerHTML = renderStrataTable(result.strataSummary);
  document.getElementById('tablesArea').innerHTML = renderRowsByCity(result.rows);
}}

function randomSeed() {{
  document.getElementById('seed').value = Math.floor(Math.random() * 999999) + 1;
  run();
}}

function exportCSV() {{
  if (!lastResult) return;

  const rows = [[
    'Cidade', 'Estrato', 'Grupo', 'ID_UBS', 'Nome_UBS',
    'IVS', 'D1', 'D2', 'D3', 'D4', 'D5', 'Pop_estimada'
  ]];

  const ordered = [...lastResult.rows].sort((a, b) =>
    a.cidade.localeCompare(b.cidade) ||
    a.estrato_key.localeCompare(b.estrato_key) ||
    a.grupo.localeCompare(b.grupo)
  );

  for (const u of ordered) {{
    rows.push([
      u.cidade,
      u.estrato,
      u.grupo,
      u.id,
      u.nome,
      u.ivs,
      u.D1 ?? '',
      u.D2 ?? '',
      u.D3 ?? '',
      u.D4 ?? '',
      u.D5 ?? '',
      u.pop,
    ]);
  }}

  const csv = rows
    .map(r => r.map(v => `"${{String(v).replace(/"/g, '""')}}"`).join(','))
    .join('\\n');

  const blob = new Blob([csv], {{ type: 'text/csv;charset=utf-8;' }});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `cluster_randomizado_estratificado_seed${{lastResult.params.seed}}.csv`;
  a.click();
}}

run();
</script>
</body>
</html>"""

    OUT_HTML.write_text(html, encoding="utf-8")
    size_kb = OUT_HTML.stat().st_size / 1024
    print(f"HTML gerado: {OUT_HTML} ({size_kb:.1f}KB)")


if __name__ == "__main__":
    main()
