"""
gerar_pagina_municipio.py
=======================
Gera index.html com mapa Leaflet interativo e tabela de indicadores
para territórios de UBS do município configurado.

Também exporta data/{slug}.json para uso com mapa.html dinâmico.
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[1]
DEFAULT_SLUG = "municipio"
DEFAULT_CIDADE = "Municipio"
PROC = BASE / "ivs_municipio" / "data" / "processed"
OUT_HTML = BASE / "index.html"
IVS_FILE = PROC / "ivs_municipio.csv"
NOME_CIDADE = DEFAULT_CIDADE

# ---------------------------------------------------------------------------
# Indicadores disponíveis por dimensão
# D1 (20%): analfabetismo, raça — Censo IBGE 2022
# D2 (20%): saneamento, lixo — Censo IBGE 2022
# D3 (20%): entidades comunitárias OSM (INVERSO: mais = menos vulnerável)
# D4 (20%): adolescentes femininas 10-19 (proxy mães adolescentes, Censo IBGE 2022)
# D5 (20%): perfil demográfico — Censo IBGE 2022
# ---------------------------------------------------------------------------
INDICADORES = {
    "D1_analf":       ("D1", "Analfabetismo 15+",                  "%"),
    "D1_negros":      ("D1", "Pop. preta+parda",                   "%"),
    "D2_sem_saneam":  ("D2", "Sem saneamento adequado",             "%"),
    "D2_sem_lixo":    ("D2", "Sem coleta de lixo",                 "%"),
    "D2_fora_creche": ("D2", "Crianças 0-3 fora da creche",        "%"),
    "D2_fora_fund":   ("D2", "Crianças 5-14 fora do fundamental",  "%"),
    "D3_osc_per1k":   ("D3", "Entidades comunitárias / 1.000 hab", "#"),
    "D4_adol_fem":    ("D4", "Adolescentes femininas 10-19",       "%"),
    "D5_menor1":      ("D5", "Crianças <1 ano (proxy 0-4/5)",      "%"),
    "D5_adol":        ("D5", "Adolescentes 10-19 anos",            "%"),
    "D5_mif":         ("D5", "Mulheres 10-49 anos",                "%"),
    "D5_idosos":      ("D5", "Idosos 60+ anos",                    "%"),
}

# Indicadores onde valor MAIOR = MENOS vulnerável (inverter na normalização)
INDICADORES_INVERSOS = {"D3_osc_per1k"}

DIM_PESOS = {"D1": 0.50, "D2": 0.30, "D3": 0.10, "D4": 0.08, "D5": 0.02}

# Cores do gradiente de vulnerabilidade (verde → amarelo → laranja → vermelho)
CORES_IVS = [
    (0.00, "#27ae60"),
    (0.33, "#f1c40f"),
    (0.66, "#e67e22"),
    (1.00, "#c0392b"),
]


def _configure_runtime(base_dir: Path, slug: str, cidade: str, out_html: Path | None = None) -> None:
    global PROC, OUT_HTML, IVS_FILE, NOME_CIDADE
    PROC = base_dir.resolve() / "data" / "processed"
    IVS_FILE = PROC / f"ivs_{slug}.csv"
    OUT_HTML = out_html.resolve() if out_html else Path(__file__).resolve().parents[1] / "docs" / "index.html"
    NOME_CIDADE = cidade


def normalizar_mm(serie: pd.Series) -> pd.Series:
    """Normalização min-max para [0, 1]."""
    mn, mx = serie.min(), serie.max()
    if mx == mn:
        return pd.Series(0.5, index=serie.index)
    return (serie - mn) / (mx - mn)


def calcular_dimensoes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula scores por dimensão (D1-D5) e IVS.
    Cada score é a média normalizada [0,1] dos indicadores disponíveis na dimensão.
    Indicadores em INDICADORES_INVERSOS são invertidos (mais = menos vulnerável).
    """
    dim_scores: dict[str, list[pd.Series]] = {}
    for col, (dim, *_) in INDICADORES.items():
        if col not in df.columns:
            continue
        med = df[col].median()
        if pd.isna(med):
            continue  # coluna toda NaN — ignorar indicador
        norm = normalizar_mm(df[col].fillna(med))
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


def interpolate_color(value: float, stops=CORES_IVS) -> str:
    """Interpola cor hexadecimal a partir do IVS normalizado [0,1]."""
    for i in range(len(stops) - 1):
        v0, c0 = stops[i]
        v1, c1 = stops[i + 1]
        if value <= v1:
            t = (value - v0) / (v1 - v0) if v1 > v0 else 0
            r0, g0, b0 = int(c0[1:3], 16), int(c0[3:5], 16), int(c0[5:7], 16)
            r1, g1, b1 = int(c1[1:3], 16), int(c1[3:5], 16), int(c1[5:7], 16)
            r = int(r0 + t * (r1 - r0))
            g = int(g0 + t * (g1 - g0))
            b = int(b0 + t * (b1 - b0))
            return f"#{r:02x}{g:02x}{b:02x}"
    return stops[-1][1]


def classe_ivs(v: float) -> str:
    if v < 0.33:
        return "Baixa"
    if v < 0.66:
        return "Média"
    return "Alta"


def _enrich_geojson(df: pd.DataFrame, geojson_str: str) -> dict:
    """Enriquece features do GeoJSON com IVS, cor, nome e indicadores chave."""
    df = df.copy()
    df["id_ubs"] = df["id_ubs"].astype(str).str.strip()
    ivs_map  = df.set_index("id_ubs")["ivs_parcial"].to_dict()
    cor_map  = df.set_index("id_ubs").apply(lambda r: interpolate_color(r["ivs_parcial"]), axis=1).to_dict()
    nome_map = df.set_index("id_ubs")["no_ubs"].to_dict()
    saneam_map = df.set_index("id_ubs")["D2_sem_saneam"].to_dict() if "D2_sem_saneam" in df.columns else {}
    analf_map  = df.set_index("id_ubs")["D1_analf"].to_dict() if "D1_analf" in df.columns else {}

    geojson = json.loads(geojson_str)
    for feat in geojson["features"]:
        cnes = str(feat["properties"].get("cnes", ""))
        feat["properties"]["ivs"]        = round(ivs_map.get(cnes, 0), 3)
        feat["properties"]["cor"]        = cor_map.get(cnes, "#999")
        feat["properties"]["nome"]       = nome_map.get(cnes, cnes)
        feat["properties"]["sem_saneam"] = round(float(saneam_map.get(cnes, 0) or 0), 1)
        feat["properties"]["analf"]      = round(float(analf_map.get(cnes, 0) or 0), 1)
    return geojson


def gerar_json(df: pd.DataFrame, geojson_str: str, slug: str, cidade: str,
               uf: str = "", ibge: str = "", out_json: Path | None = None) -> None:
    """Exporta data/{slug}.json e atualiza data/municipios.json."""
    n_ubs    = len(df)
    ivs_mean = float(df["ivs_parcial"].mean())
    ivs_max  = float(df["ivs_parcial"].max())
    ivs_min  = float(df["ivs_parcial"].min())
    n_baixa  = int((df["ivs_parcial"] < 0.33).sum())
    n_media  = int(((df["ivs_parcial"] >= 0.33) & (df["ivs_parcial"] < 0.66)).sum())
    n_alta   = int((df["ivs_parcial"] >= 0.66).sum())

    geojson  = _enrich_geojson(df, geojson_str)

    df2 = df.copy()
    df2["id_ubs"] = df2["id_ubs"].astype(str).str.strip()
    ind_cols = [c for c in INDICADORES if c in df2.columns]
    dim_cols = [d for d in ("D1", "D2", "D3", "D4", "D5") if d in df2.columns]

    tabela = []
    for _, r in df2.sort_values("ivs_parcial", ascending=False).iterrows():
        row: dict = {
            "id_ubs":   str(r["id_ubs"]),
            "nome_ubs": str(r.get("no_ubs", r["id_ubs"])),
            "pop_total": int(r["pop_total"]) if pd.notna(r.get("pop_total")) else None,
            "ivs":    round(float(r["ivs_parcial"]), 3),
            "classe": classe_ivs(float(r["ivs_parcial"])),
            "cor":    interpolate_color(float(r["ivs_parcial"])),
        }
        for d in dim_cols:
            v = r.get(d)
            row[d] = round(float(v), 3) if pd.notna(v) else None
        for c in ind_cols:
            v = r.get(c)
            row[c] = round(float(v), 2) if pd.notna(v) else None
        tabela.append(row)

    hoje = datetime.date.today().isoformat()
    payload = {
        "meta": {
            "slug": slug, "nome": cidade, "uf": uf, "ibge": ibge,
            "n_ubs": n_ubs, "n_indicadores": len(ind_cols),
            "ivs_medio":      round(ivs_mean, 3),
            "ivs_max":        round(ivs_max, 3),
            "ivs_min":        round(ivs_min, 3),
            "mean_sem_saneam": round(float(df["D2_sem_saneam"].mean()), 1) if "D2_sem_saneam" in df.columns else None,
            "mean_analf":      round(float(df["D1_analf"].mean()), 1)      if "D1_analf"      in df.columns else None,
            "n_baixa": n_baixa, "n_media": n_media, "n_alta": n_alta,
            "ano_censo": 2022, "gerado_em": hoje,
        },
        "geojson": geojson,
        "tabela": tabela,
    }

    if out_json is None:
        out_json = BASE / "docs" / "data" / f"{slug}.json"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    log.info("JSON gerado: %s (%.1fKB)", out_json, out_json.stat().st_size / 1024)

    # Atualiza manifesto data/municipios.json
    manifest_path = out_json.parent / "municipios.json"
    manifest: list[dict] = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else []
    entry = {
        "slug": slug, "nome": cidade, "uf": uf, "ibge": ibge,
        "n_ubs": n_ubs,
        "ivs_medio": round(ivs_mean, 3),
        "n_baixa": n_baixa, "n_media": n_media, "n_alta": n_alta,
        "gerado_em": hoje,
    }
    manifest = [m for m in manifest if m.get("slug") != slug]
    manifest.append(entry)
    manifest.sort(key=lambda m: m["nome"])
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Manifesto atualizado: %s (%d município(s))", manifest_path, len(manifest))


def gerar_html(df: pd.DataFrame, geojson_str: str) -> str:
    n_ubs = len(df)
    ivs_mean = df["ivs_parcial"].mean()
    ivs_max  = df["ivs_parcial"].max()
    ivs_min  = df["ivs_parcial"].min()

    n_baixa  = (df["ivs_parcial"] < 0.33).sum()
    n_media  = ((df["ivs_parcial"] >= 0.33) & (df["ivs_parcial"] < 0.66)).sum()
    n_alta   = (df["ivs_parcial"] >= 0.66).sum()

    top5 = df.nlargest(5, "ivs_parcial")[["no_ubs", "ivs_parcial"]]

    # --- Tabela HTML ---
    def fmt_dim(v):
        """Formata score de dimensão [0,1] → barra colorida + número."""
        if not pd.notna(v):
            return '<span style="color:#bbb;font-size:.8em">n/d</span>'
        cor = interpolate_color(float(v))
        pct = int(v * 100)
        bar = (
            f'<div style="display:flex;align-items:center;gap:5px">'
            f'<div style="flex:1;height:6px;border-radius:3px;background:#e8edf2">'
            f'<div style="width:{pct}%;height:100%;border-radius:3px;background:{cor}"></div>'
            f'</div>'
            f'<span style="min-width:32px;text-align:right;font-size:.82em">{v:.2f}</span>'
            f'</div>'
        )
        return bar

    rows_html = ""
    for _, r in df.sort_values("ivs_parcial", ascending=False).iterrows():
        ivs_v = r["ivs_parcial"]
        cor = interpolate_color(ivs_v)
        cls = classe_ivs(ivs_v)
        cls_badge = (
            f'<span style="background:{cor};color:#fff;border-radius:4px;'
            f'padding:2px 8px;font-size:.8em;font-weight:700">{cls}</span>'
        )
        pop = f"{r['pop_total']:,.0f}" if pd.notna(r.get("pop_total")) else "—"
        nome = r["no_ubs"].replace("UBS ", "").title()
        ivs_bar = fmt_dim(ivs_v)
        rows_html += f"""
        <tr>
          <td style="font-weight:600;color:#1a3a5c">{nome}</td>
          <td style="text-align:right">{pop}</td>
          <td>{fmt_dim(r.get('D1'))}</td>
          <td>{fmt_dim(r.get('D2'))}</td>
          <td>{fmt_dim(r.get('D3'))}</td>
          <td>{fmt_dim(r.get('D4'))}</td>
          <td>{fmt_dim(r.get('D5'))}</td>
          <td>{ivs_bar}</td>
          <td style="text-align:center">{cls_badge}</td>
        </tr>"""

    geojson_enriched = json.dumps(_enrich_geojson(df, geojson_str))

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>IVSaúde {NOME_CIDADE}</title>
<meta name="description" content="Índice de Vulnerabilidade em Saúde — territórios de UBS de {NOME_CIDADE}. Dados IBGE Censo 2022.">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',Arial,sans-serif;background:#f0f3f7;color:#222;line-height:1.6}}
.hero{{background:linear-gradient(135deg,#1a3a5c,#1a5276,#2471a3);color:#fff;padding:56px 48px 48px;text-align:center}}
.hero h1{{font-size:2.4em;font-weight:800;letter-spacing:-.01em}}
.hero p{{font-size:1.1em;opacity:.88;max-width:640px;margin:10px auto 0}}
.hero .sub{{margin-top:14px;font-size:.88em;opacity:.7}}
.badges{{display:flex;justify-content:center;gap:10px;flex-wrap:wrap;margin-top:18px}}
.badge{{border-radius:20px;padding:4px 16px;font-size:.82em;background:rgba(255,255,255,.15);border:1px solid rgba(255,255,255,.35)}}
.container{{max-width:1140px;margin:0 auto;padding:44px 22px}}
.section-title{{font-size:1.1em;font-weight:700;color:#1a5276;text-transform:uppercase;letter-spacing:.06em;margin-bottom:20px;padding-bottom:8px;border-bottom:2px solid #2980b9}}
.stats-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:16px;margin-bottom:44px}}
.stat{{background:#fff;border-radius:10px;padding:18px 14px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.07)}}
.stat-val{{font-size:2em;font-weight:800;color:#1a5276}}
.stat-lbl{{font-size:.78em;color:#666;margin-top:3px}}
.classes{{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:44px}}
.cls{{flex:1;min-width:120px;border-radius:8px;padding:14px;text-align:center;color:#fff}}
.cls-lbl{{font-size:.78em;font-weight:600;opacity:.9}}
.cls-val{{font-size:1.8em;font-weight:800}}
.cls-range{{font-size:.75em;opacity:.8;margin-top:2px}}
#map{{height:520px;border-radius:12px;box-shadow:0 3px 12px rgba(0,0,0,.12);margin-bottom:44px}}
.table-wrap{{overflow-x:auto;margin-bottom:44px}}
table{{width:100%;border-collapse:collapse;background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.07)}}
th{{background:#1a5276;color:#fff;padding:10px 12px;font-size:.82em;text-align:left;position:sticky;top:0}}
td{{padding:8px 12px;font-size:.84em;border-bottom:1px solid #eee}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:#f4f8fc}}
.legend{{background:#fff;padding:14px 18px;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.1)}}
.legend-title{{font-weight:700;font-size:.88em;margin-bottom:8px}}
.legend-bar{{height:14px;border-radius:4px;background:linear-gradient(to right,#27ae60,#f1c40f,#e67e22,#c0392b);margin-bottom:4px}}
.legend-labels{{display:flex;justify-content:space-between;font-size:.75em;color:#555}}
.method{{background:#fff;border-radius:10px;padding:26px 28px;margin-bottom:44px;box-shadow:0 2px 8px rgba(0,0,0,.07)}}
.method p{{font-size:.9em;color:#444;line-height:1.6;margin-bottom:8px}}
.method ul{{padding-left:20px;font-size:.88em;color:#555;line-height:1.8}}
footer{{text-align:center;font-size:.8em;color:#aaa;padding:28px 0 20px;border-top:1px solid #e3e8ee;margin-top:12px}}
footer a{{color:#2980b9;text-decoration:none}}
@media(max-width:600px){{.hero{{padding:36px 22px 32px}}.hero h1{{font-size:1.7em}}}}
</style>
</head>
<body>

<div class="hero">
  <h1>IVSaúde {NOME_CIDADE}</h1>
  <p>Índice de Vulnerabilidade em Saúde dos Territórios das Unidades Básicas de Saúde</p>
  <p class="sub">Dados IBGE Censo 2022 · Metodologia Determinantes Sociais da Saúde (Dahlgren &amp; Whitehead, 1991)</p>
  <div class="badges">
    <span class="badge">{n_ubs} UBS</span>
    <span class="badge">{len(INDICADORES)} Indicadores</span>
    <span class="badge">Territórios Voronoi</span>
    <span class="badge">Censo 2022 + OpenStreetMap</span>
    <span class="badge">⚠ IVS — 5 dimensões</span>
  </div>
</div>

<div class="container">

  <div class="section-title">Resumo — {NOME_CIDADE} (Censo IBGE 2022)</div>
  <div class="stats-grid">
    <div class="stat"><div class="stat-val">{n_ubs}</div><div class="stat-lbl">UBS avaliadas</div></div>
    <div class="stat"><div class="stat-val">{ivs_mean:.3f}</div><div class="stat-lbl">IVS médio parcial</div></div>
    <div class="stat"><div class="stat-val">{ivs_max:.3f}</div><div class="stat-lbl">IVS máximo</div></div>
    <div class="stat"><div class="stat-val">{ivs_min:.3f}</div><div class="stat-lbl">IVS mínimo</div></div>
    <div class="stat"><div class="stat-val">{round(df['D2_sem_saneam'].mean(),1)}%</div><div class="stat-lbl">Média sem saneamento</div></div>
    <div class="stat"><div class="stat-val">{round(df['D1_analf'].mean(),1)}%</div><div class="stat-lbl">Média analfabetismo 15+</div></div>
  </div>

  <div class="section-title">Distribuição por Classe (IVS)</div>
  <div class="classes">
    <div class="cls" style="background:#27ae60">
      <div class="cls-lbl">Baixa</div>
      <div class="cls-val">{n_baixa}</div>
      <div class="cls-range">&lt; 0,33 · {round(n_baixa/n_ubs*100)}%</div>
    </div>
    <div class="cls" style="background:#e67e22">
      <div class="cls-lbl">Média</div>
      <div class="cls-val">{n_media}</div>
      <div class="cls-range">0,33 – 0,66 · {round(n_media/n_ubs*100)}%</div>
    </div>
    <div class="cls" style="background:#c0392b">
      <div class="cls-lbl">Alta</div>
      <div class="cls-val">{n_alta}</div>
      <div class="cls-range">≥ 0,66 · {round(n_alta/n_ubs*100)}%</div>
    </div>
  </div>

  <div class="section-title">Mapa Interativo — IVS por Território de UBS</div>
  <div id="map"></div>

  <div class="section-title">Indicadores por UBS (ordenado por vulnerabilidade)</div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>UBS</th>
          <th style="text-align:right">Pop. estimada</th>
          <th title="D1 — Condição Socioeconômica (analfabetismo, raça)">D1 <span style="font-weight:400;opacity:.7;font-size:.85em">Socioecon.</span></th>
          <th title="D2 — Habitação e Saneamento (esgoto, lixo)">D2 <span style="font-weight:400;opacity:.7;font-size:.85em">Habitação</span></th>
          <th title="D3 — Capital Social (entidades comunitárias OSM por 1.000 hab)">D3 <span style="font-weight:400;opacity:.7;font-size:.85em">Capital Social</span></th>
          <th title="D4 — Saúde Adolescente (% femininas 10-19 anos, proxy mães adolescentes)">D4 <span style="font-weight:400;opacity:.7;font-size:.85em">Saúde Adol.</span></th>
          <th title="D5 — Perfil Demográfico (faixas etárias)">D5 <span style="font-weight:400;opacity:.7;font-size:.85em">Demográfico</span></th>
          <th title="IVS — D1×50% + D2×30% + D3×10% + D4×8% + D5×2%">IVS</th>
          <th>Classe</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>

  <div class="section-title">Metodologia</div>
  <div class="method">
    <p><strong>IVS</strong> — calculado com {len(INDICADORES)} indicadores, agrupados em 5 dimensões com pesos diferenciados (D1=50%, D2=30%, D3=10%, D4=8%, D5=2%):</p>
    <ul>
      <li><strong>D1 — Condição Socioeconômica:</strong> % analfabetismo 15+, % população preta+parda <em>(Censo IBGE 2022)</em></li>
      <li><strong>D2 — Habitação e Saneamento:</strong> % domicílios sem saneamento adequado, % sem coleta de lixo, % crianças 0-3 fora da creche, % crianças 5-14 fora do fundamental <em>(Censo IBGE 2022 + Censo Escolar INEP)</em></li>
      <li><strong>D3 — Capital Social:</strong> entidades comunitárias por 1.000 hab (INVERSO: mais = menos vulnerável) <em>(OpenStreetMap 2025)</em></li>
      <li><strong>D4 — Saúde do Adolescente:</strong> % femininas 10-19 anos (proxy de risco para maternidade adolescente) <em>(Censo IBGE 2022)</em></li>
      <li><strong>D5 — Perfil Demográfico:</strong> % crianças &lt;1 ano (proxy 0-4/5), % adolescentes 10-19, % mulheres 10-49, % idosos 60+ <em>(Censo IBGE 2022)</em></li>
    </ul>
    <p style="margin-top:12px">Cada indicador é normalizado min-max [0,1] dentro do município. D3 é invertido (maior densidade de OSC = menor vulnerabilidade). Territórios definidos por diagrama de Voronoi a partir dos pontos CNES. Fontes: IBGE Censo Demográfico 2022; Censo Escolar INEP 2025; OpenStreetMap via Overpass API.</p>
    <p style="margin-top:8px;color:#888;font-size:.85em">Indicadores ausentes desta versão (fontes pendentes): Bolsa Família, óbitos por causas violentas, cobertura ESF por CNES.</p>
  </div>

</div>

<footer>
  IVSaúde {NOME_CIDADE} · <a href="https://github.com/noharm-ai/ivs-ubs">github.com/noharm-ai/ivs-ubs</a>
  · IBGE Censo 2022 · Pipeline open-source
</footer>

<script>
const geojson = {geojson_enriched};

const map = L.map('map').setView([-14.24, -51.93], 4);

L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
  maxZoom: 18
}}).addTo(map);

function style(feature) {{
  return {{
    fillColor: feature.properties.cor || '#999',
    weight: 1,
    opacity: 0.8,
    color: '#fff',
    fillOpacity: 0.75
  }};
}}

const info = L.control({{position: 'topright'}});
info.onAdd = function() {{
  this._div = L.DomUtil.create('div', 'legend');
  this._div.innerHTML = '<div class="legend-title">IVS</div>'
    + '<div class="legend-bar"></div>'
    + '<div class="legend-labels"><span>Baixo</span><span>Alto</span></div>';
  return this._div;
}};
info.addTo(map);

let selectedLayer = null;
function highlightFeature(e) {{
  e.target.setStyle({{weight: 3, color: '#333', fillOpacity: 0.9}});
}}
function resetHighlight(e) {{
  if (selectedLayer !== e.target) geojsonLayer.resetStyle(e.target);
}}
function selectFeature(e) {{
  if (selectedLayer) geojsonLayer.resetStyle(selectedLayer);
  selectedLayer = e.target;
  const p = e.target.feature.properties;
  const popup = `<b>${{p.nome}}</b><br>IVS: <b>${{p.ivs}}</b><br>Sem saneamento: <b>${{p.sem_saneam}}%</b><br>Analfabetismo 15+: <b>${{p.analf}}%</b>`;
  L.popup().setLatLng(e.latlng).setContent(popup).openOn(map);
}}

const geojsonLayer = L.geoJSON(geojson, {{
  style: style,
  onEachFeature: function(feature, layer) {{
    layer.on({{mouseover: highlightFeature, mouseout: resetHighlight, click: selectFeature}});
  }}
}}).addTo(map);

map.fitBounds(geojsonLayer.getBounds());
</script>
</body></html>"""
    return html


def main():
    parser = argparse.ArgumentParser(description="Gera página IVSaúde por município")
    parser.add_argument(
        "--base-dir",
        default=str(Path(__file__).resolve().parents[1] / "ivs_municipio"),
        help="Diretório base de dados (ex.: ivs_betim)",
    )
    parser.add_argument("--slug",   default=DEFAULT_SLUG,   help="Slug do município (ex.: betim)")
    parser.add_argument("--cidade", default=DEFAULT_CIDADE, help="Nome da cidade para exibição")
    parser.add_argument("--uf",     default="",             help="Sigla do estado (ex.: RS)")
    parser.add_argument("--ibge",   default="",             help="Código IBGE do município (ex.: 4314407)")
    parser.add_argument(
        "--out-html",
        default=str(Path(__file__).resolve().parents[1] / "docs" / "index.html"),
        help="Arquivo HTML de saída (padrão: docs/index.html)",
    )
    parser.add_argument(
        "--out-json",
        default=None,
        help="Arquivo JSON de saída (padrão: docs/data/{slug}.json)",
    )
    parser.add_argument(
        "--no-html", action="store_true",
        help="Não gera HTML; apenas exporta JSON",
    )
    args = parser.parse_args()

    _configure_runtime(Path(args.base_dir), slug=args.slug, cidade=args.cidade, out_html=Path(args.out_html))
    log.info("Configuração ativa: base_dir=%s, slug=%s, cidade=%s, out=%s", args.base_dir, args.slug, args.cidade, OUT_HTML)

    log.info("Carregando dados...")
    if not IVS_FILE.exists():
        legacy_candidates = [PROC / "ivs_municipio.csv"]
        ivs_file = next((p for p in legacy_candidates if p.exists()), IVS_FILE)
    else:
        ivs_file = IVS_FILE
    df = pd.read_csv(ivs_file, dtype={"id_ubs": str})
    geojson_path = PROC / "territorios_voronoi_ubs.geojson"
    geojson_str = geojson_path.read_text(encoding="utf-8")

    log.info("Calculando scores por dimensão e IVS...")
    dim_df = calcular_dimensoes(df)
    df = df.join(dim_df)

    if not args.no_html:
        log.info("Gerando HTML...")
        html = gerar_html(df, geojson_str)
        OUT_HTML.write_text(html, encoding="utf-8")
        log.info("HTML gerado: %s (%.1fKB)", OUT_HTML, len(html) / 1024)

    out_json = Path(args.out_json) if args.out_json else None
    log.info("Gerando JSON...")
    gerar_json(df, geojson_str, slug=args.slug, cidade=args.cidade,
               uf=args.uf, ibge=args.ibge, out_json=out_json)


if __name__ == "__main__":
    main()
