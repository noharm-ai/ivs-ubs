from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import unicodedata
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import geopandas as gpd
import pandas as pd
import requests
try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    def tqdm(iterable=None, *args, **kwargs):  # type: ignore[override]
        return iterable

from gerar_voronoi import generate_voronoi

# Carrega .env da raiz do projeto (não sobrescreve variáveis já definidas no ambiente)
_env_file = Path(__file__).resolve().parents[1] / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_MUNICIPIO_IBGE = "4314407"
DEFAULT_UF = "RS"
DEFAULT_CIDADE = "Municipio"
DEFAULT_SLUG = "municipio"

MUNICIPIO_IBGE = DEFAULT_MUNICIPIO_IBGE
MUNICIPIO_IBGE_6 = MUNICIPIO_IBGE[:6]
UF = DEFAULT_UF
CIDADE = DEFAULT_CIDADE
SLUG = DEFAULT_SLUG


@dataclass
class StatusRow:
    fonte: str
    arquivo: str
    status: str
    n_registros: int
    observacao: str


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": f"ivs-{SLUG}/1.0"})
    return s


def _slugify(text: str) -> str:
    txt = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"[^a-zA-Z0-9]+", "_", txt).strip("_").lower()
    return txt or "municipio"


def _configure_runtime(municipio_ibge: str, uf: str, cidade: str, slug: str | None = None) -> None:
    global MUNICIPIO_IBGE, MUNICIPIO_IBGE_6, UF, CIDADE, SLUG
    MUNICIPIO_IBGE = str(municipio_ibge).strip()
    MUNICIPIO_IBGE_6 = MUNICIPIO_IBGE[:6]
    UF = str(uf).strip().upper()
    CIDADE = str(cidade).strip()
    SLUG = _slugify(slug or cidade)


def _city_file(prefix: str, ext: str = "csv") -> str:
    return f"{SLUG}_{prefix}.{ext}"



def _ensure_structure(base_dir: Path) -> None:
    folders = [
        base_dir / "data" / "raw" / "cnes",
        base_dir / "data" / "raw" / "ibge_setores",
        base_dir / "data" / "raw" / "ibge_universo",
        base_dir / "data" / "raw" / "cnpj",
        base_dir / "data" / "raw" / "cnefe",
        base_dir / "data" / "processed",
    ]
    for d in folders:
        d.mkdir(parents=True, exist_ok=True)


def _rel(base_dir: Path, path: Path) -> str:
    try:
        return path.relative_to(base_dir).as_posix()
    except ValueError:
        return str(path)


def _download_with_tqdm(url: str, dest: Path, session: Optional[requests.Session] = None,
                        timeout: int = 180, skip_if_exists: bool = True) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if skip_if_exists and dest.exists() and dest.stat().st_size > 0:
        log.info("  já existe: %s", dest.name)
        return dest

    s = session or _session()
    with s.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with tqdm(
            total=total if total > 0 else None,
            unit="B",
            unit_scale=True,
            desc=f"download {dest.name}",
            leave=False,
        ) as pbar:
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if not chunk:
                        continue
                    f.write(chunk)
                    pbar.update(len(chunk))
    return dest


def _download_first_available(urls: list[str], dest: Path, session: Optional[requests.Session] = None,
                              timeout: int = 180, skip_if_exists: bool = True) -> str:
    """
    Tenta baixar da primeira URL disponível e retorna a URL efetivamente usada.
    """
    if skip_if_exists and dest.exists() and dest.stat().st_size > 0:
        return "arquivo_existente"

    last_err: Exception | None = None
    for url in urls:
        try:
            _download_with_tqdm(url, dest, session=session, timeout=timeout, skip_if_exists=False)
            return url
        except Exception as e:  # noqa: BLE001
            last_err = e
    if last_err:
        raise last_err
    raise RuntimeError("Nenhuma URL fornecida para download")


def _list_zip_links(dir_url: str, session: Optional[requests.Session] = None, timeout: int = 30) -> list[str]:
    s = session or _session()
    try:
        r = s.get(dir_url, timeout=timeout)
        r.raise_for_status()
    except Exception:  # noqa: BLE001
        return []
    links = re.findall(r"""href=["']?([^"'>\s]+\.zip)""", r.text, flags=re.IGNORECASE)
    out = []
    seen = set()
    for link in links:
        if link.startswith("http://") or link.startswith("https://"):
            abs_url = link
        else:
            abs_url = dir_url.rstrip("/") + "/" + link.lstrip("/")
        if abs_url not in seen:
            seen.add(abs_url)
            out.append(abs_url)
    return out


def _unzip(archive: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, "r") as zf:
        zf.extractall(dest_dir)


def _first_non_empty(d: dict, keys: list[str]) -> str:
    d_lower = {str(k).lower(): v for k, v in d.items()}
    for key in keys:
        if key in d and d[key] not in (None, ""):
            return str(d[key]).strip()
        lk = key.lower()
        if lk in d_lower and d_lower[lk] not in (None, ""):
            return str(d_lower[lk]).strip()
    return ""


def _parse_cnes_payload(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for k in ("content", "items", "data", "result"):
            v = payload.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def _extract_cnes_from_xml(xml_path: Path) -> list[dict]:
    import xml.etree.ElementTree as ET

    rows: list[dict] = []
    for _, elem in ET.iterparse(xml_path, events=("start",)):
        tag = elem.tag.rsplit("}", 1)[-1].upper()
        if tag != "ROW":
            continue
        attrs = dict(elem.attrib)
        municipio = (attrs.get("CO_MUNICIPIO_GESTOR") or attrs.get("CO_MUNICIPIO") or "").strip()
        tp = (attrs.get("TP_UNIDADE") or attrs.get("CO_TIPO_UNIDADE") or "").strip().zfill(2)
        if municipio not in (MUNICIPIO_IBGE, MUNICIPIO_IBGE_6):
            elem.clear()
            continue
        if tp not in {"01", "02"}:
            elem.clear()
            continue

        lat_key = next((k for k in attrs if "LAT" in k.upper()), "")
        lon_key = next((k for k in attrs if "LON" in k.upper() or "LNG" in k.upper()), "")
        rows.append(
            {
                "co_cnes": attrs.get("CO_CNES", "").strip(),
                "no_fantasia": attrs.get("NO_FANTASIA", "").strip(),
                "nu_latitude": attrs.get(lat_key, "").strip() if lat_key else "",
                "nu_longitude": attrs.get(lon_key, "").strip() if lon_key else "",
                "co_municipio_gestor": municipio,
                "tp_unidade": tp,
            }
        )
        elem.clear()
    return rows


# Mapeia sigla UF → prefixo numérico de 2 dígitos do código IBGE de município
_UF_IBGE_PREFIX = {
    "RO":"11","AC":"12","AM":"13","RR":"14","PA":"15","AP":"16","TO":"17",
    "MA":"21","PI":"22","CE":"23","RN":"24","PB":"25","PE":"26","AL":"27",
    "SE":"28","BA":"29","MG":"31","ES":"32","RJ":"33","SP":"35","PR":"41",
    "SC":"42","RS":"43","MS":"50","MT":"51","GO":"52","DF":"53",
}


def _build_cnes_uf_cache(base_csv: Path, uf_cache: Path) -> None:
    """
    Lê o tbEstabelecimento completo uma única vez e salva um CSV por UF
    com apenas UBS (TP_UNIDADE 1 e 2) — usado como cache para evitar
    re-leitura do arquivo de 600k linhas a cada município.
    """
    sep, encoding = _detect_csv_sep(base_csv)
    cols = ["CO_CNES", "NO_FANTASIA", "CO_MUNICIPIO_GESTOR", "TP_UNIDADE",
            "CO_TIPO_UNIDADE", "NU_LATITUDE", "NU_LONGITUDE"]
    chunks: list[pd.DataFrame] = []
    mun_prefix = _UF_IBGE_PREFIX.get(UF.upper(), "")
    for chunk in pd.read_csv(base_csv, sep=sep, dtype=str, encoding=encoding,
                              low_memory=False, usecols=lambda c: c in cols,
                              chunksize=200_000):
        tp = chunk.get("TP_UNIDADE", pd.Series(dtype=str)).fillna("")
        if "CO_TIPO_UNIDADE" in chunk.columns:
            tp = tp.where(tp.astype(str).str.strip() != "", chunk["CO_TIPO_UNIDADE"])
        tp = tp.astype(str).str.replace(r"\D", "", regex=True).str.zfill(2)
        mun = chunk.get("CO_MUNICIPIO_GESTOR", pd.Series(dtype=str)).astype(str).str.replace(r"\D", "", regex=True)
        mask = tp.isin({"01", "02"}) & mun.str.startswith(mun_prefix)
        filt = chunk.loc[mask].copy()
        if not filt.empty:
            chunks.append(filt)
    result = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=cols)
    uf_cache.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(uf_cache, index=False)
    log.info("  [CNES cache] %d UBS de %s salvas em %s", len(result), UF.upper(), uf_cache.name)


def _build_ibge_uf_cache(
    csvs: list[Path],
    uf_cache_dir: Path,
    uf: str,
    themes: dict,
) -> None:
    """
    Lê cada CSV nacional IBGE uma única vez e salva versão filtrada por UF em
    data/Agregados_por_Setores_Censitarios/{UF}/Basico_{UF}.csv, etc.

    Chamado automaticamente na primeira execução de qualquer município da UF;
    os demais municípios reusam esses arquivos menores (~1/27 do tamanho nacional).
    """
    uf_cache_dir.mkdir(parents=True, exist_ok=True)
    mun_prefix = _UF_IBGE_PREFIX.get(uf.upper(), "")
    if not mun_prefix:
        log.warning("  [IBGE cache UF] prefixo IBGE não encontrado para UF=%s", uf)
        return

    for key, (fname, _, keywords) in themes.items():
        out = uf_cache_dir / fname
        if out.exists() and out.stat().st_size > 0:
            continue
        src = _pick_universe_source_csv(csvs, fname, keywords)
        if not src:
            log.warning("  [IBGE cache UF] CSV não encontrado para tema %s", key)
            continue

        log.info("  [IBGE cache UF=%s] %s ← %s...", uf, fname, src.name)
        sep, encoding = _detect_csv_sep(src)
        first = pd.read_csv(src, sep=sep, nrows=1, dtype=str, encoding=encoding, low_memory=False)
        cols = list(first.columns)
        mun_col = next((c for c in cols if c.upper() in {"CD_MUN", "CD_MUNICIPIO", "CO_MUNICIPIO", "COD_MUN", "COD_MUNICIPIO"}), None)
        setor_col = next((c for c in cols if c.upper() in {"CD_SETOR", "CD_SETOR_CENSITARIO", "SETOR"}), None)

        parts: list[pd.DataFrame] = []
        for chunk in tqdm(
            pd.read_csv(src, sep=sep, dtype=str, encoding=encoding, low_memory=False,
                        chunksize=200_000, on_bad_lines="skip"),
            desc=f"  cache UF={uf} {key}",
            unit="chunk",
            leave=False,
        ):
            ref_col = mun_col or setor_col
            if not ref_col:
                parts.append(chunk)
                continue
            vals = _clean_digits(chunk[ref_col])
            mask = vals.str.startswith(mun_prefix)
            part = chunk.loc[mask].copy()
            if not part.empty:
                parts.append(part)

        result = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=cols)
        result.to_csv(out, index=False)
        log.info("  [IBGE cache UF=%s] %s salvo: %d setores", uf, fname, len(result))


def _extract_cnes_from_base_csv(base_csv: Path) -> list[dict]:
    """
    Extrai UBS do município alvo de um tbEstabelecimento do pacote BASE_DE_DADOS_CNES.
    Usa cache por UF para evitar ler o arquivo completo a cada município.
    """
    uf_cache = Path(__file__).resolve().parents[1] / "data" / UF.upper() / "_cache" / f"cnes_ubs_{UF.upper()}.csv"
    if not uf_cache.exists():
        _build_cnes_uf_cache(base_csv, uf_cache)

    sep, encoding = _detect_csv_sep(uf_cache)
    cols = ["CO_CNES", "NO_FANTASIA", "CO_MUNICIPIO_GESTOR", "TP_UNIDADE",
            "CO_TIPO_UNIDADE", "NU_LATITUDE", "NU_LONGITUDE"]
    df = pd.read_csv(uf_cache, sep=sep, dtype=str, encoding=encoding,
                     low_memory=False, usecols=lambda c: c in cols)
    mun = df.get("CO_MUNICIPIO_GESTOR", pd.Series(dtype=str)).astype(str).str.replace(r"\D", "", regex=True)
    tp = df.get("TP_UNIDADE", pd.Series(dtype=str)).fillna("")
    if "CO_TIPO_UNIDADE" in df.columns:
        tp = tp.where(tp.astype(str).str.strip() != "", df["CO_TIPO_UNIDADE"])
    tp = tp.astype(str).str.replace(r"\D", "", regex=True).str.zfill(2)
    mask = (mun.str.startswith(MUNICIPIO_IBGE) | mun.str.startswith(MUNICIPIO_IBGE_6)) & tp.isin({"01", "02"})
    filt = df.loc[mask].copy()

    records: list[dict] = []
    for _, row in filt.iterrows():
        records.append({
            "co_cnes": str(row.get("CO_CNES", "")).strip(),
            "no_fantasia": str(row.get("NO_FANTASIA", "")).strip(),
            "nu_latitude": str(row.get("NU_LATITUDE", "")).strip(),
            "nu_longitude": str(row.get("NU_LONGITUDE", "")).strip(),
            "co_municipio_gestor": str(row.get("CO_MUNICIPIO_GESTOR", "")).strip(),
            "tp_unidade": str(tp.loc[row.name]).strip(),
        })
    return records


def _detect_csv_sep(path: Path) -> tuple[str, str]:
    for encoding in ("latin-1", "utf-8"):
        for sep in (";", "|", ",", "\t"):
            try:
                sample = pd.read_csv(path, sep=sep, nrows=3, dtype=str, encoding=encoding, low_memory=False)
                if sample.shape[1] > 1:
                    return sep, encoding
            except Exception:  # noqa: BLE001
                continue
    raise ValueError(f"Não foi possível detectar separador para {path}")


def _clean_digits(series: pd.Series) -> pd.Series:
    return series.astype(str).str.replace(r"\D", "", regex=True)


def _filter_csv_by_municipio(in_csv: Path, out_csv: Path, municipio: str) -> int:
    sep, encoding = _detect_csv_sep(in_csv)
    mun7 = municipio
    mun6 = municipio[:6]

    first = pd.read_csv(in_csv, sep=sep, nrows=1, dtype=str, encoding=encoding, low_memory=False)
    cols = list(first.columns)
    mun_col = next(
        (
            c for c in cols
            if c.upper() in {"CD_MUN", "CD_MUNICIPIO", "CO_MUNICIPIO", "COD_MUN", "COD_MUNICIPIO"}
        ),
        None,
    )
    setor_col = next((c for c in cols if c.upper() in {"CD_SETOR", "CD_SETOR_CENSITARIO", "SETOR"}), None)
    if not mun_col and not setor_col:
        raise KeyError(f"CSV sem coluna de município ou setor: {in_csv.name}")

    mb = in_csv.stat().st_size / 1_048_576
    log.info("    lendo %s (%.1f MB)...", in_csv.name, mb)
    parts: list[pd.DataFrame] = []
    for chunk in tqdm(
        pd.read_csv(in_csv, sep=sep, dtype=str, encoding=encoding, low_memory=False,
                    chunksize=200_000, on_bad_lines="skip"),
        desc=f"    {in_csv.name}",
        unit="chunk",
        leave=False,
    ):
        if mun_col:
            vals = _clean_digits(chunk[mun_col])
            mask = vals.str.startswith(mun7) | vals.str.startswith(mun6)
        else:
            vals = _clean_digits(chunk[setor_col])  # type: ignore[index]
            mask = vals.str.startswith(mun7) | vals.str.startswith(mun6)
        part = chunk.loc[mask].copy()
        if not part.empty:
            parts.append(part)

    if parts:
        out = pd.concat(parts, ignore_index=True)
    else:
        out = pd.DataFrame(columns=cols)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    return len(out)


def _pick_universe_source_csv(csv_pool: list[Path], target_name: str, keywords: list[str]) -> Optional[Path]:
    target_lower = target_name.lower()
    exact = next((p for p in csv_pool if p.name.lower() == target_lower), None)
    if exact:
        return exact
    for p in csv_pool:
        name = p.name.lower()
        if all(k in name for k in keywords):
            return p
    return None


def step_cnes(base_dir: Path) -> StatusRow:
    raw_cnes = base_dir / "data" / "raw" / "cnes"
    raw_cnes.mkdir(parents=True, exist_ok=True)
    json_dest = raw_cnes / f"ubs_{SLUG}.json"
    csv_dest = raw_cnes / f"ubs_{SLUG}_pontos.csv"

    records: list[dict] = []
    source = ""

    base_csv_candidates: list[Path] = []
    env_base_csv = os.environ.get("CNES_BASE_CSV_PATH", "").strip()
    if env_base_csv:
        base_csv_candidates.append(Path(env_base_csv))
    base_dirs: list[Path] = []
    env_base_dir = os.environ.get("CNES_BASE_DIR", "").strip()
    if env_base_dir:
        base_dirs.append(Path(env_base_dir))
    base_dirs.extend(
        [
            Path(__file__).resolve().parents[1] / "data" / "BASE_DE_DADOS_CNES_202601",
            Path("data/BASE_DE_DADOS_CNES_202601"),
        ]
    )
    root_data = Path(__file__).resolve().parents[1] / "data"
    for p in sorted(root_data.glob("BASE_DE_DADOS_CNES_*")):
        if p.is_dir():
            base_dirs.append(p)

    seen_files: set[Path] = set()
    for d in base_dirs:
        if not d.exists() or not d.is_dir():
            continue
        for p in sorted(d.glob("tbEstabelecimento*.csv"), reverse=True):
            real = p.resolve()
            if real not in seen_files:
                seen_files.add(real)
                base_csv_candidates.append(real)

    for p in base_csv_candidates:
        try:
            if not p.exists() or not p.is_file():
                continue
            records = _extract_cnes_from_base_csv(p)
            if records:
                source = f"BASE_DE_DADOS_CNES ({p.name})"
                json_dest.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
                break
        except Exception as e:  # noqa: BLE001
            log.warning("  leitura da BASE_DE_DADOS_CNES falhou (%s): %s", p, e)

    api_err = ""
    if not records:
        s = _session()
        url = (
            "https://cnes.datasus.gov.br/services/estabelecimentos-lite"
            f"?municipio={MUNICIPIO_IBGE}&tipoUnidade=1&size=200&page=0"
        )
        for t in range(1, 4):
            try:
                r = s.get(url, timeout=30)
                r.raise_for_status()
                payload = r.json()
                records = _parse_cnes_payload(payload)
                if records:
                    source = "API CNES"
                    json_dest.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
                    break
            except Exception as e:  # noqa: BLE001
                api_err = str(e)
                log.warning("  CNES API tentativa %d/3 falhou: %s", t, e)

    if not records:
        xml_candidates = [
            Path(os.environ.get("CNES_XML_PATH", "")) if os.environ.get("CNES_XML_PATH") else None,
            Path(__file__).resolve().parents[1] / "data" / "CNESBRASIL" / "xmlCNES.xml",
            Path("data/CNESBRASIL/xmlCNES.xml"),
        ]
        for p in xml_candidates:
            if p and p.exists():
                try:
                    records = _extract_cnes_from_xml(p.resolve())
                    if records:
                        source = f"XML CNES ({p.name})"
                        json_dest.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
                        break
                except Exception as e:  # noqa: BLE001
                    log.warning("  fallback XML CNES falhou (%s): %s", p, e)

    rows = []
    for rec in records:
        cnes = _first_non_empty(rec, ["co_cnes", "CO_CNES", "nu_cnes", "NU_CNES"])
        nome = _first_non_empty(rec, ["no_fantasia", "NO_FANTASIA", "nome", "NOME"])
        lat = _first_non_empty(rec, ["nu_latitude", "NU_LATITUDE", "latitude", "LATITUDE", "lat", "LAT"])
        lon = _first_non_empty(rec, ["nu_longitude", "NU_LONGITUDE", "longitude", "LONGITUDE", "lon", "lng", "LON"])
        if cnes:
            rows.append({"cnes": cnes, "nome": nome, "latitude": lat, "longitude": lon})

    df = pd.DataFrame(rows, columns=["cnes", "nome", "latitude", "longitude"]).drop_duplicates(subset=["cnes"])
    csv_dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_dest, index=False)

    n_coords = (
        pd.to_numeric(df["latitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce").notna()
        & pd.to_numeric(df["longitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce").notna()
    ).sum() if not df.empty else 0

    if len(df) == 0:
        status = "MANUAL"
        obs = f"CNES indisponível; API={api_err}"
    elif n_coords == 0:
        status = "PARCIAL"
        obs = f"UBS encontradas, mas sem coordenadas lat/lon para Voronoi (fonte: {source or 'desconhecida'})"
    else:
        status = "OK"
        obs = f"fonte: {source}" if source else ""
    log.info("[ETAPA 1] concluída — %d UBS em %s", len(df), _rel(base_dir, csv_dest))
    return StatusRow("CNES", _rel(base_dir, csv_dest), status, int(len(df)), obs)


def step_voronoi(base_dir: Path) -> StatusRow:
    out_file = base_dir / "data" / "processed" / "territorios_voronoi_ubs.geojson"
    try:
        res = generate_voronoi(base_dir=base_dir, municipio_ibge=MUNICIPIO_IBGE, slug=SLUG)
        cov = float(res["cobertura_area"]) * 100
        log.info(
            "[ETAPA 2] concluída — %d polígonos em %s (cobertura %.2f%%)",
            res["n_poligonos"],
            _rel(base_dir, out_file),
            cov,
        )
        obs = "" if cov >= 95 else f"cobertura de área baixa ({cov:.2f}%)"
        return StatusRow("Voronoi", _rel(base_dir, out_file), "OK", int(res["n_poligonos"]), obs)
    except Exception as e:  # noqa: BLE001
        log.warning("[ETAPA 2] Voronoi não gerado: %s", e)
        return StatusRow(
            "Voronoi",
            _rel(base_dir, out_file),
            "MANUAL",
            0,
            f"não foi possível gerar Voronoi automaticamente ({e})",
        )


def step_ibge(base_dir: Path, skip_large: bool = False) -> list[StatusRow]:
    rows: list[StatusRow] = []

    setores_dir = base_dir / "data" / "raw" / "ibge_setores"
    universo_dir = base_dir / "data" / "raw" / "ibge_universo"
    setores_dir.mkdir(parents=True, exist_ok=True)
    universo_dir.mkdir(parents=True, exist_ok=True)
    setores_geojson = setores_dir / f"setores_{SLUG}.geojson"

    if skip_large:
        rows.append(
            StatusRow(
                "IBGE setores",
                _rel(base_dir, setores_geojson),
                "MANUAL",
                0,
                "download grande pulado (--skip-large)",
            )
        )
    else:
        try:
            local_shp_candidates: list[Path] = []
            env_shp = os.environ.get("IBGE_SETORES_SHP_PATH", "").strip()
            if env_shp:
                local_shp_candidates.append(Path(env_shp))
            env_dir = os.environ.get("IBGE_SETORES_DIR", "").strip()
            if env_dir:
                local_shp_candidates.extend(sorted(Path(env_dir).glob("**/*.shp")))

            repo_root = Path(__file__).resolve().parents[1]
            local_shp_candidates.extend(
                sorted((repo_root / "data" / f"{UF}_setores_CD2022").glob("**/*.shp"))
            )
            local_shp_candidates.extend(sorted(Path(f"data/{UF}_setores_CD2022").glob("**/*.shp")))
            # fallback legado RS apenas quando UF alvo for RS
            if UF == "RS":
                local_shp_candidates.extend(sorted((repo_root / "data" / "RS_setores_CD2022").glob("**/*.shp")))
                local_shp_candidates.extend(sorted(Path("data/RS_setores_CD2022").glob("**/*.shp")))
            local_shp_candidates.extend(sorted(setores_dir.glob("**/*.shp")))

            shp_path: Optional[Path] = None
            seen: set[Path] = set()
            for cand in local_shp_candidates:
                if not cand:
                    continue
                real = cand.resolve()
                if real in seen or not real.exists() or real.suffix.lower() != ".shp":
                    continue
                seen.add(real)
                shp_path = real
                break

            source_setores = ""
            if shp_path:
                source_setores = f"local: {shp_path}"
                gdf = gpd.read_file(shp_path)
            else:
                # Salva no diretório compartilhado por UF — baixado uma única vez
                shared_setores_dir = repo_root / "data" / f"{UF}_setores_CD2022"
                shared_setores_dir.mkdir(parents=True, exist_ok=True)
                setores_zip = shared_setores_dir / f"{UF}_setores_CD2022.zip"
                setores_urls = [
                    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
                    "malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/"
                    f"censo_2022/setores/shp/UF/{UF}_setores_CD2022.zip",
                    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
                    "malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/"
                    f"censo_2022/setores_censitarios_shp/{UF}/SC_Intra_{UF}_2022.zip",
                ]
                used_url = _download_first_available(setores_urls, setores_zip)
                _unzip(setores_zip, shared_setores_dir)
                shps = sorted(shared_setores_dir.glob("**/*.shp"))
                if not shps:
                    raise FileNotFoundError("shapefile de setores não encontrado após extração")
                gdf = gpd.read_file(shps[0])
                source_setores = used_url
            mun_col = next(
                (c for c in gdf.columns if c.upper() in {"CD_MUN", "CD_MUNICIPIO", "CO_MUNICIPIO"}),
                None,
            )
            if mun_col:
                vals = gdf[mun_col].astype(str).str.replace(r"\D", "", regex=True)
                filt = gdf[vals.str.startswith(MUNICIPIO_IBGE) | vals.str.startswith(MUNICIPIO_IBGE_6)].copy()
            else:
                setor_col = next((c for c in gdf.columns if c.upper().startswith("CD_SETOR")), None)
                if not setor_col:
                    raise KeyError("coluna de município/setor não encontrada no shapefile")
                vals = gdf[setor_col].astype(str).str.replace(r"\D", "", regex=True)
                filt = gdf[vals.str.startswith(MUNICIPIO_IBGE) | vals.str.startswith(MUNICIPIO_IBGE_6)].copy()
            if filt.empty:
                raise ValueError(f"nenhum setor encontrado para o município {MUNICIPIO_IBGE} na fonte de setores")

            filt.to_file(setores_geojson, driver="GeoJSON")
            obs = f"fonte: {source_setores}" if source_setores else ""
            rows.append(StatusRow("IBGE setores", _rel(base_dir, setores_geojson), "OK", int(len(filt)), obs))
        except Exception as e:  # noqa: BLE001
            rows.append(
                StatusRow(
                    "IBGE setores",
                    _rel(base_dir, setores_geojson),
                    "MANUAL",
                    0,
                    f"falha no download/processamento ({e})",
                )
            )

    # Mapeamento: chave → (nome_csv_legado, destino_filtrado, keywords_busca)
    # A nova estrutura do IBGE (2025) usa arquivos BR nacionais por tema.
    # Keywords sem "rs" para compatibilidade com ambos os formatos.
    expected = {
        "basico": (f"Basico_{UF}.csv", universo_dir / _city_file("basico"), ["basico"]),
        "domicilio1": (f"Domicilio01_{UF}.csv", universo_dir / _city_file("domicilio"), ["domicilio1"]),
        "domicilio2": (f"Domicilio02_{UF}.csv", universo_dir / _city_file("domicilio2"), ["domicilio2"]),
        "alfabetizacao": (f"Pessoa02_{UF}.csv", universo_dir / _city_file("alfabetizacao"), ["alfabetizacao"]),
        "pessoa01": (f"Pessoa01_{UF}.csv", universo_dir / _city_file("pessoa01"), ["demografia"]),
        "cor_raca": (f"CorRaca_{UF}.csv", universo_dir / _city_file("cor_raca"), ["cor", "raca"]),
        "domicilio3": (f"Domicilio03_{UF}.csv", universo_dir / _city_file("domicilio3"), ["domicilio3"]),
    }
    if skip_large:
        rows.append(
            StatusRow(
                "IBGE universo",
                f"raw/ibge_universo/{SLUG}_*.csv",
                "MANUAL",
                0,
                "download grande pulado (--skip-large)",
            )
        )
    else:
        try:
            repo_root = Path(__file__).resolve().parents[1]
            local_universe_dirs: list[Path] = []
            env_univ_dir = os.environ.get("IBGE_UNIVERSO_DIR", "").strip()
            if env_univ_dir:
                local_universe_dirs.append(Path(env_univ_dir))
            local_universe_dirs.extend(
                [
                    repo_root / "data" / "Agregados_por_Setores_Censitarios",
                    Path("data/Agregados_por_Setores_Censitarios"),
                    universo_dir,
                ]
            )

            csvs: list[Path] = []
            for d in local_universe_dirs:
                if d.exists() and d.is_dir():
                    csvs.extend(sorted(d.glob("**/*.csv")))

            source_universe = "local"
            if not csvs:
                # Diretório compartilhado para os ZIPs/CSVs BR — baixado uma única vez
                shared_universo_dir = repo_root / "data" / "Agregados_por_Setores_Censitarios"
                shared_universo_dir.mkdir(parents=True, exist_ok=True)

                # Nova estrutura IBGE (a partir de 2025): arquivos BR por tema
                new_base = (
                    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
                    "Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/"
                )
                # Zips de cada tema necessário (basico + domicilio são os mais usados)
                new_theme_zips = [
                    "Agregados_por_setores_basico_BR_20250417.zip",
                    "Agregados_por_setores_caracteristicas_domicilio1_BR.zip",
                    "Agregados_por_setores_caracteristicas_domicilio2_BR_20250417.zip",
                    "Agregados_por_setores_caracteristicas_domicilio2_BR.zip",
                    "Agregados_por_setores_demografia_BR.zip",
                    "Agregados_por_setores_alfabetizacao_BR.zip",
                    "Agregados_por_setores_cor_ou_raca_BR.zip",
                    "Agregados_por_setores_cor_raca_BR.zip",
                    "Agregados_por_setores_caracteristicas_domicilio3_BR_20250417.zip",
                    "Agregados_por_setores_caracteristicas_domicilio3_BR.zip",
                ]
                downloaded_any = False
                _ftp_links_cache: list[str] | None = None
                for zip_name in new_theme_zips:
                    zip_dest = shared_universo_dir / zip_name
                    if zip_dest.exists():
                        downloaded_any = True
                        continue
                    try:
                        # Descobre dinamicamente links do FTP (cache para não repetir a listagem)
                        if _ftp_links_cache is None:
                            try:
                                _ftp_links_cache = _list_zip_links(new_base)
                            except Exception:
                                _ftp_links_cache = []
                        candidate_urls: list[str] = [new_base + zip_name]
                        kw = zip_name.split("_")[3] if "_" in zip_name else ""
                        for link in _ftp_links_cache:
                            lname = Path(urlparse(link).path).name.lower()
                            if kw and kw in lname:
                                candidate_urls.insert(0, link)
                        _download_first_available(candidate_urls, zip_dest, timeout=300)
                        _unzip(zip_dest, shared_universo_dir)
                        downloaded_any = True
                    except Exception as e:  # noqa: BLE001
                        log.warning("  não foi possível baixar %s: %s", zip_name, e)

                if downloaded_any:
                    source_universe = new_base
                else:
                    # Fallback: estrutura antiga com zip estadual
                    old_base = (
                        "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
                        "Resultados_do_Universo/Agregados_por_Setores_Censitarios/"
                    )
                    univ_zip = shared_universo_dir / f"{UF}_20231030.zip"
                    if univ_zip.exists() and not zipfile.is_zipfile(univ_zip):
                        log.warning("  ZIP IBGE universo inválido detectado (%s), removendo para novo download", univ_zip.name)
                        univ_zip.unlink()
                    fallback_urls: list[str] = [old_base + f"{UF}_20231030.zip", old_base + f"{UF.lower()}_20231030.zip"]
                    for link in _list_zip_links(old_base):
                        lname = Path(urlparse(link).path).name.upper()
                        if lname.startswith(f"{UF}_") and lname.endswith(".ZIP"):
                            fallback_urls.insert(0, link)
                    used_url = _download_first_available(fallback_urls, univ_zip)
                    source_universe = used_url
                    _unzip(univ_zip, shared_universo_dir)

                csvs = sorted(shared_universo_dir.glob("**/*.csv"))

            log.info("  IBGE universo: %d CSV(s) disponíveis", len(csvs))

            # Cache por UF: construído uma vez, reutilizado por todos os municípios da UF.
            # Os arquivos resultantes têm o mesmo nome que os legados (ex: Basico_RS.csv),
            # portanto _pick_universe_source_csv os encontra automaticamente por match exato.
            uf_universo_cache = repo_root / "data" / "Agregados_por_Setores_Censitarios" / UF.upper()
            missing_in_uf_cache = [
                key for key, (fname, _, _kw) in expected.items()
                if not (uf_universo_cache / fname).exists()
            ]
            if missing_in_uf_cache:
                log.info("  [IBGE cache UF=%s] construindo cache (temas ausentes: %s)...", UF, missing_in_uf_cache)
                _build_ibge_uf_cache(csvs, uf_universo_cache, UF, expected)
            else:
                log.info("  [IBGE cache UF=%s] cache completo, pulando construção", UF)

            # Prefixa csvs com os arquivos de UF para que sejam preferidos pelo match exato
            if uf_universo_cache.exists():
                uf_files = sorted(uf_universo_cache.glob("*.csv"))
                uf_fnames = {f.name for f in uf_files}
                csvs = uf_files + [c for c in csvs if c.name not in uf_fnames]

            counts = []
            missing = []

            def _filter_theme(key: str, fname: str, out_csv: Path, keywords: list) -> tuple:
                src = _pick_universe_source_csv(csvs, fname, keywords)
                if not src:
                    return key, None, fname
                n = _filter_csv_by_municipio(src, out_csv, MUNICIPIO_IBGE)
                return key, n, None

            n_workers = min(len(expected), os.cpu_count() or 4)
            log.info("  filtrando %d temas em paralelo (%d workers)...", len(expected), n_workers)
            with ThreadPoolExecutor(max_workers=n_workers) as ex:
                futures = {
                    ex.submit(_filter_theme, key, fname, out_csv, keywords): key
                    for key, (fname, out_csv, keywords) in expected.items()
                }
                for fut in as_completed(futures):
                    key, n, missing_fname = fut.result()
                    if missing_fname:
                        missing.append(missing_fname)
                        log.info("    %s: não encontrado", key)
                    else:
                        log.info("    %s -> %d setores filtrados", key, n)
                        counts.append(n)
            if missing:
                obs = f"arquivos ausentes: {', '.join(missing)}"
                status = "PARCIAL" if counts else "MANUAL"
                n_reg = int(max(counts) if counts else 0)
            else:
                obs = f"fonte: {source_universe}" if source_universe and source_universe != "local" else "fonte: local"
                status = "OK"
                n_reg = int(max(counts) if counts else 0)
            rows.append(
                StatusRow(
                    "IBGE universo",
                    f"raw/ibge_universo/{SLUG}_*.csv",
                    status,
                    n_reg,
                    obs,
                )
            )
        except Exception as e:  # noqa: BLE001
            rows.append(
                StatusRow(
                    "IBGE universo",
                    f"raw/ibge_universo/{SLUG}_*.csv",
                    "MANUAL",
                    0,
                    f"falha no download/processamento ({e})",
                )
            )

    log.info(
        "[ETAPA 3] concluída — setores=%s; universo=%s",
        rows[0].status if rows else "N/A",
        rows[1].status if len(rows) > 1 else "N/A",
    )
    return rows




_CNPJ_COLS = [
    "CNPJ_BASICO", "CNPJ_ORDEM", "CNPJ_DV", "IDENTIFICADOR_MATRIZ_FILIAL",
    "NOME_FANTASIA", "SITUACAO_CADASTRAL", "DATA_SITUACAO_CADASTRAL",
    "MOTIVO_SITUACAO_CADASTRAL", "NOME_CIDADE_EXTERIOR", "PAIS",
    "DATA_INICIO_ATIVIDADE", "CNAE_FISCAL_PRINCIPAL", "CNAE_FISCAL_SECUNDARIA",
    "TIPO_LOGRADOURO", "LOGRADOURO", "NUMERO", "COMPLEMENTO", "BAIRRO",
    "CEP", "UF", "MUNICIPIO", "DDD_1", "TELEFONE_1", "DDD_2", "TELEFONE_2",
    "DDD_FAX", "FAX", "CORREIO_ELETRONICO", "SITUACAO_ESPECIAL", "DATA_SITUACAO_ESPECIAL",
]

# CNAEs de interesse para capital social (D3): associações, assistência social, centros comunitários
_CNPJ_OSC_CNAE_PREFIXES = ("9430", "9499", "8800", "8899", "9101", "9420", "9411", "9412")

# URL base do CNEFE — Cadastro Nacional de Endereços para Fins Estatísticos (Censo 2022)
_CNEFE_BASE = (
    "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/"
    "Censo_Demografico_2022/Arquivos_CNEFE/CSV/Municipio/"
)

# URL dos arquivos CNPJ da Receita Federal.
# Fonte/catálogo oficial: https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj
# Os ZIPs ficam em: https://arquivos.receitafederal.gov.br/CNPJ/
_CNPJ_BASE_URL = "https://arquivos.receitafederal.gov.br/CNPJ/"


def _get_rf_municipio_code(cidade: str, cnpj_cache_dir: Path, session: requests.Session) -> str | None:
    """Retorna o código de 4 dígitos da Receita Federal para o município."""
    cnpj_cache_dir.mkdir(parents=True, exist_ok=True)
    mun_zip = cnpj_cache_dir / "Municipios.zip"
    mun_csv = cnpj_cache_dir / "Municipios.csv"
    if not mun_csv.exists():
        try:
            _download_with_tqdm(_CNPJ_BASE_URL + "Municipios.zip", mun_zip, session=session, timeout=60)
            with zipfile.ZipFile(mun_zip, "r") as zf:
                names = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
                if names:
                    with zf.open(names[0]) as src, open(mun_csv, "wb") as dst:
                        dst.write(src.read())
        except Exception as e:  # noqa: BLE001
            log.warning("  Municipios.zip RF: %s", e)
            return None

    if not mun_csv.exists():
        return None

    try:
        df = pd.read_csv(mun_csv, sep=";", encoding="latin1", dtype=str, header=None, names=["CODIGO", "DESCRICAO"])
        cidade_norm = unicodedata.normalize("NFKD", cidade.upper()).encode("ascii", "ignore").decode("ascii")
        df["DESCRICAO_NORM"] = df["DESCRICAO"].astype(str).apply(
            lambda x: unicodedata.normalize("NFKD", x.upper()).encode("ascii", "ignore").decode("ascii")
        )
        match = df[df["DESCRICAO_NORM"] == cidade_norm]
        if match.empty:
            match = df[df["DESCRICAO_NORM"].str.startswith(cidade_norm[:6])]
        if not match.empty:
            return str(match.iloc[0]["CODIGO"]).strip().zfill(4)
    except Exception as e:  # noqa: BLE001
        log.warning("  parse Municipios.csv: %s", e)
    return None


def _build_cnpj_uf_cache(cnpj_zip_cache: Path, uf_cache: Path, uf: str) -> None:
    """
    Baixa os 10 ZIPs da Receita Federal (cache compartilhado), filtra por UF + CNAE ativo
    e salva CSV de OSCs da UF em data/{UF}/_cache/cnpj_osc_{UF}.csv.
    Os ZIPs ficam em data/_cache/cnpj/ para reuso entre UFs.
    """
    cnpj_zip_cache.mkdir(parents=True, exist_ok=True)
    uf_cache.parent.mkdir(parents=True, exist_ok=True)
    s = _session()
    all_chunks: list[pd.DataFrame] = []
    n_parts = 10
    for i in range(n_parts):
        zip_name = f"Estabelecimentos{i}.zip"
        zip_dest = cnpj_zip_cache / zip_name
        if zip_dest.exists():
            log.info("  %s: já existe no cache, pulando download", zip_name)
        else:
            try:
                _download_with_tqdm(_CNPJ_BASE_URL + zip_name, zip_dest, session=s, timeout=900)
            except Exception as e:  # noqa: BLE001
                log.warning("  %s: download falhou (%s) — parte ignorada", zip_name, e)
                continue
        try:
            extracted = cnpj_zip_cache / f"_tmp_estab{i}.csv"
            with zipfile.ZipFile(zip_dest, "r") as zf:
                csv_members = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
                if not csv_members:
                    continue
                with zf.open(csv_members[0]) as src, open(extracted, "wb") as dst:
                    dst.write(src.read())

            chunks: list[pd.DataFrame] = []
            for chunk in tqdm(
                pd.read_csv(extracted, sep=";", encoding="latin1", dtype=str, header=None,
                            names=_CNPJ_COLS, chunksize=200_000, low_memory=False),
                desc=f"  parte {i+1}/{n_parts}",
                unit="chunk",
                leave=False,
            ):
                filt = chunk[chunk["UF"].astype(str).str.upper() == uf.upper()]
                filt = filt[filt["SITUACAO_CADASTRAL"].astype(str).str.strip() == "02"]
                filt = filt[filt["CNAE_FISCAL_PRINCIPAL"].astype(str).str[:4].isin(_CNPJ_OSC_CNAE_PREFIXES)]
                if not filt.empty:
                    chunks.append(filt[["CNPJ_BASICO", "CNPJ_ORDEM", "CNPJ_DV",
                                        "NOME_FANTASIA", "CNAE_FISCAL_PRINCIPAL",
                                        "CEP", "LOGRADOURO", "NUMERO", "BAIRRO", "UF", "MUNICIPIO"]])
            extracted.unlink(missing_ok=True)
            if chunks:
                all_chunks.append(pd.concat(chunks, ignore_index=True))
            log.info("  parte %d/%d — %d OSCs acumuladas para %s", i + 1, n_parts,
                     sum(len(c) for c in all_chunks), uf)
        except Exception as e:  # noqa: BLE001
            log.warning("  %s: processamento falhou (%s)", zip_name, e)

    result = pd.concat(all_chunks, ignore_index=True).drop_duplicates(
        subset=["CNPJ_BASICO", "CNPJ_ORDEM", "CNPJ_DV"]
    ) if all_chunks else pd.DataFrame(columns=["CNPJ_BASICO", "CNPJ_ORDEM", "CNPJ_DV",
                                                "NOME_FANTASIA", "CNAE_FISCAL_PRINCIPAL",
                                                "CEP", "LOGRADOURO", "NUMERO", "BAIRRO", "UF", "MUNICIPIO"])
    result.to_csv(uf_cache, index=False)
    log.info("  cache CNPJ UF=%s salvo: %s (%d OSCs)", uf, uf_cache.name, len(result))


def step_cnpj_osc(base_dir: Path, skip_large: bool = True) -> StatusRow:
    """
    Baixa e filtra estabelecimentos CNPJ da Receita Federal com CNAEs de OSC/capital social.
    Por padrão skip_large=True — os arquivos chegam a 5GB cada (10 partes).
    """
    out_csv = base_dir / "data" / "raw" / "cnpj" / _city_file("cnpj_osc")
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    if out_csv.exists() and out_csv.stat().st_size > 0:
        df = pd.read_csv(out_csv, dtype=str)
        if not df.empty:
            log.info("[ETAPA cnpj_osc] arquivo existente (%d registros)", len(df))
            return StatusRow("CNPJ OSC", _rel(base_dir, out_csv), "OK", int(len(df)), "arquivo existente")

    if skip_large:
        pd.DataFrame(columns=["CNPJ_BASICO", "CNAE_FISCAL_PRINCIPAL", "CEP", "UF", "MUNICIPIO"]).to_csv(out_csv, index=False)
        log.info("[ETAPA cnpj_osc] pulado (skip_large)")
        return StatusRow("CNPJ OSC", _rel(base_dir, out_csv), "MANUAL", 0,
                         f"download pulado (--skip-large). Fonte: {_CNPJ_BASE_URL}")

    repo_root = Path(__file__).resolve().parents[1]
    cnpj_zip_cache = repo_root / "data" / "_cache" / "cnpj"
    uf_cache = repo_root / "data" / UF.upper() / "_cache" / f"cnpj_osc_{UF.upper()}.csv"

    # Nível 1: cache da UF — construído uma vez para todos os municípios da UF
    if not uf_cache.exists():
        log.info("  [cnpj_osc] construindo cache UF=%s (ZIPs em %s)...", UF, cnpj_zip_cache)
        _build_cnpj_uf_cache(cnpj_zip_cache, uf_cache, UF)

    # Nível 2: filtro por município sobre o cache da UF
    s = _session()
    rf_code = _get_rf_municipio_code(CIDADE, cnpj_zip_cache, s)
    if not rf_code:
        log.warning("[ETAPA cnpj_osc] código RF não encontrado para %s — usando todos da UF", CIDADE)

    try:
        df_uf = pd.read_csv(uf_cache, dtype=str)
        if rf_code:
            result = df_uf[df_uf["MUNICIPIO"].astype(str).str.zfill(4) == rf_code].copy()
        else:
            result = df_uf.copy()
    except Exception as e:  # noqa: BLE001
        log.warning("  erro ao ler cache UF: %s", e)
        result = pd.DataFrame(columns=["CNPJ_BASICO", "CNAE_FISCAL_PRINCIPAL", "CEP", "UF", "MUNICIPIO"])

    result.to_csv(out_csv, index=False)
    log.info("[ETAPA cnpj_osc] concluída — %d OSCs em %s (cache UF=%s)", len(result), _rel(base_dir, out_csv), UF)
    return StatusRow("CNPJ OSC", _rel(base_dir, out_csv), "OK", int(len(result)), f"rf_code={rf_code}, cache_uf={uf_cache.name}")




def step_cnefe(base_dir: Path) -> StatusRow:
    """
    Baixa o CSV do CNEFE (endereços geocodificados do Censo 2022) para o município.
    Salva apenas colunas essenciais (COD_SETOR, LATITUDE, LONGITUDE) para uso como
    pesos de agregação setor→território em vez da fração de área.
    """
    out_csv = base_dir / "data" / "raw" / "cnefe" / f"{SLUG}_cnefe.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    if out_csv.exists() and out_csv.stat().st_size > 0:
        try:
            n = sum(1 for _ in open(out_csv)) - 1
            log.info("[ETAPA cnefe] arquivo existente (%d endereços)", n)
            return StatusRow("CNEFE", _rel(base_dir, out_csv), "OK", n, "arquivo existente")
        except Exception:
            pass

    uf_prefix = _UF_IBGE_PREFIX.get(UF.upper(), "")
    if not uf_prefix:
        return StatusRow("CNEFE", _rel(base_dir, out_csv), "MANUAL", 0,
                         f"UF {UF} sem prefixo IBGE mapeado")

    uf_dir_url = f"{_CNEFE_BASE}{uf_prefix}_{UF.upper()}/"
    s = _session()

    # Descobre o nome exato do ZIP listando o diretório da UF
    zip_url: str | None = None
    try:
        r = s.get(uf_dir_url, timeout=30)
        r.raise_for_status()
        links = re.findall(r'href=["\']?([^"\'>\s]+\.zip)', r.text, flags=re.IGNORECASE)
        for link in links:
            fname = link.split("/")[-1]
            if fname.startswith(MUNICIPIO_IBGE):
                zip_url = uf_dir_url + fname if not link.startswith("http") else link
                break
    except Exception as e:
        log.warning("  CNEFE: erro ao listar diretório UF: %s", e)

    if not zip_url:
        # Fallback: constrói URL com nome slugificado da cidade
        city_up = unicodedata.normalize("NFKD", CIDADE.upper()).encode("ascii", "ignore").decode("ascii")
        city_up = re.sub(r"[^A-Z0-9]+", "_", city_up).strip("_")
        zip_url = f"{uf_dir_url}{MUNICIPIO_IBGE}_{city_up}.zip"

    # Cache do ZIP compartilhado por município (não apaga entre runs)
    cache_dir = Path(__file__).resolve().parents[1] / "data" / "_cache" / "cnefe"
    cache_dir.mkdir(parents=True, exist_ok=True)
    zip_dest = cache_dir / f"{MUNICIPIO_IBGE}_cnefe.zip"

    try:
        _download_with_tqdm(zip_url, zip_dest, session=s, timeout=300)
    except Exception as e:
        return StatusRow("CNEFE", _rel(base_dir, out_csv), "MANUAL", 0,
                         f"download falhou ({zip_url}): {e}")

    try:
        with zipfile.ZipFile(zip_dest, "r") as zf:
            csv_members = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
            if not csv_members:
                return StatusRow("CNEFE", _rel(base_dir, out_csv), "MANUAL", 0, "ZIP sem CSV")
            with zf.open(csv_members[0]) as src:
                df = pd.read_csv(
                    src, sep=";", dtype=str, encoding="latin-1", low_memory=False,
                    usecols=["COD_SETOR", "LATITUDE", "LONGITUDE", "COD_ESPECIE"],
                    on_bad_lines="skip",
                )
        # Apenas endereços residenciais (espécie 1=domicílio particular permanente, 2=improvisado)
        df = df[df["COD_ESPECIE"].isin(["1", "2"])].copy()
        df["LATITUDE"] = pd.to_numeric(df["LATITUDE"].str.replace(",", ".", regex=False), errors="coerce")
        df["LONGITUDE"] = pd.to_numeric(df["LONGITUDE"].str.replace(",", ".", regex=False), errors="coerce")
        df = df.dropna(subset=["LATITUDE", "LONGITUDE"])
        df = df[["COD_SETOR", "LATITUDE", "LONGITUDE"]].copy()
        df.to_csv(out_csv, index=False)
        log.info("[ETAPA cnefe] %d endereços residenciais salvos em %s", len(df), _rel(base_dir, out_csv))
        return StatusRow("CNEFE", _rel(base_dir, out_csv), "OK", len(df),
                         f"fonte: {zip_url}")
    except Exception as e:
        return StatusRow("CNEFE", _rel(base_dir, out_csv), "MANUAL", 0,
                         f"extração falhou: {e}")


def write_status(base_dir: Path, rows: list[StatusRow]) -> Path:
    out = base_dir / "data" / "processed" / "status_downloads.csv"
    df = pd.DataFrame([asdict(r) for r in rows])
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    return out


def run_pipeline(base_dir: Path, only: str | None = None, skip_large: bool = False) -> list[StatusRow]:
    _ensure_structure(base_dir)
    status_rows: list[StatusRow] = []

    steps = {
        "cnes": lambda: [step_cnes(base_dir)],
        "voronoi": lambda: [step_voronoi(base_dir)],
        "ibge": lambda: step_ibge(base_dir, skip_large=skip_large),
        "cnefe": lambda: [step_cnefe(base_dir)],
        "cnpj_osc": lambda: [step_cnpj_osc(base_dir, skip_large=skip_large)],
    }

    run_order = ["cnes", "voronoi", "ibge", "cnefe", "cnpj_osc"]
    if only:
        run_order = [only]

    total_steps = len(run_order)
    for i, key in enumerate(run_order, 1):
        log.info("  ┌ [%d/%d] %s iniciando...", i, total_steps, key)
        try:
            status_rows.extend(steps[key]())
        except Exception as e:  # noqa: BLE001
            log.exception("Falha inesperada em %s", key)
            status_rows.append(StatusRow(key.upper(), "-", "ERRO", 0, str(e)))
        log.info("  └ [%d/%d] %s concluído", i, total_steps, key)

    status_path = write_status(base_dir, status_rows)
    log.info("[ETAPA 9] concluída — status salvo em %s", _rel(base_dir, status_path))
    return status_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Download de dados IVSaúde por município")
    parser.add_argument(
        "--base-dir",
        default=None,
        help="Diretório base de saída (ex.: ivs_betim). Padrão: ivs_<slug>",
    )
    parser.add_argument("--municipio-ibge", default=DEFAULT_MUNICIPIO_IBGE, help="Código IBGE do município")
    parser.add_argument("--uf", default=DEFAULT_UF, help="Sigla da UF (ex.: MG)")
    parser.add_argument("--cidade", default=DEFAULT_CIDADE, help="Nome da cidade")
    parser.add_argument("--slug", default=None, help="Slug para nomes de arquivo (ex.: betim)")
    parser.add_argument(
        "--only",
        choices=["cnes", "voronoi", "ibge", "cnefe", "cnpj_osc"],
        default=None,
        help="Executa apenas uma etapa/fonte",
    )
    parser.add_argument("--skip-large", action="store_true", help="Pula arquivos grandes (IBGE/Censo Escolar)")
    args = parser.parse_args()

    _configure_runtime(args.municipio_ibge, args.uf, args.cidade, args.slug)
    base_dir = (
        Path(args.base_dir).resolve()
        if args.base_dir
        else (Path(__file__).resolve().parents[1] / "data" / UF.upper() / f"ivs_{SLUG}").resolve()
    )
    log.info(
        "Configuração ativa: cidade=%s, ibge=%s, uf=%s, slug=%s, base_dir=%s",
        CIDADE,
        MUNICIPIO_IBGE,
        UF,
        SLUG,
        base_dir,
    )
    rows = run_pipeline(base_dir=base_dir, only=args.only, skip_large=args.skip_large)

    df = pd.DataFrame([asdict(r) for r in rows])
    if not df.empty:
        print("\nResumo de disponibilidade:")
        print(df.to_string(index=False))

    # Verifica se arquivos críticos foram gerados (necessários para calcular_ivs)
    if not args.only and not args.skip_large:
        voronoi = base_dir / "data" / "processed" / "territorios_voronoi_ubs.geojson"
        if not voronoi.exists() or voronoi.stat().st_size == 0:
            log.error("Voronoi ausente — abortando para retry: %s", voronoi)
            sys.exit(1)

        universo_dir = base_dir / "data" / "raw" / "ibge_universo"
        critical = ["basico", "pessoa01", "alfabetizacao", "domicilio", "cor_raca"]
        missing_critical = [
            key for key in critical
            if not any(universo_dir.glob(f"*{key}*"))
        ]
        if missing_critical:
            log.error("Arquivos IBGE críticos ausentes: %s — abortando para retry", missing_critical)
            sys.exit(1)


if __name__ == "__main__":
    main()
