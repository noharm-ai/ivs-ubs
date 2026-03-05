from __future__ import annotations

import argparse
import json
import logging
import os
import re
import unicodedata
import zipfile
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import geopandas as gpd
import pandas as pd
import requests
try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    class _TqdmNoop:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def update(self, n: int) -> None:
            return None

    def tqdm(*args, **kwargs):  # type: ignore[override]
        return _TqdmNoop()

from download_sim import download_sim_municipio
from download_sinasc import download_sinasc_municipio
from gerar_voronoi import generate_voronoi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_MUNICIPIO_IBGE = "4314407"
DEFAULT_UF = "RS"
DEFAULT_CIDADE = "Pelotas"
DEFAULT_SLUG = "pelotas"

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


def _month_sequence(n: int = 18, include_current: bool = False) -> list[str]:
    ref = date.today()
    year = ref.year
    month = ref.month if include_current else ref.month - 1
    if month == 0:
        month = 12
        year -= 1
    out = []
    for _ in range(n):
        out.append(f"{year}{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return out


def _ensure_structure(base_dir: Path) -> None:
    folders = [
        base_dir / "data" / "raw" / "cnes",
        base_dir / "data" / "raw" / "ibge_setores",
        base_dir / "data" / "raw" / "ibge_universo",
        base_dir / "data" / "raw" / "sim",
        base_dir / "data" / "raw" / "sinasc",
        base_dir / "data" / "raw" / "sinan",
        base_dir / "data" / "raw" / "esf",
        base_dir / "data" / "raw" / "censo_escolar",
        base_dir / "data" / "raw" / "pbf",
        base_dir / "data" / "processed",
        base_dir / "outputs" / "maps",
        base_dir / "outputs" / "tables",
        base_dir / "src",
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


def _extract_cnes_from_base_csv(base_csv: Path) -> list[dict]:
    """
    Extrai UBS do município alvo de um tbEstabelecimento do pacote BASE_DE_DADOS_CNES.
    """
    sep, encoding = _detect_csv_sep(base_csv)
    cols = [
        "CO_CNES",
        "NO_FANTASIA",
        "CO_MUNICIPIO_GESTOR",
        "TP_UNIDADE",
        "CO_TIPO_UNIDADE",
        "NU_LATITUDE",
        "NU_LONGITUDE",
    ]

    records: list[dict] = []
    for chunk in pd.read_csv(
        base_csv,
        sep=sep,
        dtype=str,
        encoding=encoding,
        low_memory=False,
        usecols=lambda c: c in cols,
        chunksize=200_000,
    ):
        mun = chunk.get("CO_MUNICIPIO_GESTOR", pd.Series(dtype=str)).astype(str).str.replace(r"\D", "", regex=True)
        tp = chunk.get("TP_UNIDADE", pd.Series(dtype=str)).fillna("")
        if "CO_TIPO_UNIDADE" in chunk.columns:
            tp = tp.where(tp.astype(str).str.strip() != "", chunk["CO_TIPO_UNIDADE"])
        tp = tp.astype(str).str.replace(r"\D", "", regex=True).str.zfill(2)
        mask = (mun.str.startswith(MUNICIPIO_IBGE) | mun.str.startswith(MUNICIPIO_IBGE_6)) & tp.isin({"01", "02"})
        filt = chunk.loc[mask].copy()
        if filt.empty:
            continue
        for _, row in filt.iterrows():
            records.append(
                {
                    "co_cnes": str(row.get("CO_CNES", "")).strip(),
                    "no_fantasia": str(row.get("NO_FANTASIA", "")).strip(),
                    "nu_latitude": str(row.get("NU_LATITUDE", "")).strip(),
                    "nu_longitude": str(row.get("NU_LONGITUDE", "")).strip(),
                    "co_municipio_gestor": str(row.get("CO_MUNICIPIO_GESTOR", "")).strip(),
                    "tp_unidade": str(tp.loc[row.name]).strip(),
                }
            )
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
    setor_col = next((c for c in cols if c.upper() in {"CD_SETOR", "CD_SETOR_CENSITARIO"}), None)
    if not mun_col and not setor_col:
        raise KeyError(f"CSV sem coluna de município ou setor: {in_csv.name}")

    parts: list[pd.DataFrame] = []
    for chunk in pd.read_csv(
        in_csv,
        sep=sep,
        dtype=str,
        encoding=encoding,
        low_memory=False,
        chunksize=200_000,
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
                setores_zip = setores_dir / f"{UF}_setores_CD2022.zip"
                setores_urls = [
                    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
                    "malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/"
                    f"censo_2022/setores/shp/UF/{UF}_setores_CD2022.zip",
                    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
                    "malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/"
                    f"censo_2022/setores_censitarios_shp/{UF}/SC_Intra_{UF}_2022.zip",
                ]
                used_url = _download_first_available(setores_urls, setores_zip)
                _unzip(setores_zip, setores_dir)
                shps = sorted(setores_dir.glob("**/*.shp"))
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
                # Nova estrutura IBGE (a partir de 2025): arquivos BR por tema
                new_base = (
                    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
                    "Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/"
                )
                # Zips de cada tema necessário (basico + domicilio são os mais usados)
                new_theme_zips = [
                    "Agregados_por_setores_basico_BR_20250417.zip",
                    "Agregados_por_setores_caracteristicas_domicilio1_BR.zip",
                    "Agregados_por_setores_caracteristicas_domicilio2_BR.zip",
                    "Agregados_por_setores_demografia_BR.zip",
                    "Agregados_por_setores_alfabetizacao_BR.zip",
                    "Agregados_por_setores_cor_ou_raca_BR.zip",
                    "Agregados_por_setores_cor_raca_BR.zip",
                ]
                downloaded_any = False
                for zip_name in new_theme_zips:
                    zip_dest = universo_dir / zip_name
                    try:
                        # Também tenta descobrir dinamicamente se o nome mudou
                        candidate_urls: list[str] = [new_base + zip_name]
                        for link in (_list_zip_links(new_base) if not downloaded_any else []):
                            lname = Path(urlparse(link).path).name.lower()
                            kw = zip_name.split("_")[3] if "_" in zip_name else ""
                            if kw and kw in lname:
                                candidate_urls.insert(0, link)
                        _download_first_available(candidate_urls, zip_dest, timeout=300)
                        _unzip(zip_dest, universo_dir)
                        downloaded_any = True
                    except Exception as e:  # noqa: BLE001
                        log.warning("  não foi possível baixar %s: %s", zip_name, e)

                if downloaded_any:
                    source_universe = new_base
                else:
                    # Fallback: estrutura antiga com zip estadual RS
                    old_base = (
                        "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
                        "Resultados_do_Universo/Agregados_por_Setores_Censitarios/"
                    )
                    univ_zip = universo_dir / f"{UF}_20231030.zip"
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
                    _unzip(univ_zip, universo_dir)

                csvs = sorted(universo_dir.glob("**/*.csv"))

            log.info("  IBGE universo: %d CSV(s) disponíveis", len(csvs))
            counts = []
            missing = []
            for _, (fname, out_csv, keywords) in expected.items():
                src = _pick_universe_source_csv(csvs, fname, keywords)
                if not src:
                    missing.append(fname)
                    continue
                n = _filter_csv_by_municipio(src, out_csv, MUNICIPIO_IBGE)
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


def step_esf(base_dir: Path) -> StatusRow:
    out_csv = base_dir / "data" / "raw" / "esf" / _city_file("cobertura_esf")
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    if out_csv.exists() and out_csv.stat().st_size > 0:
        n = max(sum(1 for _ in out_csv.open("r", encoding="utf-8")) - 1, 0)
        if n > 0:
            log.info("[ETAPA 4] concluída — ESF já existente (%d linhas)", n)
            return StatusRow("ESF", _rel(base_dir, out_csv), "OK", n, "arquivo existente")
        log.info("[ETAPA 4] concluída — ESF existente sem linhas (manual)")
        return StatusRow("ESF", _rel(base_dir, out_csv), "MANUAL", 0, "arquivo existente sem registros")

    s = _session()
    for comp in _month_sequence(18, include_current=False):
        url = (
            "https://egestorab.saude.gov.br/api/public/relatorios/profissionais/cobertura"
            f"?municipio={MUNICIPIO_IBGE}&periodo={comp}"
        )
        try:
            r = s.get(url, timeout=30)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                df = pd.json_normalize(data)
            elif isinstance(data, dict):
                arr = data.get("content") or data.get("items") or data.get("data") or []
                if isinstance(arr, list):
                    df = pd.json_normalize(arr)
                else:
                    df = pd.DataFrame([data])
            else:
                continue
            if df.empty:
                continue
            df["competencia"] = comp
            df.to_csv(out_csv, index=False)
            log.info("[ETAPA 4] concluída — %d linhas em %s", len(df), _rel(base_dir, out_csv))
            return StatusRow("ESF", _rel(base_dir, out_csv), "OK", int(len(df)), "")
        except Exception:  # noqa: BLE001
            continue

    pd.DataFrame(columns=["co_cnes", "nu_cnes", "nu_cobertura_esf"]).to_csv(out_csv, index=False)
    obs = (
        "download manual necessário em egestorab.saude.gov.br — "
        f"filtrar {CIDADE} ({MUNICIPIO_IBGE}) e exportar CSV"
    )
    log.info("[ETAPA 4] concluída — modo manual")
    return StatusRow("ESF", _rel(base_dir, out_csv), "MANUAL", 0, obs)


def step_sim(base_dir: Path) -> StatusRow:
    res = download_sim_municipio(
        base_dir=base_dir,
        years=(2021, 2022, 2023),
        municipio=MUNICIPIO_IBGE,
        uf=UF,
        file_prefix=f"obitos_{SLUG}",
    )
    ok = [r for r in res if r["status"] == "OK"]
    n_total = int(sum(r["n_registros"] for r in ok))
    if len(ok) == len(res):
        status = "OK"
        obs = "anos 2021-2023"
    elif ok:
        status = "PARCIAL"
        obs = "; ".join(sorted({r["obs"] for r in res if r["status"] != "OK"}))
    else:
        status = "MANUAL"
        obs = "; ".join(sorted({r["obs"] for r in res}))
    log.info("[ETAPA 5] SIM concluída — total %d registros (2021-2023)", n_total)
    return StatusRow("SIM", f"raw/sim/obitos_{SLUG}_YYYY.csv", status, n_total, obs)


def step_sinasc(base_dir: Path) -> StatusRow:
    res = download_sinasc_municipio(
        base_dir=base_dir,
        years=(2021, 2022, 2023),
        municipio=MUNICIPIO_IBGE,
        uf=UF,
        file_prefix=f"nascidos_{SLUG}",
    )
    ok = [r for r in res if r["status"] == "OK"]
    n_total = int(sum(r["n_registros"] for r in ok))
    if len(ok) == len(res):
        status = "OK"
        obs = "anos 2021-2023"
    elif ok:
        status = "PARCIAL"
        obs = "; ".join(sorted({r["obs"] for r in res if r["status"] != "OK"}))
    else:
        status = "MANUAL"
        obs = "; ".join(sorted({r["obs"] for r in res}))
    log.info("[ETAPA 5] SINASC concluída — total %d registros (2021-2023)", n_total)
    return StatusRow("SINASC", f"raw/sinasc/nascidos_{SLUG}_YYYY.csv", status, n_total, obs)


def step_sinan(base_dir: Path) -> StatusRow:
    out_csv = base_dir / "data" / "raw" / "sinan" / _city_file("sifilis")
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # Pula apenas se arquivo já tem dados
    if out_csv.exists() and out_csv.stat().st_size > 0:
        n = max(sum(1 for _ in out_csv.open("r", encoding="utf-8")) - 1, 0)
        if n > 0:
            log.info("[ETAPA 6] concluída — SINAN já existente (%d linhas)", n)
            return StatusRow("SINAN", _rel(base_dir, out_csv), "OK", n, "arquivo existente")

    dfs: list[pd.DataFrame] = []
    obs_parts: list[str] = []
    try:
        from pysus import SINAN as _SINAN  # type: ignore  # nova API pysus ≥1.0

        sinan_db = _SINAN()
        sinan_db.load()
        for disease in ("SIFC", "SIFG", "SIFA"):
            for year in (2022, 2021, 2023):
                try:
                    files = sinan_db.get_files(disease, year=year)
                    if not files:
                        continue
                    data = files[0].download(local_dir=str(out_csv.parent))
                    df_raw = data.to_dataframe()
                    col = next((c for c in ("ID_MN_RESI", "id_mn_resi", "MUNICIPIO") if c in df_raw.columns), None)
                    if not col:
                        continue
                    vals = df_raw[col].astype(str).str.replace(r"\D", "", regex=True)
                    filt = df_raw[vals.str.startswith(MUNICIPIO_IBGE) | vals.str.startswith(MUNICIPIO_IBGE_6)].copy()
                    filt["agravo"] = disease
                    filt["ano"] = year
                    if not filt.empty:
                        dfs.append(filt)
                        obs_parts.append(f"{disease}/{year}:{len(filt)}")
                    break  # ano encontrado; passa para próximo agravo
                except Exception as e:  # noqa: BLE001
                    log.debug("SINAN %s/%s falhou: %s", disease, year, e)
    except Exception as e:  # noqa: BLE001
        log.warning("[ETAPA 6] pysus SINAN falhou: %s", e)

    if dfs:
        result = pd.concat(dfs, ignore_index=True)
        result.to_csv(out_csv, index=False)
        obs = "; ".join(obs_parts)
        log.info("[ETAPA 6] concluída — %d registros SINAN em %s", len(result), _rel(base_dir, out_csv))
        return StatusRow("SINAN", _rel(base_dir, out_csv), "OK", int(len(result)), obs)

    pd.DataFrame(columns=["DT_NOTIFIC", "ID_MN_RESI", "CS_SEXO", "NU_CEP"]).to_csv(out_csv, index=False)
    log.info("[ETAPA 6] concluída — modo manual")
    return StatusRow("SINAN", _rel(base_dir, out_csv), "MANUAL", 0, "download automático falhou")


def _resolve_censo_url(session: requests.Session) -> tuple[str, int] | None:
    current = date.today().year - 1
    candidates: list[tuple[str, int]] = []
    for year in range(current, current - 6, -1):
        # Nova URL (a partir de ~2024): dados_abertos raiz
        candidates.append((
            f"https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{year}_.zip",
            year,
        ))
        candidates.append((
            f"https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{year}.zip",
            year,
        ))
        # URL antiga (subdirectório por ano)
        candidates.append((
            "https://download.inep.gov.br/dados_abertos/microdados/"
            f"censo_escolar/{year}/microdados_educacao_basica_{year}.zip",
            year,
        ))
        candidates.append((
            "https://download.inep.gov.br/dados_abertos/microdados/"
            f"censo_escolar/{year}/microdados_censo_escolar_{year}.zip",
            year,
        ))
    for url, year in candidates:
        try:
            r = session.head(url, timeout=20, allow_redirects=True)
            if r.status_code < 400:
                return url, year
        except Exception:  # noqa: BLE001
            continue
    return None


def step_censo_escolar(base_dir: Path, skip_large: bool = False) -> StatusRow:
    out_csv = base_dir / "data" / "raw" / "censo_escolar" / _city_file("matriculas")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if out_csv.exists() and out_csv.stat().st_size > 0:
        df = pd.read_csv(out_csv, dtype=str)
        if df.empty:
            log.info("[ETAPA 7] concluída — arquivo existente sem matrículas (manual)")
            return StatusRow("Censo Escolar", _rel(base_dir, out_csv), "MANUAL", 0, "arquivo existente sem registros")
        anos_col = next((c for c in ("NU_ANO_CENSO", "ANO", "CO_ANO") if c in df.columns), None)
        anos = sorted(df[anos_col].dropna().astype(str).unique().tolist()) if anos_col else []
        obs = f"anos={','.join(anos)}" if anos else "arquivo existente"
        log.info("[ETAPA 7] concluída — %d matrículas em %s", len(df), _rel(base_dir, out_csv))
        return StatusRow("Censo Escolar", _rel(base_dir, out_csv), "OK", int(len(df)), obs)

    if skip_large:
        pd.DataFrame(columns=["CO_MUNICIPIO"]).to_csv(out_csv, index=False)
        return StatusRow("Censo Escolar", _rel(base_dir, out_csv), "MANUAL", 0, "download grande pulado (--skip-large)")

    s = _session()
    resolved = _resolve_censo_url(s)
    if not resolved:
        pd.DataFrame(columns=["CO_MUNICIPIO"]).to_csv(out_csv, index=False)
        return StatusRow("Censo Escolar", _rel(base_dir, out_csv), "MANUAL", 0, "arquivo mais recente não localizado")

    url, year = resolved
    zip_path = out_csv.parent / f"microdados_censo_escolar_{year}.zip"
    try:
        _download_with_tqdm(url, zip_path, session=s, timeout=600)
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            csvs_upper = {n.upper(): n for n in names if n.upper().endswith(".CSV")}

            # Formato novo (≥2025): Tabela_Matricula_{year}.csv + Tabela_Escola_{year}.csv
            mat_new = next((v for k, v in csvs_upper.items() if "TABELA_MATRICULA" in k), None)
            esc_new = next((v for k, v in csvs_upper.items() if "TABELA_ESCOLA" in k), None)

            if mat_new and esc_new:
                with zf.open(esc_new) as f:
                    escola_df = pd.read_csv(f, sep=";", encoding="latin1", low_memory=False,
                                            usecols=["CO_ENTIDADE", "CO_MUNICIPIO"])
                municipio_entidades = set(
                    escola_df[escola_df["CO_MUNICIPIO"].astype(str).str.replace(r"\D", "", regex=True)
                               .str.startswith(MUNICIPIO_IBGE_6)]["CO_ENTIDADE"].astype(str)
                )
                with zf.open(mat_new) as f:
                    mat_df = pd.read_csv(f, sep=";", encoding="latin1", low_memory=False, dtype=str)
                mat_df["CO_MUNICIPIO"] = mat_df["CO_ENTIDADE"].map(
                    escola_df.set_index("CO_ENTIDADE")["CO_MUNICIPIO"].astype(str)
                )
                filt = mat_df[mat_df["CO_ENTIDADE"].isin(municipio_entidades)].copy()
            else:
                # Formato antigo: MATRICULA_UF.CSV com CO_MUNICIPIO direto
                uf_csvs = [v for k, v in csvs_upper.items() if "MATRICULA" in k and UF in k]
                if not uf_csvs:
                    uf_csvs = [v for k, v in csvs_upper.items() if "MATRICULA" in k]
                if not uf_csvs:
                    raise FileNotFoundError("arquivo matrícula não encontrado no ZIP")
                member = uf_csvs[0]
                extracted = out_csv.parent / Path(member).name
                with zf.open(member) as src, open(extracted, "wb") as dst:
                    dst.write(src.read())
                sep, encoding = _detect_csv_sep(extracted)
                df = pd.read_csv(extracted, sep=sep, dtype=str, encoding=encoding, low_memory=False)
                if "CO_MUNICIPIO" not in df.columns:
                    raise KeyError("CO_MUNICIPIO não encontrado em matrículas")
                vals = df["CO_MUNICIPIO"].astype(str).str.replace(r"\D", "", regex=True)
                filt = df[vals.str.startswith(MUNICIPIO_IBGE) | vals.str.startswith(MUNICIPIO_IBGE_6)].copy()

        filt.to_csv(out_csv, index=False)
        anos_col = next((c for c in ("NU_ANO_CENSO", "ANO", "CO_ANO") if c in filt.columns), None)
        anos = sorted(filt[anos_col].dropna().astype(str).unique().tolist()) if anos_col else []
        obs = f"ano_fonte={year}; anos_presentes={','.join(anos)}" if anos else f"ano_fonte={year}"
        log.info("[ETAPA 7] concluída — %d escolas em %s", len(filt), _rel(base_dir, out_csv))
        return StatusRow("Censo Escolar", _rel(base_dir, out_csv), "OK", int(len(filt)), obs)
    except Exception as e:  # noqa: BLE001
        pd.DataFrame(columns=["CO_MUNICIPIO"]).to_csv(out_csv, index=False)
        return StatusRow("Censo Escolar", _rel(base_dir, out_csv), "MANUAL", 0, f"download/processamento falhou ({e})")


def step_pbf(base_dir: Path) -> StatusRow:
    out_csv = base_dir / "data" / "raw" / "pbf" / _city_file("pbf_202312")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    s = _session()

    api_key = os.environ.get("TRANSPARENCIA_API_KEY", "SEM_CHAVE")
    headers = {"chave-api-dados": api_key}
    # Endpoint correto: novo-bolsa-familia-por-municipio (agrega por mês/município)
    # Coleta múltiplos meses para ter série histórica
    meses = ["202312", "202401", "202406", "202412"]
    rows: list[dict] = []
    api_err = ""
    for mes in meses:
        for page in range(1, 200):
            url = (
                "https://api.portaldatransparencia.gov.br/api-de-dados/"
                f"novo-bolsa-familia-por-municipio?mesAno={mes}&codigoIbge={MUNICIPIO_IBGE}&pagina={page}"
            )
            try:
                r = s.get(url, timeout=30, headers=headers)
                if r.status_code == 401:
                    api_err = "API retornou 401 (chave necessária)"
                    break
                r.raise_for_status()
                data = r.json()
                if not data:
                    break
                rows.extend(data if isinstance(data, list) else [data])
            except Exception as e:  # noqa: BLE001
                api_err = str(e)
                break
    if rows:
        df = pd.json_normalize(rows)
        df.to_csv(out_csv, index=False)
        n_benef = int(df["quantidadeBeneficiados"].sum()) if "quantidadeBeneficiados" in df.columns else len(df)
        log.info("[ETAPA 8] concluída — %d registros (%d beneficiários) em %s", len(df), n_benef, _rel(base_dir, out_csv))
        return StatusRow("PBF", _rel(base_dir, out_csv), "OK", n_benef, f"via API; {len(df)} meses")

    fallback_url = "https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/202312"
    tmp_file = out_csv.parent / "bolsa_familia_202312_download"
    try:
        _download_with_tqdm(fallback_url, tmp_file, session=s, timeout=180)
        # O portal pode devolver HTML ou ZIP. Se não for parseável, cai para manual.
        if zipfile.is_zipfile(tmp_file):
            with zipfile.ZipFile(tmp_file, "r") as zf:
                csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_members:
                    raise FileNotFoundError("ZIP sem CSV")
                member = csv_members[0]
                extracted = out_csv.parent / Path(member).name
                with zf.open(member) as src, open(extracted, "wb") as dst:
                    dst.write(src.read())
            sep, enc = _detect_csv_sep(extracted)
            df = pd.read_csv(extracted, sep=sep, dtype=str, encoding=enc, low_memory=False)
        else:
            sep, enc = _detect_csv_sep(tmp_file)
            df = pd.read_csv(tmp_file, sep=sep, dtype=str, encoding=enc, low_memory=False)

        col_mun = next((c for c in df.columns if "MUNICIP" in c.upper()), None)
        col_ibge = next((c for c in df.columns if "IBGE" in c.upper()), None)
        if col_ibge:
            vals = _clean_digits(df[col_ibge])
            filt = df[vals.str.startswith(MUNICIPIO_IBGE) | vals.str.startswith(MUNICIPIO_IBGE_6)].copy()
        elif col_mun:
            vals = df[col_mun].astype(str).str.upper()
            filt = df[vals.str.contains(CIDADE.upper(), na=False)].copy()
        else:
            raise KeyError("coluna de município/IBGE não encontrada")

        if filt.empty:
            raise ValueError(f"fallback CSV sem linhas de {CIDADE}")
        filt.to_csv(out_csv, index=False)
        log.info("[ETAPA 8] concluída — %d linhas em %s", len(filt), _rel(base_dir, out_csv))
        return StatusRow("PBF", _rel(base_dir, out_csv), "OK", int(len(filt)), "via CSV mensal")
    except Exception as e:  # noqa: BLE001
        pd.DataFrame().to_csv(out_csv, index=False)
        obs = (
            "PBF: necessário cadastro em portaldatransparencia.gov.br/api-de-dados "
            f"para obter chave gratuita; erro atual: {api_err or e}"
        )
        log.info("[ETAPA 8] concluída — modo manual")
        return StatusRow("PBF", _rel(base_dir, out_csv), "MANUAL", 0, obs)


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
        "esf": lambda: [step_esf(base_dir)],
        "sim": lambda: [step_sim(base_dir)],
        "sinasc": lambda: [step_sinasc(base_dir)],
        "sinan": lambda: [step_sinan(base_dir)],
        "censo_escolar": lambda: [step_censo_escolar(base_dir, skip_large=skip_large)],
        "pbf": lambda: [step_pbf(base_dir)],
    }

    run_order = ["cnes", "voronoi", "ibge", "esf", "sim", "sinasc", "sinan", "censo_escolar", "pbf"]
    if only:
        run_order = [only]

    for key in run_order:
        try:
            status_rows.extend(steps[key]())
        except Exception as e:  # noqa: BLE001
            log.exception("Falha inesperada em %s", key)
            status_rows.append(StatusRow(key.upper(), "-", "ERRO", 0, str(e)))

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
        choices=["cnes", "voronoi", "ibge", "esf", "sim", "sinasc", "sinan", "censo_escolar", "pbf"],
        default=None,
        help="Executa apenas uma etapa/fonte",
    )
    parser.add_argument("--skip-large", action="store_true", help="Pula arquivos grandes (IBGE/Censo Escolar)")
    args = parser.parse_args()

    _configure_runtime(args.municipio_ibge, args.uf, args.cidade, args.slug)
    base_dir = (
        Path(args.base_dir).resolve()
        if args.base_dir
        else (Path(__file__).resolve().parents[1] / f"ivs_{SLUG}").resolve()
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


if __name__ == "__main__":
    main()
