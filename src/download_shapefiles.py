"""
download_shapefiles.py
======================
Baixa automaticamente os dados geoespaciais e tabulares necessários para
o cálculo do IVSaúde das 141 UBS de Porto Alegre.

Fontes cobertas:
  1. Setores censitários IBGE 2022 — RS (shapefile + resultados do Universo)
  2. CNES — localização georreferenciada das UBS de Porto Alegre
  3. GeoSampa / dados abertos PMPA — territórios das UBS (shapefile)
  4. e-Gestor APS — cobertura ESF por equipe/município (API pública)
  5. DataSUS FTP — SIM, SINASC, SINAN (arquivos DBC compactados)
  6. INEP — Censo Escolar (microdados de matrícula)
  7. MDS/e-Gestor — beneficiários do Bolsa Família por município/setor

Uso:
    python src/download_shapefiles.py [--skip-large] [--only FONTE]

Flags:
    --skip-large  Pula arquivos > 500 MB (útil em testes)
    --only FONTE  Baixa apenas a fonte especificada (ibge|cnes|ubs|esf|sim|
                  sinasc|sinan|censo_escolar|pbf)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

import requests

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
COD_MUNICIPIO_IBGE = "4314902"   # Porto Alegre
COD_UF = "RS"
UF_IBGE_NUM = "43"               # código numérico do RS no IBGE
CNES_TP_UNIDADE_UBS = "02"       # Unidade Básica/Centro de Saúde (schema XML CNES)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Utilidades de download
# ---------------------------------------------------------------------------

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "IVSaude-POA/1.0 (saude@prefpoa.com.br)"})
    return s


def download(url: str, dest: Path, session: Optional[requests.Session] = None,
             skip_if_exists: bool = True, chunk_size: int = 1 << 20,
             timeout: int = 120) -> Path:
    """Baixa `url` para `dest` com barra de progresso simples."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if skip_if_exists and dest.exists() and dest.stat().st_size > 0:
        log.info("  já existe: %s", dest.name)
        return dest

    s = session or _session()
    log.info("  baixando: %s", url)
    with s.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(f"\r    {pct:5.1f}%  ({downloaded/1e6:.1f} MB)", end="", flush=True)
        if total:
            print()
    log.info("  salvo: %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
    return dest


def _http_exists(url: str, session: Optional[requests.Session] = None, timeout: int = 30) -> bool:
    """Retorna True se a URL responder com status HTTP < 400."""
    s = session or _session()
    try:
        r = s.head(url, timeout=timeout, allow_redirects=True)
        if r.status_code < 400:
            return True
        # Alguns servidores não suportam HEAD corretamente.
        if r.status_code in (403, 405):
            with s.get(url, timeout=timeout, stream=True, allow_redirects=True) as g:
                return g.status_code < 400
        return False
    except requests.RequestException:
        return False


def _list_zip_links(dir_url: str, session: Optional[requests.Session] = None,
                    timeout: int = 30) -> list[str]:
    """Lista links .zip em um índice HTTP de diretório."""
    s = session or _session()
    try:
        r = s.get(dir_url, timeout=timeout)
        r.raise_for_status()
    except requests.RequestException:
        return []

    links = re.findall(r"""href=["']?([^"'>\s]+\.zip)""", r.text, flags=re.IGNORECASE)
    out: list[str] = []
    seen: set[str] = set()
    for link in links:
        abs_url = urljoin(dir_url, link)
        if abs_url not in seen:
            seen.add(abs_url)
            out.append(abs_url)
    return out


def _month_sequence(n: int = 12, include_current: bool = False) -> list[str]:
    """Retorna competências AAAAMM, da mais recente para a mais antiga."""
    ref = date.today()
    year = ref.year
    month = ref.month if include_current else ref.month - 1
    if month == 0:
        month = 12
        year -= 1

    comps: list[str] = []
    for _ in range(n):
        comps.append(f"{year}{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return comps


def _first_non_empty(props: dict, keys: list[str]) -> str:
    for k in keys:
        v = props.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return ""


def _load_cnes_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            return set()
        ids = {
            _first_non_empty(r if isinstance(r, dict) else {}, ["co_cnes", "CO_CNES", "nu_cnes", "NU_CNES"])
            for r in raw
        }
        return {i for i in ids if i}
    except Exception:  # noqa: BLE001
        return set()


def _normalize_ubs_geojson(src: Path, dest: Path) -> tuple[int, set[str]]:
    """
    Normaliza campos de identificação da UBS em GeoJSON:
      - co_cnes
      - no_fantasia
    """
    obj = json.loads(src.read_text(encoding="utf-8"))
    features = obj.get("features", []) if isinstance(obj, dict) else []
    if not isinstance(features, list):
        raise ValueError("GeoJSON inválido: 'features' ausente")

    cnes_ids: set[str] = set()
    for feat in features:
        if not isinstance(feat, dict):
            continue
        props = feat.get("properties")
        if not isinstance(props, dict):
            props = {}
            feat["properties"] = props
        co_cnes = _first_non_empty(props, ["co_cnes", "CO_CNES", "cnes", "CNES", "nu_cnes", "NU_CNES"])
        no_fantasia = _first_non_empty(
            props,
            ["no_fantasia", "NO_FANTASIA", "nome", "NOME", "UBS", "unidade", "NO_UNIDADE"],
        )
        if co_cnes:
            props["co_cnes"] = co_cnes
            cnes_ids.add(co_cnes)
        if no_fantasia:
            props["no_fantasia"] = no_fantasia

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    return len(features), cnes_ids


def unzip(archive: Path, dest_dir: Optional[Path] = None) -> Path:
    """Descompacta um ZIP no diretório indicado (padrão: mesmo diretório)."""
    dest_dir = dest_dir or archive.parent
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, "r") as zf:
        zf.extractall(dest_dir)
    log.info("  extraído: %s -> %s", archive.name, dest_dir)
    return dest_dir


# ---------------------------------------------------------------------------
# 1. IBGE — setores censitários 2022
# ---------------------------------------------------------------------------

def download_ibge_setores(skip_large: bool = False) -> None:
    """
    Shapefile dos setores censitários do RS (Censo 2022).

    URL oficial:
    https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/
    malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022/
    setores_censitarios_shp/
    """
    log.info("=== IBGE — Setores Censitários 2022 (%s) ===", COD_UF)

    s = _session()
    base_dirs = [
        "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
        "malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/"
        "censo_2022/setores_censitarios_shp/",
        "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
        "malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/"
        "censo_2022/setores_censitarios/",
    ]
    filename_candidates = [
        f"SC_Intra_{COD_UF}_2022.zip",
        f"SC_2022_{COD_UF}.zip",
        f"SC_{COD_UF}_2022.zip",
    ]

    candidate_urls: list[str] = []
    for base in base_dirs:
        uf_dir = urljoin(base, f"{COD_UF}/")
        for filename in filename_candidates:
            candidate_urls.append(urljoin(uf_dir, filename))
        # Tenta descobrir nome real do ZIP no índice do diretório.
        for link in _list_zip_links(uf_dir, session=s):
            link_up = link.upper()
            if COD_UF in link_up and "2022" in link_up:
                candidate_urls.insert(0, link)

    url = next((u for u in candidate_urls if _http_exists(u, session=s)), None)
    if not url:
        manual_url = candidate_urls[0] if candidate_urls else ""
        _write_stub(
            RAW / "ibge_setores" / "DOWNLOAD_MANUAL.txt",
            f"Baixar manualmente:\n{manual_url}\n\n"
            "Se o arquivo mudou de nome, abra o diretório do estado no GeoFTP IBGE.\n"
            "Extrair em: data/raw/ibge_setores/\n",
        )
        log.warning("  não foi possível resolver URL automática dos setores IBGE")
        return

    filename = Path(urlparse(url).path).name
    dest = RAW / "ibge_setores" / filename

    if skip_large:
        log.warning("  --skip-large ativo; ignorando setores IBGE (arquivo ~200 MB)")
        _write_stub(dest.parent / "DOWNLOAD_MANUAL.txt",
                    f"Baixar manualmente:\n{url}\n\n"
                    "Extrair em: data/raw/ibge_setores/\n")
        return

    archive = download(url, dest, session=s)
    unzip(archive, dest.parent)

    # Tabelas de resultados do Universo (Censo 2022) — basico_RS
    log.info("=== IBGE — Resultado do Universo (basico_RS) ===")
    tabelas_base = (
        "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
        "Resultados_do_Universo/Agregados_por_Setores_Censitarios/"
    )
    # Arquivo compactado por UF (nome pode mudar conforme atualização do IBGE)
    universe_candidates = [tabelas_base + "RS_20231030.zip"]
    for link in _list_zip_links(tabelas_base, session=s):
        name = Path(urlparse(link).path).name.upper()
        if name.startswith("RS_") and name.endswith(".ZIP"):
            universe_candidates.insert(0, link)

    tab_url = next((u for u in universe_candidates if _http_exists(u, session=s)), None)
    if skip_large:
        manual_url = tab_url or universe_candidates[0]
        _write_stub(RAW / "ibge_universo" / "DOWNLOAD_MANUAL.txt",
                    f"Baixar manualmente:\n{manual_url}\n\nExtrair em: data/raw/ibge_universo/\n")
        return

    if not tab_url:
        log.warning("  não foi possível resolver URL do Universo IBGE automaticamente")
        _write_stub(
            RAW / "ibge_universo" / "DOWNLOAD_MANUAL.txt",
            "Baixar manualmente no diretório oficial de agregados por setores censitários do IBGE.\n"
            "Extrair em: data/raw/ibge_universo/\n",
        )
        return

    tab_file = Path(urlparse(tab_url).path).name
    tab_dest = RAW / "ibge_universo" / tab_file
    archive2 = download(tab_url, tab_dest, session=s)
    unzip(archive2, tab_dest.parent)


# ---------------------------------------------------------------------------
# 2. CNES — georreferenciamento das UBS
# ---------------------------------------------------------------------------

def _xml_localname(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _extract_cnes_from_xml(xml_path: Path, dest: Path,
                           municipio_ibge: str = COD_MUNICIPIO_IBGE,
                           tp_unidade: str = CNES_TP_UNIDADE_UBS) -> int:
    """
    Extrai registros de UBS de um XML CNES grande em modo streaming (iterparse).
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    registros: list[dict[str, str]] = []
    total_lidos = 0
    total_filtrados = 0
    municipio_alvo_6 = municipio_ibge[:6]

    for _, elem in ET.iterparse(xml_path, events=("start",)):
        if _xml_localname(elem.tag).upper() != "ROW":
            continue
        total_lidos += 1
        attrs = dict(elem.attrib)
        municipio = (attrs.get("CO_MUNICIPIO_GESTOR")
                     or attrs.get("CO_MUNICIPIO")
                     or "")
        tipo = (attrs.get("TP_UNIDADE")
                or attrs.get("CO_TIPO_UNIDADE")
                or "")
        if municipio not in (municipio_ibge, municipio_alvo_6):
            continue
        if tp_unidade and tipo and tipo.zfill(2) != tp_unidade.zfill(2):
            continue

        # Preserva todos os campos em minúsculas e normaliza os principais.
        rec = {k.lower(): v for k, v in attrs.items()}
        rec["co_cnes"] = attrs.get("CO_CNES", rec.get("co_cnes", ""))
        rec["nu_cnes"] = rec["co_cnes"]
        rec["no_fantasia"] = attrs.get("NO_FANTASIA", rec.get("no_fantasia", ""))
        rec["co_municipio_gestor"] = municipio
        rec["tp_unidade"] = tipo

        lat_key = next((k for k in attrs.keys() if "LAT" in k.upper()), None)
        lon_key = next((k for k in attrs.keys() if "LON" in k.upper() or "LNG" in k.upper()), None)
        if lat_key and "nu_latitude" not in rec:
            rec["nu_latitude"] = attrs.get(lat_key, "")
        if lon_key and "nu_longitude" not in rec:
            rec["nu_longitude"] = attrs.get(lon_key, "")

        registros.append(rec)
        total_filtrados += 1
        elem.clear()

    log.info("  XML CNES lido: %d registros, %d filtrados (POA/UBS)", total_lidos, total_filtrados)
    if total_filtrados == 0:
        return 0
    dest.write_text(json.dumps(registros, ensure_ascii=False), encoding="utf-8")
    log.info("  salvo: %s", dest.name)
    return total_filtrados


def download_cnes_ubs() -> None:
    """
    Usa a API REST pública do DataSUS/CNES para buscar todas as UBS
    (tipoUnidade = 1) de Porto Alegre e salva em JSON.

    Endpoint documentado em:
    https://cnes.datasus.gov.br/pages/downloads/documentacaoAPI.jsp
    """
    log.info("=== CNES — UBS de Porto Alegre ===")
    url = (
        "https://cnes.datasus.gov.br/services/estabelecimentos-lite"
        f"?municipio={COD_MUNICIPIO_IBGE}&tipoUnidade=1&size=500&page=0"
    )
    dest = RAW / "cnes" / "ubs_poa.json"
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and dest.stat().st_size > 0:
        log.info("  já existe: %s", dest.name)
        return

    s = _session()
    for tentativa in range(1, 4):
        try:
            r = s.get(url, timeout=30)
            r.raise_for_status()
            dest.write_bytes(r.content)
            log.info("  salvo: %s", dest.name)
            return
        except requests.RequestException as e:
            log.warning("  falha CNES API (tentativa %d/3): %s", tentativa, e)
            if tentativa < 3:
                time.sleep(2 * tentativa)

    # Fallback local: XML CNES completo (streaming para suportar arquivo grande em 1 linha).
    xml_candidates = [
        Path(os.environ.get("CNES_XML_PATH", "")) if os.environ.get("CNES_XML_PATH") else None,
        Path(__file__).resolve().parents[1] / "data" / "CNESBRASIL" / "xmlCNES.xml",
        Path("data/CNESBRASIL/xmlCNES.xml"),
    ]
    seen_xml: set[Path] = set()
    for xml_path in xml_candidates:
        if not xml_path or not xml_path.exists():
            continue
        xml_real = xml_path.resolve()
        if xml_real in seen_xml:
            continue
        seen_xml.add(xml_real)
        try:
            log.info("  API indisponível; usando XML local: %s", xml_real)
            n = _extract_cnes_from_xml(xml_real, dest)
            if n > 0:
                return
            log.warning("  XML local encontrado, mas sem registros para POA/UBS")
        except Exception as e:  # noqa: BLE001
            log.warning("  falha ao processar XML CNES local (%s): %s", xml_real, e)

    _write_stub(
        dest.parent / "DOWNLOAD_MANUAL.txt",
        "Acesse: https://cnes.datasus.gov.br\n"
        "Menu: Downloads > Estabelecimentos\n"
        f"Filtrar: Município={COD_MUNICIPIO_IBGE}, Tipo=UBS\n"
        "Salvar como: data/raw/cnes/ubs_poa.csv\n",
    )


# ---------------------------------------------------------------------------
# 3. GeoSampa — Territórios das UBS (shapefile)
# ---------------------------------------------------------------------------

def download_ubs_shapefile() -> None:
    """
    Tenta baixar o shapefile de territórios das UBS da API WFS do GeoSampa.

    Layer: 'ugeo:ds_ubs_areas' (áreas de abrangência de UBS — SMS POA).
    Se falhar, gera instruções manuais e um script alternativo com Voronoi.

    GeoSampa WFS:
    https://geosampa.prefpoa.com.br/geoserver/ugeo/wfs
    """
    log.info("=== GeoSampa — Territórios das 141 UBS ===")
    dest_dir = RAW / "ubs_territorios"
    dest_dir.mkdir(parents=True, exist_ok=True)
    geojson_dest = dest_dir / "territorios_ubs.geojson"

    if geojson_dest.exists() and geojson_dest.stat().st_size > 0:
        log.info("  já existe: %s", geojson_dest.name)
        return

    cnes_ids = _load_cnes_ids(RAW / "cnes" / "ubs_poa.json")

    # Tentativa 1: WFS GeoSampa
    layers_to_try = [
        ("ugeo:ds_ubs_areas",       "areas de abrangencia UBS"),
        ("ugeo:ubs",                "pontos UBS"),
        ("smams:areas_risco",       "areas de risco ambiental"),
    ]

    s = _session()
    for layer, desc in layers_to_try:
        url = (
            "https://geosampa.prefpoa.com.br/geoserver/ugeo/wfs"
            f"?service=WFS&version=2.0.0&request=GetFeature"
            f"&typeName={layer}&outputFormat=application/json"
            f"&srsName=EPSG:4674"
        )
        out = dest_dir / f"{layer.replace(':', '_')}.geojson"
        try:
            log.info("  WFS layer: %s (%s)", layer, desc)
            r = s.get(url, timeout=60)
            r.raise_for_status()
            out.write_bytes(r.content)
            log.info("  salvo: %s (%.1f MB)", out.name, out.stat().st_size / 1e6)
            if layer == "ugeo:ds_ubs_areas":
                try:
                    n_feat, area_cnes_ids = _normalize_ubs_geojson(out, geojson_dest)
                    if cnes_ids and area_cnes_ids:
                        inter = len(cnes_ids.intersection(area_cnes_ids))
                        log.info("  territórios normalizados: %d feições, %d CNES em comum com CNES local", n_feat, inter)
                    else:
                        log.info("  territórios normalizados: %d feições", n_feat)
                    return
                except Exception as e:  # noqa: BLE001
                    log.warning("  falha ao normalizar camada principal de territórios: %s", e)
        except requests.RequestException as e:
            log.warning("  falha WFS %s: %s", layer, e)

    # Tentativa 2: Portal de dados abertos PMPA
    dados_abertos_urls = [
        (
            "https://dadosabertos.poa.br/dataset/unidades-basicas-saude/resource/"
            "e1e5e9e2-7e3b-4b9e-9c7b-1e0b0e1e9e2e",
            dest_dir / "ubs_dadosabertos.geojson",
        ),
    ]
    for url, out in dados_abertos_urls:
        try:
            r = s.get(url, timeout=30)
            r.raise_for_status()
            out.write_bytes(r.content)
            log.info("  dados abertos salvo: %s", out.name)
            try:
                n_feat, area_cnes_ids = _normalize_ubs_geojson(out, geojson_dest)
                if cnes_ids and area_cnes_ids:
                    inter = len(cnes_ids.intersection(area_cnes_ids))
                    log.info("  territórios normalizados (dados abertos): %d feições, %d CNES em comum", n_feat, inter)
                else:
                    log.info("  territórios normalizados (dados abertos): %d feições", n_feat)
                return
            except Exception as e:  # noqa: BLE001
                log.warning("  falha ao normalizar GeoJSON de dados abertos: %s", e)
        except requests.RequestException:
            pass  # silencioso — vai gerar manual abaixo

    # Tentativa 3: fallback local (arquivo já baixado manualmente no repositório).
    local_candidates = [
        Path(os.environ.get("UBS_AREAS_LOCAL_PATH", "")) if os.environ.get("UBS_AREAS_LOCAL_PATH") else None,
        Path(__file__).resolve().parents[1] / "data" / "AreaAbrangenciaUBS.geojson",
        Path("data/AreaAbrangenciaUBS.geojson"),
    ]
    force_local = os.environ.get("UBS_AREAS_FORCE", "").strip() in {"1", "true", "TRUE", "yes", "YES"}
    seen_local: set[Path] = set()
    for p in local_candidates:
        if not p or not p.exists():
            continue
        real = p.resolve()
        if real in seen_local:
            continue
        seen_local.add(real)
        try:
            temp_dest = dest_dir / "_tmp_territorios_ubs_local.geojson"
            n_feat, area_cnes_ids = _normalize_ubs_geojson(real, temp_dest)
            if cnes_ids and area_cnes_ids:
                inter = len(cnes_ids.intersection(area_cnes_ids))
                if inter == 0:
                    if force_local:
                        log.warning("  fallback local forçado com 0 CNES em comum: %s", real)
                    else:
                        if temp_dest.exists():
                            temp_dest.unlink()
                        log.warning("  fallback local ignorado (%s): 0 CNES em comum com o cadastro CNES de referência", real)
                        continue
                log.info("  fallback local aplicado: %s (%d feições, %d CNES em comum)", real, n_feat, inter)
            else:
                log.info("  fallback local aplicado: %s (%d feições)", real, n_feat)
            temp_dest.replace(geojson_dest)
            return
        except Exception as e:  # noqa: BLE001
            log.warning("  falha no fallback local de territórios (%s): %s", real, e)

    # Sempre gerar instruções manuais de fallback
    _write_stub(
        dest_dir / "INSTRUCOES_MANUAIS.txt",
        """Opção A — GeoSampa (navegador):
  1. Acesse https://geosampa.prefpoa.com.br
  2. Busque 'UBS' ou 'Unidades Básicas de Saúde'
  3. Exporte a camada de áreas de abrangência como GeoJSON/Shapefile
  4. Salve em: data/raw/ubs_territorios/territorios_ubs.geojson

Opção B — DATASUS / e-SUS APS:
  https://egestorab.saude.gov.br/paginas/acesso/login.xhtml
  Menu: Relatórios > Rede de Saúde > Territórios

Opção C — Voronoi pelo CNES (automático):
  Execute: python src/gerar_voronoi_ubs.py
  Isso gera territórios aproximados a partir dos pontos geocodificados do CNES.

Formato esperado:
  Arquivo GeoJSON com CRS EPSG:4674 (SIRGAS 2000)
  Campo obrigatório: 'co_cnes' (código CNES da UBS) ou 'no_fantasia'
""",
    )


# ---------------------------------------------------------------------------
# 4. e-Gestor APS — Cobertura ESF
# ---------------------------------------------------------------------------

def download_esf_cobertura() -> None:
    """
    Baixa relatório de cobertura da Estratégia Saúde da Família por equipe
    via API pública do e-Gestor APS.

    Endpoint: https://egestorab.saude.gov.br/api/public/
    """
    log.info("=== e-Gestor APS — Cobertura ESF ===")
    dest = RAW / "esf" / "cobertura_esf_poa.json"
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and dest.stat().st_size > 0:
        log.info("  já existe: %s", dest.name)
        return

    # Competência mais recente disponível nem sempre é o mês corrente.
    competencias = _month_sequence(n=18, include_current=False)
    s = _session()
    out = RAW / "esf" / "cobertura_esf_poa.json"

    for competencia in competencias:
        api_url = (
            "https://egestorab.saude.gov.br/api/public/relatorios/profissionais/cobertura"
            f"?municipio={COD_MUNICIPIO_IBGE}&periodo={competencia}"
        )
        try:
            r = s.get(api_url, timeout=30)
            if r.status_code == 404:
                log.info("  competência sem publicação (%s), tentando anterior...", competencia)
                continue
            r.raise_for_status()
            out.write_bytes(r.content)
            log.info("  salvo: %s (competência %s)", out.name, competencia)
            return
        except requests.RequestException as e:
            log.warning("  falha e-Gestor APS (%s): %s", competencia, e)

    _write_stub(
        dest.parent / "DOWNLOAD_MANUAL.txt",
        f"""e-Gestor APS — Cobertura ESF:
  1. Acesse https://egestorab.saude.gov.br
  2. Menu: Relatórios > Atenção Básica > Cobertura da AB
  3. Filtrar: Porto Alegre (RS), competência mais recente
  4. Exportar CSV e salvar em: data/raw/esf/cobertura_esf_poa.csv

Campo necessário: co_cnes, nu_cnes, nu_cobertura_esf
""",
    )


# ---------------------------------------------------------------------------
# 5. DataSUS FTP — SIM, SINASC, SINAN
# ---------------------------------------------------------------------------

def download_datasus_ftp(skip_large: bool = False) -> None:
    """
    Baixa arquivos DBC do DataSUS via FTP para SIM, SINASC e SINAN.

    Os arquivos DBC podem ser convertidos para CSV com o pacote `pysus`
    ou com `blast-dbf` / `read.dbc` do R.
    """
    log.info("=== DataSUS FTP — SIM / SINASC / SINAN ===")

    ftp_base = "ftp://ftp.datasus.gov.br/dissemin/publicos"
    ano_atual = date.today().year
    anos = [str(a)[-2:] for a in range(ano_atual - 1, ano_atual - 8, -1)]
    alvo_por_fonte = 3

    if skip_large:
        log.warning("  --skip-large ativo; pulando DataSUS FTP")
        for pasta in ("sim", "sinasc", "sinan"):
            _write_stub(
                RAW / pasta / "DOWNLOAD_MANUAL.txt",
                f"Baixar via FTP:\n{ftp_base}\n"
                "Ou via TabNet: https://datasus.saude.gov.br/transferencia-de-arquivos\n"
                "Usar pysus para converter DBC -> DataFrame:\n"
                "  import pysus.online_data.SIM as SIM\n"
                "  df = SIM.download(state='RS', year=2022)\n",
            )
        return

    import urllib.request

    def _download_ftp_first(candidates: list[str], dest: Path) -> bool:
        if dest.exists() and dest.stat().st_size > 0:
            log.info("  já existe: %s", dest.name)
            return True
        last_err: Optional[Exception] = None
        for url in candidates:
            try:
                log.info("  baixando (FTP): %s", Path(urlparse(url).path).name)
                dest.parent.mkdir(parents=True, exist_ok=True)
                urllib.request.urlretrieve(url, dest)
                log.info("  salvo: %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
                return True
            except Exception as e:  # noqa: BLE001
                last_err = e
        log.warning("  falha FTP %s: %s", dest.name, last_err)
        return False

    baixados_sim = 0
    baixados_sinasc = 0
    for ano in anos:
        if baixados_sim < alvo_por_fonte:
            sim_dest = RAW / "sim" / f"DORES{COD_UF}{ano}.dbc"
            sim_urls = [
                f"{ftp_base}/SIM/CID10/DORES/DORES{COD_UF}{ano}.dbc",
            ]
            if _download_ftp_first(sim_urls, sim_dest):
                baixados_sim += 1

        if baixados_sinasc < alvo_por_fonte:
            sinasc_dest = RAW / "sinasc" / f"DNRES{COD_UF}{ano}.dbc"
            sinasc_urls = [
                f"{ftp_base}/SINASC/NOV/DNRES/DNRES{COD_UF}{ano}.dbc",
                f"{ftp_base}/SINASC/1996_2022/DNRES/DNRES{COD_UF}{ano}.dbc",
            ]
            if _download_ftp_first(sinasc_urls, sinasc_dest):
                baixados_sinasc += 1

    sinan_dest = RAW / "sinan" / "SIFCRS.dbc"
    sinan_urls = [
        f"{ftp_base}/SINAN/DADOS/FINAIS/SIFCRS.dbc",
        f"{ftp_base}/SINAN/DADOS/FINAIS/SIFILRS.dbc",
    ]
    baixado_sinan = _download_ftp_first(sinan_urls, sinan_dest)

    if baixados_sim == 0:
        _write_stub(
            RAW / "sim" / "PYSUS_ALTERNATIVO.txt",
            "# Usar pysus como alternativa:\n"
            "# pip install pysus\n"
            "# python src/download_pysus.py\n",
        )
    if baixados_sinasc == 0:
        _write_stub(
            RAW / "sinasc" / "PYSUS_ALTERNATIVO.txt",
            "# Usar pysus como alternativa:\n"
            "# pip install pysus\n"
            "# python src/download_pysus.py\n",
        )
    if not baixado_sinan:
        _write_stub(
            RAW / "sinan" / "PYSUS_ALTERNATIVO.txt",
            "# Usar pysus como alternativa:\n"
            "# pip install pysus\n"
            "# python src/download_pysus.py\n",
        )


# ---------------------------------------------------------------------------
# 6. INEP — Censo Escolar (matrícula e evasão)
# ---------------------------------------------------------------------------

def download_censo_escolar(skip_large: bool = False) -> None:
    """
    Microdados do Censo Escolar INEP — matrículas e funções docentes.

    URL: https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/
         microdados/censo-escolar
    """
    log.info("=== INEP — Censo Escolar ===")
    s = _session()
    ano_ref = date.today().year - 1
    candidatos: list[tuple[str, int]] = []
    for ano in range(ano_ref, ano_ref - 6, -1):
        candidatos.extend([
            (
                "https://download.inep.gov.br/dados_abertos/microdados/"
                f"censo_escolar/{ano}/microdados_educacao_basica_{ano}.zip",
                ano,
            ),
            (
                "https://download.inep.gov.br/dados_abertos/microdados/"
                f"censo_escolar/{ano}/microdados_censo_escolar_{ano}.zip",
                ano,
            ),
        ])

    escolhido = next(((url, ano) for url, ano in candidatos if _http_exists(url, session=s)), None)
    if not escolhido:
        _write_stub(
            RAW / "censo_escolar" / "DOWNLOAD_MANUAL.txt",
            "Não foi possível localizar automaticamente o arquivo no INEP.\n"
            "Baixar em: https://www.gov.br/inep/pt-br/acesso-a-informacao/"
            "dados-abertos/microdados/censo-escolar\n"
            "Extrair em: data/raw/censo_escolar/\n",
        )
        log.warning("  não foi possível resolver URL automática do Censo Escolar")
        return

    url, ano = escolhido
    dest = RAW / "censo_escolar" / f"microdados_censo_escolar_{ano}.zip"

    if skip_large:
        log.warning("  --skip-large ativo; pulando Censo Escolar (>1 GB)")
        _write_stub(
            dest.parent / "DOWNLOAD_MANUAL.txt",
            f"Baixar em: https://www.gov.br/inep/pt-br/acesso-a-informacao/"
            f"dados-abertos/microdados/censo-escolar\n"
            f"Arquivo: microdados_educacao_basica_{ano}.zip (~1.5 GB)\n"
            f"Extrair em: data/raw/censo_escolar/\n"
            f"Usar apenas: matricula_{COD_UF}.csv (filtrar CO_MUNICIPIO={COD_MUNICIPIO_IBGE})\n",
        )
        return

    archive = download(url, dest, timeout=300, session=s)
    # Extrai apenas os arquivos do RS para economizar espaço
    log.info("  extraindo arquivos do %s...", COD_UF)
    with zipfile.ZipFile(archive, "r") as zf:
        rs_files = [n for n in zf.namelist()
                    if COD_UF in n.upper() or "MATRICULA" in n.upper()]
        for name in rs_files:
            zf.extract(name, dest.parent)
    log.info("  extraídos %d arquivos", len(rs_files))


# ---------------------------------------------------------------------------
# 7. MDS — Bolsa Família por município
# ---------------------------------------------------------------------------

def download_bolsa_familia() -> None:
    """
    Dados do Bolsa Família por município via API do Portal da Transparência.

    Documentação: https://portaldatransparencia.gov.br/api-de-dados/bolsa-familia-por-municipio
    """
    log.info("=== MDS — Bolsa Família (Portal da Transparência) ===")
    dest = RAW / "pbf" / "bolsa_familia_poa.json"
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and dest.stat().st_size > 0:
        log.info("  já existe: %s", dest.name)
        return

    # Competência: mês anterior
    hoje = date.today()
    if hoje.month == 1:
        ano_comp, mes_comp = hoje.year - 1, 12
    else:
        ano_comp, mes_comp = hoje.year, hoje.month - 1
    mes_str = f"{ano_comp}{mes_comp:02d}"

    url = (
        "https://api.portaldatransparencia.gov.br/api-de-dados/bolsa-familia-por-municipio"
        f"?anoMesReferencia={mes_str}&codigoIbge={COD_MUNICIPIO_IBGE}&pagina=1"
    )
    s = _session()
    # A API exige header com chave — orientação para obtenção
    # https://portaldatransparencia.gov.br/api-de-dados/cadastro-api
    api_key = os.environ.get("TRANSPARENCIA_API_KEY", "")
    if api_key:
        s.headers["chave-api-dados"] = api_key
    try:
        r = s.get(url, timeout=30)
        r.raise_for_status()
        dest.write_bytes(r.content)
        log.info("  salvo: %s", dest.name)
    except requests.RequestException as e:
        log.warning("  falha Portal Transparência: %s", e)
        _write_stub(
            dest.parent / "DOWNLOAD_MANUAL.txt",
            """Portal da Transparência — Bolsa Família:
  1. Cadastre-se em: https://portaldatransparencia.gov.br/api-de-dados/cadastro-api
  2. Obtenha sua chave de API
  3. Execute: TRANSPARENCIA_API_KEY=<sua_chave> python src/download_shapefiles.py --only pbf

  Alternativa — download manual:
  https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos
  Filtrar por município (Porto Alegre) e salvar em: data/raw/pbf/

  Para granularidade por setor censitário, usar:
  https://cecad.cidadania.gov.br (dados por setor — requer login gov.br)
""",
        )


# ---------------------------------------------------------------------------
# Utilidade: stub de instrução
# ---------------------------------------------------------------------------

def _write_stub(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(content, encoding="utf-8")
        log.info("  instruções manuais: %s", path)


# ---------------------------------------------------------------------------
# Script auxiliar: geração de Voronoi a partir dos pontos CNES
# ---------------------------------------------------------------------------

VORONOI_SCRIPT = '''\
"""
gerar_voronoi_ubs.py
====================
Gera polígonos de Voronoi a partir dos pontos geocodificados das UBS do CNES.
Usado como fallback quando o shapefile oficial de territórios não está disponível.

Saída: data/raw/ubs_territorios/territorios_ubs_voronoi.geojson
"""
from __future__ import annotations
import json
from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union
from scipy.spatial import Voronoi
import numpy as np
from shapely.geometry import Polygon

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"

def pontos_cnes_para_gdf() -> gpd.GeoDataFrame:
    """Lê o JSON do CNES e retorna GeoDataFrame de pontos."""
    cnes_file = RAW / "cnes" / "ubs_poa.json"
    if not cnes_file.exists():
        raise FileNotFoundError(
            "Execute primeiro: python src/download_shapefiles.py --only cnes"
        )
    data = json.loads(cnes_file.read_text())
    # Adaptar ao schema real da resposta CNES
    registros = data if isinstance(data, list) else data.get("content", data.get("items", []))
    df = pd.json_normalize(registros)
    # Colunas esperadas (ajustar se o schema mudar):
    lat_col = next((c for c in df.columns if "lat" in c.lower()), None)
    lon_col = next((c for c in df.columns if "lon" in c.lower() or "lng" in c.lower()), None)
    if not lat_col or not lon_col:
        raise ValueError(f"Colunas lat/lon não encontradas. Colunas disponíveis: {list(df.columns)}")
    df = df.dropna(subset=[lat_col, lon_col])
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs="EPSG:4674",
    )
    return gdf


def voronoi_finito(pontos: gpd.GeoDataFrame, contorno: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Calcula polígonos de Voronoi recortados pelo contorno do município."""
    coords = np.array([[p.x, p.y] for p in pontos.geometry])
    # Adicionar pontos espelho nas bordas para fechar o diagrama
    bounding = contorno.total_bounds  # minx, miny, maxx, maxy
    pad = 0.1
    espelhos = np.array([
        [bounding[0] - pad, bounding[1] - pad],
        [bounding[2] + pad, bounding[1] - pad],
        [bounding[0] - pad, bounding[3] + pad],
        [bounding[2] + pad, bounding[3] + pad],
    ])
    coords_ext = np.vstack([coords, espelhos])
    vor = Voronoi(coords_ext)
    poligonos = []
    limite = unary_union(contorno.geometry)
    for i, ponto_idx in enumerate(range(len(coords))):
        region_idx = vor.point_region[ponto_idx]
        region = vor.regions[region_idx]
        if -1 in region or not region:
            poligonos.append(None)
            continue
        vertices = vor.vertices[region]
        poly = Polygon(vertices).intersection(limite)
        poligonos.append(poly)
    result = pontos.copy()
    result["geometry"] = poligonos
    result = result[result.geometry.notna()].copy()
    result = result.set_geometry("geometry")
    return result


def main():
    import sys
    # Contorno de Porto Alegre — baixar do IBGE se necessário
    contorno_file = RAW / "ibge_setores" / "contorno_poa.geojson"
    if not contorno_file.exists():
        print("Gerando contorno de POA a partir dos setores censitários...")
        setores_dir = RAW / "ibge_setores"
        shps = list(setores_dir.glob("*.shp"))
        if not shps:
            print("ERRO: shapefile de setores não encontrado.")
            print("Execute: python src/download_shapefiles.py --only ibge")
            sys.exit(1)
        setores = gpd.read_file(shps[0])
        # Filtrar Porto Alegre (CD_MUNICIPIO = '4314902')
        poa = setores[setores["CD_MUNICIPIO"] == "4314902"]
        contorno = gpd.GeoDataFrame(geometry=[unary_union(poa.geometry)], crs=poa.crs)
        contorno.to_file(contorno_file, driver="GeoJSON")
    else:
        contorno = gpd.read_file(contorno_file)

    print("Carregando pontos CNES...")
    pontos = pontos_cnes_para_gdf()
    print(f"  {len(pontos)} UBS encontradas")

    print("Calculando Voronoi...")
    territorios = voronoi_finito(pontos, contorno)
    out = RAW / "ubs_territorios" / "territorios_ubs_voronoi.geojson"
    out.parent.mkdir(parents=True, exist_ok=True)
    territorios.to_file(out, driver="GeoJSON")
    print(f"Salvo: {out}")
    print("ATENÇÃO: territórios Voronoi são aproximações geométricas.")
    print("Prefira o shapefile oficial da SMS-POA quando disponível.")


if __name__ == "__main__":
    main()
'''


def _create_voronoi_script() -> None:
    dest = Path(__file__).resolve().parent / "gerar_voronoi_ubs.py"
    if not dest.exists():
        dest.write_text(VORONOI_SCRIPT, encoding="utf-8")
        log.info("criado: %s", dest)


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------

FONTES = {
    "ibge":          download_ibge_setores,
    "cnes":          download_cnes_ubs,
    "ubs":           download_ubs_shapefile,
    "esf":           download_esf_cobertura,
    "sim":           download_datasus_ftp,
    "sinasc":        download_datasus_ftp,
    "sinan":         download_datasus_ftp,
    "censo_escolar": download_censo_escolar,
    "pbf":           download_bolsa_familia,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Download de dados para o IVSaúde POA")
    parser.add_argument("--skip-large", action="store_true",
                        help="Pula arquivos > 500 MB")
    parser.add_argument("--only", choices=list(FONTES), default=None,
                        help="Baixa apenas a fonte especificada")
    args = parser.parse_args()

    RAW.mkdir(parents=True, exist_ok=True)
    _create_voronoi_script()

    if args.only:
        fontes_rodar = {args.only: FONTES[args.only]}
    else:
        fontes_rodar = FONTES

    # DataSUS FTP agrupa SIM+SINASC+SINAN em uma única função
    funcs_vistas: set = set()
    for nome, func in fontes_rodar.items():
        if func in funcs_vistas:
            continue
        funcs_vistas.add(func)
        kwargs: dict = {}
        if func in (download_ibge_setores, download_datasus_ftp, download_censo_escolar):
            kwargs["skip_large"] = args.skip_large
        try:
            func(**kwargs)
        except Exception as e:
            log.error("Erro em %s: %s", nome, e, exc_info=True)

    log.info("=== Download concluído. Verifique data/raw/ ===")


if __name__ == "__main__":
    main()
