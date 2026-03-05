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
import logging
import os
import sys
import time
import zipfile
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
COD_MUNICIPIO_IBGE = "4314902"   # Porto Alegre
COD_UF = "RS"
UF_IBGE_NUM = "43"               # código numérico do RS no IBGE

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

    base = (
        "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
        "malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/"
        "censo_2022/setores_censitarios_shp/"
    )
    filename = f"SC_Intra_{COD_UF}_2022.zip"
    url = urljoin(base, f"{COD_UF}/{filename}")
    dest = RAW / "ibge_setores" / filename

    if skip_large:
        log.warning("  --skip-large ativo; ignorando setores IBGE (arquivo ~200 MB)")
        _write_stub(dest.parent / "DOWNLOAD_MANUAL.txt",
                    f"Baixar manualmente:\n{url}\n\n"
                    "Extrair em: data/raw/ibge_setores/\n")
        return

    archive = download(url, dest)
    unzip(archive, dest.parent)

    # Tabelas de resultados do Universo (Censo 2022) — basico_RS
    log.info("=== IBGE — Resultado do Universo (basico_RS) ===")
    tabelas_base = (
        "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
        "Resultados_do_Universo/Agregados_por_Setores_Censitarios/"
    )
    # Arquivo compactado por UF
    tab_file = f"RS_20231030.zip"
    tab_url = tabelas_base + tab_file
    tab_dest = RAW / "ibge_universo" / tab_file
    if not skip_large:
        archive2 = download(tab_url, tab_dest)
        unzip(archive2, tab_dest.parent)
    else:
        _write_stub(tab_dest.parent / "DOWNLOAD_MANUAL.txt",
                    f"Baixar manualmente:\n{tab_url}\n\nExtrair em: data/raw/ibge_universo/\n")


# ---------------------------------------------------------------------------
# 2. CNES — georreferenciamento das UBS
# ---------------------------------------------------------------------------

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
    try:
        r = s.get(url, timeout=30)
        r.raise_for_status()
        dest.write_bytes(r.content)
        log.info("  salvo: %s", dest.name)
    except requests.RequestException as e:
        log.warning("  falha CNES API: %s", e)
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
    Tenta baixar o shapefile de territórios das UBS de Porto Alegre.

    Fontes tentadas (em ordem):
      1. Portal de Dados Abertos de POA (dadosabertos.poa.br) — grupo Saúde
      2. GIS-SMAMUS (gis-smamus.portoalegre.rs.gov.br) — ArcGIS REST
    Se todas falharem, gera instruções manuais e fallback Voronoi.

    ATENÇÃO: GeoSampa (geosampa.prefpoa.com.br) é o geoportal de São Paulo
    e NÃO existe em Porto Alegre. O portal correto é o GIS-SMAMUS/SMS-POA.
    """
    log.info("=== SMS-POA — Territórios das 141 UBS ===")
    dest_dir = RAW / "ubs_territorios"
    dest_dir.mkdir(parents=True, exist_ok=True)
    geojson_dest = dest_dir / "territorios_ubs.geojson"

    if geojson_dest.exists() and geojson_dest.stat().st_size > 0:
        log.info("  já existe: %s", geojson_dest.name)
        return

    s = _session()

    # Tentativa 1: Portal Dados Abertos POA — grupo Saúde
    # Verificar datasets disponíveis em: https://dadosabertos.poa.br/group/saude
    dados_abertos_urls = [
        "https://dadosabertos.poa.br/dataset/areas-abrangencia-ubs/resource/territorios-ubs.geojson",
        "https://dadosabertos.poa.br/dataset/unidades-de-saude/resource/ubs-territorios.geojson",
    ]
    for url in dados_abertos_urls:
        try:
            log.info("  tentando dados abertos POA: %s", url)
            r = s.get(url, timeout=30)
            r.raise_for_status()
            geojson_dest.write_bytes(r.content)
            log.info("  salvo: %s", geojson_dest.name)
            return
        except requests.RequestException as e:
            log.warning("  falha: %s", e)

    # Tentativa 2: GIS-SMAMUS ArcGIS REST (camadas públicas)
    # Explorar layers disponíveis: https://gis-smamus.portoalegre.rs.gov.br/server/rest/services
    smamus_urls = [
        (
            "https://gis-smamus.portoalegre.rs.gov.br/server/rest/services/"
            "01_PUBLICACOES/saude_ubs/MapServer/0/query"
            "?where=1%3D1&outFields=*&f=geojson&outSR=4674",
            dest_dir / "smamus_ubs.geojson",
        ),
    ]
    for url, out in smamus_urls:
        try:
            log.info("  tentando GIS-SMAMUS: %s", url)
            r = s.get(url, timeout=60)
            r.raise_for_status()
            out.write_bytes(r.content)
            log.info("  salvo: %s (%.1f MB)", out.name, out.stat().st_size / 1e6)
        except requests.RequestException as e:
            log.warning("  falha GIS-SMAMUS: %s", e)

    # Sempre gerar instruções manuais de fallback
    _write_stub(
        dest_dir / "INSTRUCOES_MANUAIS.txt",
        """FONTES CORRETAS para territórios de UBS em Porto Alegre
========================================================

Opção A — Portal de Dados Abertos de Porto Alegre:
  1. Acesse https://dadosabertos.poa.br/group/saude
  2. Procure dataset "Áreas de Abrangência das UBS" ou "Unidades de Saúde"
  3. Baixe o arquivo GeoJSON ou Shapefile disponível
  4. Salve em: data/raw/ubs_territorios/territorios_ubs.geojson

Opção B — GIS-SMAMUS (Portal GIS da Prefeitura de Porto Alegre):
  1. Acesse https://gis-smamus.portoalegre.rs.gov.br/portal/home/
  2. Pesquise por "UBS" ou "Saúde" nas camadas públicas
  3. Exporte como GeoJSON com CRS EPSG:4674 (SIRGAS 2000)
  4. Salve em: data/raw/ubs_territorios/territorios_ubs.geojson

Opção C — SMS / Territorialização (contato direto):
  https://prefeitura.poa.br/sms/bvaps-biblioteca-virtual-de-atencao-primaria-saude/territorializacao
  Solicitar shapefile ao Comitê de Territorialização em Saúde (CMTS)
  via protocolo SEI ou e-mail para a SMS-POA.

Opção D — Mapa interativo SMS:
  https://prefeitura.poa.br/sms/onde-esta-o-aedes/mapas
  Inspecionar as requisições de rede (DevTools > Network) para
  capturar a URL da API usada pelo mapa interativo.

Opção E — Voronoi pelo CNES (automático, sem shapefile oficial):
  Execute: python src/gerar_voronoi_ubs.py
  Gera territórios aproximados a partir dos pontos geocodificados do CNES.
  Ativar com: python src/calcular_ivs.py --voronoi

ATENÇÃO: "GeoSampa" é o geoportal de SÃO PAULO e não existe em Porto Alegre.

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

    # Competência mais recente — usar AAAAMM
    from datetime import date
    competencia = date.today().strftime("%Y%m")

    url = (
        f"https://egestorab.saude.gov.br/gestaoaps/relCoberturaABEmEsf.xhtml"
        f"?municipio={COD_MUNICIPIO_IBGE}&competencia={competencia}"
    )
    # Tentativa via API (pode requerer autenticação em produção)
    api_url = (
        "https://egestorab.saude.gov.br/api/public/relatorios/profissionais/cobertura"
        f"?municipio={COD_MUNICIPIO_IBGE}&periodo={competencia}"
    )
    s = _session()
    for try_url, fname in [(api_url, "cobertura_esf_poa.json")]:
        out = RAW / "esf" / fname
        try:
            r = s.get(try_url, timeout=30)
            r.raise_for_status()
            out.write_bytes(r.content)
            log.info("  salvo: %s", out.name)
            return
        except requests.RequestException as e:
            log.warning("  falha e-Gestor APS: %s", e)

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
    # Último triênio disponível (ajuste o ano conforme necessidade)
    from datetime import date
    ano_atual = date.today().year
    anos_sim = [str(a)[-2:] for a in range(ano_atual - 3, ano_atual)]

    arquivos = []
    # SIM — óbitos (arquivo por UF, filtrar POA depois)
    for ano in anos_sim:
        arquivos.append((
            f"{ftp_base}/SIM/CID10/DORES/DORS{COD_UF}{ano}.dbc",
            RAW / "sim" / f"DORS{COD_UF}{ano}.dbc",
        ))
    # SINASC — nascidos vivos
    for ano in anos_sim:
        arquivos.append((
            f"{ftp_base}/SINASC/1996_2022/DNRES/DN{COD_UF}{ano}.dbc",
            RAW / "sinasc" / f"DN{COD_UF}{ano}.dbc",
        ))
    # SINAN — sífilis congênita
    arquivos.append((
        f"{ftp_base}/SINAN/DADOS/FINAIS/SIFCRS.dbc",
        RAW / "sinan" / "SIFCRS.dbc",
    ))

    if skip_large:
        log.warning("  --skip-large ativo; pulando DataSUS FTP")
        for _, dest in arquivos:
            _write_stub(dest.parent / "DOWNLOAD_MANUAL.txt",
                        f"Baixar via FTP:\n{ftp_base}\n"
                        "Ou via TabNet: https://datasus.saude.gov.br/transferencia-de-arquivos\n"
                        "Usar pysus para converter DBC -> DataFrame:\n"
                        "  import pysus.online_data.SIM as SIM\n"
                        "  df = SIM.download(state='RS', year=2022)\n")
        return

    s = _session()
    for url, dest in arquivos:
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            # FTP via requests não funciona — usar urllib
            import urllib.request
            if not (dest.exists() and dest.stat().st_size > 0):
                log.info("  baixando (FTP): %s", dest.name)
                urllib.request.urlretrieve(url, dest)
                log.info("  salvo: %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
            else:
                log.info("  já existe: %s", dest.name)
        except Exception as e:
            log.warning("  falha FTP %s: %s", dest.name, e)
            _write_stub(dest.parent / "PYSUS_ALTERNATIVO.txt",
                        f"# Usar pysus como alternativa:\n"
                        f"# pip install pysus\n"
                        f"# python src/download_pysus.py\n")


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
    from datetime import date
    ano = date.today().year - 1  # último ano publicado

    url = (
        f"https://download.inep.gov.br/dados_abertos/microdados/"
        f"censo_escolar/{ano}/microdados_educacao_basica_{ano}.zip"
    )
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

    archive = download(url, dest, timeout=300)
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

    from datetime import date
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
