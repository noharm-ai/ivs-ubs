"""
gerar_lista_municipios.py
=========================
Gera src/data/municipios_com_ubs.csv com todos os municípios brasileiros
que possuem UBS cadastrada no CNES.

Fonte primária (local): tbEstabelecimento*.csv da BASE_DE_DADOS_CNES
  python src/gerar_lista_municipios.py --cnes-dir data/BASE_DE_DADOS_CNES_202601

Fonte alternativa (API IBGE sem filtro CNES):
  python src/gerar_lista_municipios.py

Filtrar por UF:
  python src/gerar_lista_municipios.py --cnes-dir data/BASE_DE_DADOS_CNES_202601 --uf RS
"""
from __future__ import annotations

import argparse
import csv
import logging
import re
import unicodedata
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
OUT_CSV = Path(__file__).resolve().parent / "data" / "municipios_com_ubs.csv"
IBGE_API = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome"

# TP_UNIDADE: 1 = Posto de Saúde, 2 = Centro de Saúde/Unidade Básica
UBS_TIPOS = {1, 2}


def slugify(text: str) -> str:
    txt = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"[^a-zA-Z0-9]+", "_", txt).strip("_").lower()
    return txt or "municipio"


def fetch_municipios_ibge(session: requests.Session) -> list[dict]:
    log.info("Buscando lista de municípios do IBGE API...")
    r = session.get(IBGE_API, timeout=60)
    r.raise_for_status()
    data = r.json()
    log.info("  %d municípios retornados", len(data))
    return data


def contagem_ubs_por_municipio(cnes_dir: Path) -> dict[str, int]:
    """
    Lê tbEstabelecimento*.csv e retorna {co_municipio_gestor: n_ubs}.
    Conta apenas TP_UNIDADE 1 (Posto de Saúde) e 2 (Centro de Saúde/UBS).
    """
    # Localiza o arquivo tbEstabelecimento na pasta
    candidates = sorted(cnes_dir.glob("tbEstabelecimento*.csv"), reverse=True)
    if not candidates:
        raise FileNotFoundError(f"tbEstabelecimento*.csv não encontrado em {cnes_dir}")

    estab_file = candidates[0]
    log.info("Lendo %s ...", estab_file.name)

    df = pd.read_csv(
        estab_file,
        sep=";",
        encoding="latin-1",
        usecols=["TP_UNIDADE", "CO_MUNICIPIO_GESTOR"],
        dtype={"CO_MUNICIPIO_GESTOR": str},
        low_memory=False,
    )

    ubs = df[df["TP_UNIDADE"].isin(UBS_TIPOS)].copy()
    # CNES usa código de 6 dígitos (sem o dígito verificador do IBGE)
    ubs["CO_MUNICIPIO_GESTOR"] = ubs["CO_MUNICIPIO_GESTOR"].str.strip().str.zfill(6)

    counts = ubs.groupby("CO_MUNICIPIO_GESTOR").size().to_dict()
    log.info(
        "  %d UBS em %d municípios (tipos %s)",
        ubs.shape[0], len(counts), UBS_TIPOS,
    )
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera lista de municípios com UBS no CNES")
    parser.add_argument(
        "--cnes-dir",
        default=None,
        help="Pasta com BASE_DE_DADOS_CNES (ex.: data/BASE_DE_DADOS_CNES_202601). "
             "Se omitido, lista todos os municípios da API IBGE sem filtrar por UBS.",
    )
    parser.add_argument("--uf", default=None, help="Filtrar por UF (ex.: RS)")
    parser.add_argument(
        "--min-ubs",
        type=int,
        default=1,
        help="Mínimo de UBS para incluir o município (padrão: 1, requer --cnes-dir)",
    )
    parser.add_argument("--out", default=str(OUT_CSV), help="Arquivo CSV de saída")
    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # --- Contagem de UBS por município (fonte local) ---
    ubs_counts: dict[str, int] = {}
    if args.cnes_dir:
        cnes_dir = Path(args.cnes_dir)
        if not cnes_dir.is_absolute():
            cnes_dir = ROOT / cnes_dir
        ubs_counts = contagem_ubs_por_municipio(cnes_dir)

    # --- Lista de municípios (IBGE API) ---
    session = requests.Session()
    session.headers["User-Agent"] = "ivs-batch/1.0 (pesquisa saude publica)"
    municipios_ibge = fetch_municipios_ibge(session)

    rows: list[dict] = []
    skipped = 0

    for m in municipios_ibge:
        ibge7 = str(m["id"]).zfill(7)
        nome = m["nome"]
        try:
            uf = m["microrregiao"]["mesorregiao"]["UF"]["sigla"]
        except (TypeError, KeyError):
            uf = (m.get("regiao-imediata") or {}).get("regiao-intermediaria", {}).get("UF", {}).get("sigla", "")
            if not uf:
                continue

        if args.uf and uf.upper() != args.uf.upper():
            continue

        ibge6 = ibge7[:6]
        n_ubs: int | str = ubs_counts.get(ibge6, 0) if ubs_counts else ""

        if ubs_counts and isinstance(n_ubs, int) and n_ubs < args.min_ubs:
            skipped += 1
            continue

        rows.append({
            "ibge7": ibge7,
            "ibge6": ibge7[:6],
            "nome": nome,
            "uf": uf,
            "slug": slugify(nome),
            "n_ubs": n_ubs,
        })

    fieldnames = ["ibge7", "ibge6", "nome", "uf", "slug", "n_ubs"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    log.info("Salvo: %s (%d municípios)", out_path, len(rows))
    if ubs_counts:
        log.info("  %d municípios sem UBS suficiente removidos", skipped)


if __name__ == "__main__":
    main()
