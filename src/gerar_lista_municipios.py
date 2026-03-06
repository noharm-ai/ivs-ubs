"""
gerar_lista_municipios.py
=========================
Gera src/data/municipios_com_ubs.csv com todos os municípios brasileiros
que possuem UBS cadastrada no CNES (tipoUnidade=1).

Uso rápido (sem verificar CNES — lista todos os 5570 municípios):
    python src/gerar_lista_municipios.py

Uso completo (verifica CNES para cada município, ~30 min):
    python src/gerar_lista_municipios.py --check-cnes

Filtrar por UF:
    python src/gerar_lista_municipios.py --check-cnes --uf RS
"""
from __future__ import annotations

import argparse
import csv
import logging
import re
import time
import unicodedata
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUT_CSV = Path(__file__).resolve().parent / "data" / "municipios_com_ubs.csv"
IBGE_API = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome"
CNES_API = "https://cnes.datasus.gov.br/services/estabelecimentos-lite?municipio={ibge}&tipoUnidade=1"


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


def check_cnes_ubs(session: requests.Session, ibge7: str, retries: int = 3) -> int:
    """Retorna número de UBS no CNES para o município. -1 em caso de erro."""
    url = CNES_API.format(ibge=ibge7)
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                return len(data)
            if isinstance(data, dict):
                for k in ("content", "items", "data", "result"):
                    v = data.get(k)
                    if isinstance(v, list):
                        return len(v)
            return 0
        except Exception as e:  # noqa: BLE001
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                log.debug("CNES erro para %s: %s", ibge7, e)
    return -1


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera lista de municípios com UBS no CNES")
    parser.add_argument(
        "--check-cnes",
        action="store_true",
        help="Verifica CNES para cada município e filtra os sem UBS (~30 min para o Brasil todo)",
    )
    parser.add_argument("--uf", default=None, help="Filtrar por UF (ex.: RS)")
    parser.add_argument(
        "--min-ubs",
        type=int,
        default=1,
        help="Mínimo de UBS para incluir o município (padrão: 1, requer --check-cnes)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.3,
        help="Delay em segundos entre requisições CNES (padrão: 0.3)",
    )
    parser.add_argument("--out", default=str(OUT_CSV), help="Arquivo CSV de saída")
    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers["User-Agent"] = "ivs-batch/1.0 (pesquisa saude publica)"

    municipios_ibge = fetch_municipios_ibge(session)

    rows: list[dict] = []
    total = len(municipios_ibge)
    skipped = 0

    for i, m in enumerate(municipios_ibge, 1):
        ibge7 = str(m["id"])
        nome = m["nome"]
        uf = m["microrregiao"]["mesorregiao"]["UF"]["sigla"]

        if args.uf and uf.upper() != args.uf.upper():
            continue

        n_ubs: int | str = ""

        if args.check_cnes:
            n_ubs = check_cnes_ubs(session, ibge7)
            if n_ubs >= 0 and n_ubs < args.min_ubs:
                skipped += 1
                if i % 100 == 0:
                    log.info("  progresso: %d/%d — %d incluídos, %d sem UBS", i, total, len(rows), skipped)
                time.sleep(args.delay)
                continue
            time.sleep(args.delay)

        rows.append({
            "ibge7": ibge7,
            "ibge6": ibge7[:6],
            "nome": nome,
            "uf": uf,
            "slug": slugify(nome),
            "n_ubs": n_ubs,
        })

        if args.check_cnes and i % 100 == 0:
            log.info("  progresso: %d/%d — %d incluídos, %d sem UBS", i, total, len(rows), skipped)

    fieldnames = ["ibge7", "ibge6", "nome", "uf", "slug", "n_ubs"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    log.info("Salvo: %s (%d municípios)", out_path, len(rows))
    if args.check_cnes:
        log.info("  %d municípios sem UBS removidos", skipped)


if __name__ == "__main__":
    main()
