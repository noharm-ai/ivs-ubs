"""
batch_ivs.py
============
Processa múltiplos municípios em sequência:
  1. download_municipio.py   — CNES, IBGE, OSM, SIM, SINASC, Censo Escolar…
  2. calcular_ivs_municipio.py — IVS por território de UBS
  3. gerar_pagina_municipio.py --no-html --out-json — JSON para GitHub Pages

O script registra o estado de cada município em batch_status.csv e
retoma de onde parou se interrompido.

Uso:
    # Rodar em background (nohup):
    nohup python src/batch_ivs.py > batch.log 2>&1 &

    # Filtrar por UF:
    python src/batch_ivs.py --uf RS

    # Pular downloads grandes (IBGE/Censo Escolar) para teste rápido:
    python src/batch_ivs.py --skip-large

    # Reprocessar município específico (ignora status anterior):
    python src/batch_ivs.py --municipio 4314407

    # Limitar quantidade de municípios:
    python src/batch_ivs.py --limit 10
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Carrega .env da raiz do projeto (não sobrescreve variáveis já definidas no ambiente)
_env_file = ROOT / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())
SRC = ROOT / "src"
DOCS_DATA = ROOT / "docs" / "data"
LISTA_CSV = SRC / "data" / "municipios_com_ubs.csv"
STATUS_CSV = ROOT / "batch_status.csv"
LOG_FILE = ROOT / "batch_ivs.log"

# ---------------------------------------------------------------------------
# Logging: arquivo + stdout
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Estrutura de status
# ---------------------------------------------------------------------------
@dataclass
class MunicipioStatus:
    ibge7: str
    nome: str
    uf: str
    slug: str
    etapa: str = ""          # download | calcular | gerar_json | ok | erro
    ok: bool = False
    erro: str = ""
    inicio: str = ""
    fim: str = ""
    duracao_s: float = 0.0


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_status() -> dict[str, MunicipioStatus]:
    if not STATUS_CSV.exists():
        return {}
    result: dict[str, MunicipioStatus] = {}
    with open(STATUS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            s = MunicipioStatus(**{
                k: v for k, v in row.items() if k in MunicipioStatus.__dataclass_fields__
            })
            s.ok = s.ok in (True, "True", "true", "1")
            s.duracao_s = float(s.duracao_s or 0)
            result[s.ibge7] = s
    return result


def _save_status(statuses: dict[str, MunicipioStatus]) -> None:
    STATUS_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(MunicipioStatus.__dataclass_fields__.keys())
    with open(STATUS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for s in statuses.values():
            w.writerow(asdict(s))


def _load_lista(lista_csv: Path, uf: str | None, municipio_ibge: str | None) -> list[dict]:
    if not lista_csv.exists():
        log.error("Lista de municípios não encontrada: %s", lista_csv)
        log.error("Execute primeiro: python src/gerar_lista_municipios.py")
        sys.exit(1)
    rows = []
    with open(lista_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if municipio_ibge and row["ibge7"] != municipio_ibge:
                continue
            if uf and row["uf"].upper() != uf.upper():
                continue
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Execução de subprocesso com log
# ---------------------------------------------------------------------------
def _run(cmd: list[str], label: str, timeout: int = 3600) -> tuple[bool, str]:
    """Executa comando com streaming de output em tempo real e retorna (sucesso, erro)."""
    log.info("  [%s] %s", label, " ".join(cmd))
    try:
        import time as _time
        proc = subprocess.Popen(
            cmd,
            cwd=str(SRC),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        lines: list[str] = []
        deadline = _time.monotonic() + timeout
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip()
            lines.append(line)
            log.info("    %s", line)
            if _time.monotonic() > deadline:
                proc.kill()
                err = f"timeout após {timeout}s"
                log.error("  [%s] %s", label, err)
                return False, err
        proc.wait()
        if proc.returncode != 0:
            err = "\n".join(lines[-10:]) or "erro sem mensagem"
            log.error("  [%s] FALHOU (código %d)", label, proc.returncode)
            return False, err[-500:]
        return True, ""
    except Exception as e:  # noqa: BLE001
        log.error("  [%s] exceção: %s", label, e)
        return False, str(e)


# ---------------------------------------------------------------------------
# Pipeline por município
# ---------------------------------------------------------------------------
def processar_municipio(
    ibge7: str,
    uf: str,
    nome: str,
    slug: str,
    skip_large: bool = False,
) -> tuple[bool, str]:
    """
    Executa as 3 etapas para um município.
    Retorna (sucesso, mensagem_de_erro).
    """
    base_dir = ROOT / "data" / uf.upper() / f"ivs_{slug}"
    out_json = DOCS_DATA / uf.upper() / f"{slug}.json"

    # Etapa 1: download
    cmd_download = [
        sys.executable, "download_municipio.py",
        "--municipio-ibge", ibge7,
        "--uf", uf,
        "--cidade", nome,
        "--slug", slug,
        "--base-dir", str(base_dir),
    ]
    if skip_large:
        cmd_download.append("--skip-large")

    ok, err = _run(cmd_download, "download", timeout=7200)
    if not ok:
        return False, f"download: {err}"

    # Etapa 2: calcular IVS
    cmd_ivs = [
        sys.executable, "calcular_ivs_municipio.py",
        "--base-dir", str(base_dir),
        "--slug", slug,
    ]
    ok, err = _run(cmd_ivs, "calcular_ivs", timeout=1800)
    if not ok:
        return False, f"calcular_ivs: {err}"

    # Etapa 3: gerar JSON
    DOCS_DATA.mkdir(parents=True, exist_ok=True)
    cmd_json = [
        sys.executable, "gerar_pagina_municipio.py",
        "--base-dir", str(base_dir),
        "--slug", slug,
        "--cidade", nome,
        "--uf", uf,
        "--ibge", ibge7,
        "--no-html",
        "--out-json", str(out_json),
    ]
    ok, err = _run(cmd_json, "gerar_json", timeout=600)
    if not ok:
        return False, f"gerar_json: {err}"

    return True, ""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Batch: download + IVS + JSON para múltiplos municípios")
    parser.add_argument("--lista", default=str(LISTA_CSV), help="CSV com lista de municípios")
    parser.add_argument("--uf", default=None, help="Processar apenas municípios desta UF (ex.: RS)")
    parser.add_argument("--municipio", default=None, help="Processar apenas este código IBGE (7 dígitos)")
    parser.add_argument("--skip-large", action="store_true", help="Pular downloads grandes (IBGE/Censo Escolar)")
    parser.add_argument("--limit", type=int, default=None, help="Processar no máximo N municípios")
    parser.add_argument("--force", action="store_true", help="Reprocessar mesmo municípios já concluídos")
    parser.add_argument("--delay", type=float, default=2.0, help="Pausa em segundos entre municípios (padrão: 2)")
    args = parser.parse_args()

    lista = _load_lista(Path(args.lista), args.uf, args.municipio)
    if not lista:
        log.error("Nenhum município encontrado com os filtros informados.")
        sys.exit(1)

    statuses = _load_status()
    log.info("=" * 60)
    log.info("BATCH IVS — %s", _now())
    log.info("Municípios na lista: %d", len(lista))
    log.info("Já processados: %d", sum(1 for s in statuses.values() if s.ok))
    log.info("=" * 60)

    processados = 0
    erros = 0

    for row in lista:
        if args.limit and processados + erros >= args.limit:
            break

        ibge7 = row["ibge7"]
        nome = row["nome"]
        uf = row["uf"]
        slug = row["slug"]

        # Retomar: pular se já OK
        prev = statuses.get(ibge7)
        if prev and prev.ok and not args.force:
            log.info("SKIP %s (%s) — já processado em %s", nome, ibge7, prev.fim)
            continue

        log.info("")
        log.info(">>> Iniciando: %s (%s / %s)", nome, ibge7, uf)
        t0 = time.time()

        st = MunicipioStatus(
            ibge7=ibge7, nome=nome, uf=uf, slug=slug,
            inicio=_now(), etapa="download",
        )
        statuses[ibge7] = st
        _save_status(statuses)

        ok, err = processar_municipio(ibge7, uf, nome, slug, skip_large=args.skip_large)

        st.fim = _now()
        st.duracao_s = round(time.time() - t0, 1)
        st.ok = ok
        st.etapa = "ok" if ok else "erro"
        st.erro = err[:300] if err else ""
        statuses[ibge7] = st
        _save_status(statuses)

        if ok:
            processados += 1
            log.info("<<< OK: %s — %.0fs", nome, st.duracao_s)
        else:
            erros += 1
            log.error("<<< ERRO: %s — %s", nome, err[:150])

        if args.delay > 0:
            time.sleep(args.delay)

    log.info("")
    log.info("=" * 60)
    log.info("BATCH CONCLUÍDO — %s", _now())
    log.info("Processados com sucesso: %d", processados)
    log.info("Erros: %d", erros)
    log.info("Status salvo em: %s", STATUS_CSV)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
