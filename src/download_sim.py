from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

log = logging.getLogger(__name__)


def _to_dataframe(obj) -> pd.DataFrame:
    if isinstance(obj, pd.DataFrame):
        return obj
    if isinstance(obj, (list, tuple)):
        dfs = [x for x in obj if isinstance(x, pd.DataFrame)]
        if dfs:
            return pd.concat(dfs, ignore_index=True)
    if hasattr(obj, "to_dataframe"):
        return obj.to_dataframe()
    raise TypeError(f"Tipo de retorno não suportado: {type(obj)}")


def _filter_by_municipio(df: pd.DataFrame, col_candidates: Iterable[str], municipio: str) -> pd.DataFrame:
    col = next((c for c in col_candidates if c in df.columns), None)
    if not col:
        raise KeyError(f"Coluna de município não encontrada. Colunas: {list(df.columns)[:20]}")
    mun6 = municipio[:6]
    vals = df[col].astype(str).str.strip()
    mask = vals.str.startswith(municipio) | vals.str.startswith(mun6)
    return df.loc[mask].copy()


def download_sim_municipio(
    base_dir: Path,
    years: tuple[int, ...] = (2021, 2022, 2023),
    municipio: str = "4314407",
    uf: str = "RS",
    file_prefix: str = "obitos",
) -> list[dict]:
    """
    Baixa SIM por ano e filtra município.
    Retorna lista de resultados por ano.
    """
    out_dir = base_dir / "data" / "raw" / "sim"
    out_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    # Carrega índice FTP do pysus uma única vez para todos os anos
    _sim_db = None

    for year in years:
        out_csv = out_dir / f"{file_prefix}_{year}.csv"
        # Pula apenas se o arquivo já tem dados (n > 0)
        if out_csv.exists() and out_csv.stat().st_size > 0:
            try:
                n = max(sum(1 for _ in out_csv.open("r", encoding="utf-8")) - 1, 0)
            except OSError:
                n = 0
            if n > 0:
                results.append({"year": year, "status": "OK", "n_registros": n,
                                 "arquivo": out_csv, "obs": "arquivo existente"})
                continue

        df_filtrado: pd.DataFrame | None = None
        obs = ""

        try:
            from pysus import SIM as _SIM  # type: ignore  # nova API pysus ≥1.0

            if _sim_db is None:
                _sim_db = _SIM()
                _sim_db.load()
            files = _sim_db.get_files("CID10", uf=uf, year=year)
            if not files:
                raise FileNotFoundError(f"SIM CID10 {uf} {year} não encontrado no FTP")
            data = files[0].download(local_dir=str(out_dir))
            df = data.to_dataframe()
            df_filtrado = _filter_by_municipio(df, ("CODMUNRES", "codmunres"), municipio)
            obs = "via pysus"
        except Exception as e:  # noqa: BLE001
            obs = f"pysus falhou ({e})"

        if df_filtrado is not None and not df_filtrado.empty:
            df_filtrado.to_csv(out_csv, index=False)
            n = len(df_filtrado)
            status = "OK"
        else:
            pd.DataFrame(columns=["CODMUNRES", "DTOBITO", "CAUSABAS"]).to_csv(out_csv, index=False)
            n = 0
            status = "MANUAL"

        results.append({"year": year, "status": status, "n_registros": n, "arquivo": out_csv, "obs": obs})

    return results


def download_sim_pelotas(
    base_dir: Path,
    years: tuple[int, ...] = (2021, 2022, 2023),
    municipio: str = "4314407",
) -> list[dict]:
    """Compatibilidade retroativa."""
    return download_sim_municipio(
        base_dir=base_dir,
        years=years,
        municipio=municipio,
        uf="RS",
        file_prefix="obitos_pelotas",
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
    base = Path(__file__).resolve().parents[1] / "ivs_pelotas"
    res = download_sim_municipio(base_dir=base, file_prefix="obitos_pelotas")
    for row in res:
        log.info("SIM %s: %s (%s) - %s", row["year"], row["n_registros"], row["status"], row["arquivo"])
