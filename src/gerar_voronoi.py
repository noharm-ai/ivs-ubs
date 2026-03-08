from __future__ import annotations

import argparse
import logging
import os
import shutil
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from shapely.geometry import LineString, MultiPoint, Point
from shapely.ops import split, unary_union, voronoi_diagram

log = logging.getLogger(__name__)

CRS_GEO = "EPSG:4674"
CRS_UTM_22S = "EPSG:32722"



def _download_limite_municipal(municipio_ibge: str, dest: Path, timeout: int = 60) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        return dest

    urls = [
        "https://servicodados.ibge.gov.br/api/v4/malhas/municipios/"
        f"{municipio_ibge}?formato=application/vnd.geo+json&qualidade=maxima",
        "https://servicodados.ibge.gov.br/api/v3/malhas/municipios/"
        f"{municipio_ibge}?formato=application/vnd.geo+json",
    ]
    last_err: Exception | None = None
    for url in urls:
        try:
            with requests.get(url, timeout=timeout) as r:
                r.raise_for_status()
                dest.write_bytes(r.content)
                return dest
        except Exception as e:  # noqa: BLE001
            last_err = e
    if last_err:
        raise last_err
    raise RuntimeError("Não foi possível baixar limite municipal do IBGE")


def _resolve_limite_municipal(base_dir: Path, municipio_ibge: str, dest: Path, slug: str) -> Path:
    """
    Resolve o limite municipal priorizando arquivo local.
    Ordem:
      1) dest já existente
      2) IBGE_MUNICIPIO_GEOJSON_PATH (env)
      3) <repo>/data/{slug}.geojson
      4) <repo>/data/limite_{slug}.geojson
      5) download API IBGE
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        return dest

    env_arg = os.environ.get("IBGE_MUNICIPIO_GEOJSON_PATH", "").strip()
    env_path = Path(env_arg) if env_arg else None
    repo_root = Path(__file__).resolve().parents[1]
    local_candidates = [
        env_path,
        repo_root / "data" / f"{slug}.geojson",
        repo_root / "data" / f"limite_{slug}.geojson",
        base_dir / "data" / f"{slug}.geojson",
        base_dir / "data" / f"limite_{slug}.geojson",
    ]
    seen: set[Path] = set()
    for cand in local_candidates:
        if not cand:
            continue
        cand = cand.resolve()
        if cand in seen:
            continue
        seen.add(cand)
        if cand.exists() and cand.is_file() and cand.stat().st_size > 0:
            shutil.copy2(cand, dest)
            return dest

    # Fallback sem rede: gerar limite municipal a partir dos setores já filtrados
    setores_candidates = [
        base_dir / "data" / "raw" / "ibge_setores" / f"setores_{slug}.geojson",
        base_dir / "data" / "raw" / "ibge_setores" / "setores.geojson",
    ]
    for setores_path in setores_candidates:
        if not setores_path.exists() or not setores_path.is_file():
            continue
        try:
            setores = gpd.read_file(setores_path)
            if setores.empty:
                continue
            limite = gpd.GeoDataFrame(
                geometry=[unary_union(setores.geometry)],
                crs=setores.crs or CRS_GEO,
            )
            limite.to_file(dest, driver="GeoJSON")
            return dest
        except Exception:  # noqa: BLE001
            continue

    return _download_limite_municipal(municipio_ibge, dest)


def generate_voronoi(base_dir: Path, municipio_ibge: str = "4314407", slug: str = "municipio") -> dict:
    """
    Gera territórios Voronoi das UBS para o município configurado.
    """
    pontos_dir = base_dir / "data" / "raw" / "cnes"
    pontos_candidates = [
        pontos_dir / f"ubs_{slug}_pontos.csv",
        pontos_dir / f"{slug}_ubs_pontos.csv",  # compatibilidade com naming anterior
    ]
    pontos_csv = next((p for p in pontos_candidates if p.exists()), pontos_candidates[0])
    limite_file = base_dir / "data" / "raw" / "ibge_setores" / f"limite_{slug}.geojson"
    out_file = base_dir / "data" / "processed" / "territorios_voronoi_ubs.geojson"

    if not pontos_csv.exists():
        listed = ", ".join(str(p) for p in pontos_candidates)
        raise FileNotFoundError(f"Pontos CNES não encontrado. Esperado um destes arquivos: {listed}")

    df = pd.read_csv(pontos_csv, dtype=str)
    required_cols = {"cnes", "nome", "latitude", "longitude"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"CSV sem colunas obrigatórias: {sorted(missing)}")

    for col in ("latitude", "longitude"):
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .str.strip()
        )
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"]).copy()
    df = df.drop_duplicates(subset=["cnes"])

    if len(df) < 2:
        raise ValueError(
            f"Pontos com coordenadas insuficientes para Voronoi: {len(df)} (mínimo 2)."
        )

    pontos = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs=CRS_GEO,
    ).to_crs(CRS_UTM_22S)

    try:
        limite_path = _resolve_limite_municipal(base_dir, municipio_ibge, limite_file, slug=slug)
        limite = gpd.read_file(limite_path).to_crs(CRS_UTM_22S)
        if limite.empty:
            raise ValueError("Limite municipal vazio")
        limite_union = unary_union(limite.geometry)
    except Exception as e:  # noqa: BLE001
        # Fallback final: recorte aproximado usando convex hull dos pontos de UBS.
        # Mantém o pipeline executável em ambientes sem rede/limite municipal.
        log.warning("Limite municipal indisponível (%s). Usando convex hull das UBS.", e)
        limite_union = unary_union(pontos.geometry).convex_hull.buffer(5_000)

    coords = np.array([(geom.x, geom.y) for geom in pontos.geometry])

    # Jitter pontos com coordenadas idênticas (~1 m) para evitar erro qhull QH6154
    rng = np.random.default_rng(seed=42)
    seen: dict[tuple, int] = {}
    for i, (x, y) in enumerate(coords):
        key = (round(x, 1), round(y, 1))
        count = seen.get(key, 0)
        if count > 0:
            angle = rng.uniform(0, 2 * np.pi)
            coords[i] += np.array([np.cos(angle), np.sin(angle)]) * (count * 2.0)
            log.warning(
                "UBS '%s' tem coordenadas duplicadas — aplicando jitter de %dm",
                pontos.iloc[i]["nome"], count * 2,
            )
        seen[key] = count + 1

    if len(coords) == 2:
        # Caso especial: 2 UBS → dividir pelo bisector perpendicular
        p1, p2 = coords[0], coords[1]
        midpoint = (p1 + p2) / 2
        direction = p2 - p1
        perp = np.array([-direction[1], direction[0]])
        perp = perp / np.linalg.norm(perp)
        far = np.ptp(coords) * 100 + 10_000
        bisector = LineString([midpoint - perp * far, midpoint + perp * far])
        try:
            halves = split(limite_union, bisector)
            geoms = list(halves.geoms)
        except Exception:  # noqa: BLE001
            geoms = [limite_union, limite_union]
        polys = [geoms[i] if i < len(geoms) else None for i in range(2)]
    else:
        mp = MultiPoint([Point(x, y) for x, y in coords])
        vor_col = voronoi_diagram(mp, envelope=limite_union)
        polys = []
        for geom in pontos.geometry:
            matched = None
            for region in vor_col.geoms:
                if region.contains(geom):
                    matched = region.intersection(limite_union)
                    break
            if matched is None:
                closest = min(vor_col.geoms, key=lambda r: r.distance(geom))
                matched = closest.intersection(limite_union)
            polys.append(matched)

    gdf = gpd.GeoDataFrame(
        {
            "cnes": pontos["cnes"].values,
            "nome": pontos["nome"].values,
        },
        geometry=polys,
        crs=CRS_UTM_22S,
    )
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    gdf = gdf.to_crs(CRS_GEO)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_file, driver="GeoJSON")

    area_cov = unary_union(gdf.to_crs(CRS_UTM_22S).geometry).area
    area_lim = limite_union.area
    cobertura = area_cov / area_lim if area_lim else 0.0

    return {
        "arquivo": out_file,
        "n_poligonos": len(gdf),
        "n_pontos_entrada": len(df),
        "cobertura_area": cobertura,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera Voronoi das UBS por município")
    parser.add_argument(
        "--base-dir",
        default=str(Path(__file__).resolve().parents[1] / "ivs_municipio"),
        help="Diretório base do projeto (ex.: ivs_betim)",
    )
    parser.add_argument("--municipio", default="4314407", help="Código IBGE do município")
    parser.add_argument("--slug", default="municipio", help="Slug para nomes de arquivos")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")

    res = generate_voronoi(Path(args.base_dir), municipio_ibge=args.municipio, slug=args.slug)
    log.info(
        "Voronoi concluído: %s polígonos; cobertura %.2f%%; arquivo %s",
        res["n_poligonos"],
        res["cobertura_area"] * 100,
        res["arquivo"],
    )


if __name__ == "__main__":
    main()
