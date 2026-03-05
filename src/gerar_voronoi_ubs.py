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
