# IVSaúde POA — Índice de Vulnerabilidade em Saúde

Replicação e atualização do **IVSaúde** da SMS de Porto Alegre para os
**141 territórios de Unidades Básicas de Saúde (UBS)**, usando dados do
Censo IBGE 2022 e fontes do SUS.

Metodologia: Determinantes Sociais da Saúde (Dahlgren & Whitehead, 1991).

---

## Estrutura do projeto

```
ivs-ubs/
├── data/
│   ├── raw/                 # dados brutos (não versionados)
│   └── processed/           # dados processados por UBS
├── outputs/
│   ├── maps/                # mapas PNG e HTML
│   └── tables/              # tabelas CSV com scores
├── src/
│   ├── download_shapefiles.py   # download automatizado de todas as fontes
│   ├── gerar_voronoi_ubs.py     # territórios Voronoi (fallback CNES)
│   └── calcular_ivs.py          # pipeline completo IVSaúde
├── requirements.txt
└── README.md
```

---

## Instalação

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

---

## Uso

### 1. Baixar os dados

```bash
python src/download_shapefiles.py
```

Para pular arquivos grandes (> 500 MB) em testes:
```bash
python src/download_shapefiles.py --skip-large
```

Para baixar apenas uma fonte:
```bash
python src/download_shapefiles.py --only ibge
# ibge | cnes | ubs | esf | sim | sinasc | sinan | censo_escolar | pbf
```

### 2. Calcular o IVSaúde

Com dados reais:
```bash
python src/calcular_ivs.py
```

Com territórios Voronoi (quando o shapefile oficial não está disponível):
```bash
python src/calcular_ivs.py --voronoi
```

Modo demonstração (dados sintéticos, sem nenhum arquivo externo):
```bash
python src/calcular_ivs.py --modo-demo
```

---

## Dimensões e indicadores

| Dim | Peso | Indicadores |
|-----|------|-------------|
| D1  | 0,50 | % Bolsa Família · % analfabetismo · % pop negra · % óbitos violentos · % área de risco ambiental |
| D2  | 0,30 | % sem saneamento · % sem coleta de lixo · % sem ESF · % evasão EM · % sem creche |
| D3  | 0,10 | Nº entidades comunitárias *(indicador invertido)* |
| D4  | 0,08 | % RN de mães adolescentes |
| D5  | 0,02 | % < 1 ano · % adolescentes · % mulheres em idade fértil · % idosos |

### Padronização

- **Modelo I** (D1, D2, D4, D5): compara com a média de Porto Alegre
  → 0,00 / 0,25 / 0,50 / 0,75 / 1,00
- **Modelo II** (D3 — invertido): compara com média ± DP
  → score final = 1 − score calculado

---

## Obtenção do shapefile das UBS

O maior gargalo é o shapefile oficial dos 141 territórios de UBS.

**Opção A — GeoSampa (recomendada):**
O script `download_shapefiles.py` tenta baixar via WFS automaticamente.
Se falhar, acesse <https://geosampa.prefpoa.com.br>, busque
*"áreas de abrangência UBS"* e exporte como GeoJSON.
Salve em `data/raw/ubs_territorios/territorios_ubs.geojson`.

**Opção B — Voronoi pelo CNES (fallback):**
```bash
python src/download_shapefiles.py --only cnes
python src/gerar_voronoi_ubs.py
python src/calcular_ivs.py --voronoi
```
Gera territórios aproximados (polígonos de Voronoi) a partir dos
pontos geocodificados do CNES. Precisão menor que o shapefile oficial.

---

## Dados para outros municípios

O pipeline foi projetado para ser reutilizável. Para adaptar a outro município:

1. Altere `COD_MUNICIPIO_IBGE` em `download_shapefiles.py`
2. Obtenha o shapefile dos territórios das UBS da Secretaria Municipal de Saúde
3. Execute o pipeline normalmente

---

## Entregáveis

| Arquivo | Descrição |
|---------|-----------|
| `outputs/tables/ivs_poa_resultado_final.csv` | Ranking das 141 UBS com IVS e scores por dimensão |
| `outputs/tables/indicadores_por_ubs.csv` | Valores brutos de cada indicador por UBS |
| `outputs/tables/qualidade_dados.csv` | Metadados de qualidade (fonte, ano, % missing) |
| `outputs/maps/ivs_poa_mapa_interativo.html` | Mapa coroplético interativo (Folium) |
| `outputs/maps/ivs_poa_mapa_estatico.png` | Mapa estático de alta resolução |
| `outputs/maps/ivs_poa_top20_vulneraveis.png` | Top 20 UBS mais vulneráveis |
| `outputs/maps/ivs_poa_top20_menos_vulneraveis.png` | Top 20 UBS menos vulneráveis |
| `outputs/maps/ivs_poa_heatmap_dimensoes.png` | Heatmap UBS × dimensões |

---

## Fontes de dados

| Dado | Fonte | Acesso |
|------|-------|--------|
| Setores censitários / pop | IBGE Censo 2022 | <https://geoftp.ibge.gov.br> |
| Territórios UBS | SMS-POA / GeoSampa | <https://geosampa.prefpoa.com.br> |
| Localização UBS | CNES/DataSUS | <https://cnes.datasus.gov.br> |
| Cobertura ESF | e-Gestor APS | <https://egestorab.saude.gov.br> |
| Bolsa Família | Portal da Transparência | <https://portaldatransparencia.gov.br> |
| Óbitos (SIM) | DataSUS FTP | <ftp://ftp.datasus.gov.br> |
| Nascidos vivos (SINASC) | DataSUS FTP | <ftp://ftp.datasus.gov.br> |
| Sífilis congênita (SINAN) | DataSUS FTP | <ftp://ftp.datasus.gov.br> |
| Matrículas escolares | Censo Escolar INEP | <https://www.gov.br/inep> |
| Entidades OSC | MAPA OSC/IPEA | <https://mapaosc.ipea.gov.br> |

---

## Referência metodológica

> Dahlgren, G. & Whitehead, M. (1991). *Policies and strategies to promote
> social equity in health.* Stockholm: Institute for Future Studies.
>
> SMS Porto Alegre / DGVS — IVSaúde (metodologia original, territórios 2015).
