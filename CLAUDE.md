# CLAUDE.md — Contexto técnico do projeto IVSaúde

Arquivo de referência para sessões Claude Code. Contém arquitetura, mapeamento de variáveis,
inventário de dados, limitações conhecidas e convenções do projeto.

---

## Visão geral

Projeto que computa o **Índice de Vulnerabilidade em Saúde (IVSaúde)** por
**território de UBS**, usando dados abertos do Censo IBGE 2022 e outras fontes públicas.
Metodologia baseada no IVSaúde da SMS-POA (2019). Suporta múltiplos municípios via batch.

GitHub Pages: `docs/` → publicado em `noharm-ai.github.io/ivs-ubs`

---

## Estrutura de diretórios

```
ivs-ubs/
├── src/
│   ├── download_municipio.py       # download automatizado (IBGE, CNES, SIM, etc.)
│   ├── calcular_ivs_municipio.py   # IBGE + OSM → ivs_<slug>.csv
│   ├── gerar_pagina_municipio.py   # gera mapa Leaflet + JSON para GitHub Pages
│   ├── gerar_voronoi.py            # gera territorios_voronoi_ubs.geojson
│   ├── gerar_lista_municipios.py   # gera src/data/municipios_com_ubs.csv a partir do CNES local
│   ├── batch_ivs.py                # processa múltiplos municípios (download→IVS→JSON)
│   ├── download_sinasc.py          # download SINASC via pysus
│   ├── download_sim.py             # download SIM via pysus
│   └── data/
│       └── municipios_com_ubs.csv  # lista de municípios para o batch (gerada por gerar_lista_municipios.py)
│
├── data/                           # dados externos (NÃO versionados, exceto docs/data/)
│   └── BASE_DE_DADOS_CNES_AAAAMM/ # base completa CNES (download manual em cnes.datasus.gov.br)
│       ├── tbEstabelecimento*.csv  # 602k estabelecimentos — usado por gerar_lista_municipios.py
│       └── ...
│
├── ivs_<slug>/                     # dados por município (gerado pelo pipeline, não versionado)
│   └── data/
│       ├── raw/
│       │   ├── ibge_universo/      # CSVs do Censo 2022 por setor censitário
│       │   ├── ibge_setores/       # GeoJSON dos setores
│       │   ├── cnes/               # UBS geocodificadas
│       │   ├── esf/                # cobertura APS
│       │   ├── pbf/                # Bolsa Família
│       │   ├── sim/                # SIM (óbitos)
│       │   ├── sinasc/             # SINASC (nascidos vivos)
│       │   ├── sinan/              # SINAN (sífilis congênita)
│       │   ├── censo_escolar/      # matrículas INEP
│       │   ├── cnpj/               # OSC por CNPJ
│       │   └── osc_municipio_osm.json
│       └── processed/
│           ├── territorios_voronoi_ubs.geojson
│           ├── ivs_<slug>.csv
│           └── ibge_por_ubs.csv
│
├── docs/                           # GitHub Pages (versionado)
│   ├── index.html                  # landing page com lista de municípios
│   ├── mapa.html                   # mapa dinâmico por município (?m=slug)
│   ├── caso-controle.html
│   ├── comparacao-ivs-poa.html
│   └── data/
│       ├── municipios.json         # manifesto de municípios publicados
│       └── <slug>.json             # dados IVS + Voronoi por município
│
├── batch_status.csv                # status de cada município processado pelo batch
├── batch_ivs.log                   # log do batch
└── README.md
```

---

## Pipeline de execução

### Município único
```bash
source .venv/bin/activate
python src/download_municipio.py --municipio-ibge 4314407 --uf RS --cidade Pelotas
python src/calcular_ivs_municipio.py --base-dir ivs_pelotas --slug pelotas
python src/gerar_pagina_municipio.py --base-dir ivs_pelotas --slug pelotas \
    --cidade Pelotas --uf RS --ibge 4314407 --no-html
```

### Batch (múltiplos municípios)
```bash
# Pré-requisito: BASE_DE_DADOS_CNES extraída em data/
python src/gerar_lista_municipios.py --cnes-dir data/BASE_DE_DADOS_CNES_202601

# Rodar em background
nohup python src/batch_ivs.py > batch.log 2>&1 &
tail -f batch.log
```

---

## Variáveis IBGE utilizadas (Censo 2022 — Agregados por Setores)

### municipio_basico.csv
| Variável | Descrição |
|----------|-----------|
| `v0001` | Total de moradores do setor |

### municipio_domicilio.csv (dom1)
| Variável | Descrição |
|----------|-----------|
| `V00001` | Domicílios particulares permanentes ocupados (DPPO) |

### municipio_domicilio2.csv (dom2)
| Variável | Descrição |
|----------|-----------|
| `V00309` | DPPO com esgotamento via rede geral ou pluvial |
| `V00310` | DPPO com fossa séptica ligada à rede |
| `V00397` | DPPO com lixo coletado por serviço de limpeza |
| `V00398` | DPPO com lixo depositado em caçamba de serviço de limpeza |

### municipio_alfabetizacao.csv
| Variável | Descrição |
|----------|-----------|
| `V00900` | Pessoas 15+ anos que sabem ler e escrever |
| `V00901` | Pessoas 15+ anos que NÃO sabem ler e escrever |

### municipio_pessoa01.csv
| Variável | Descrição |
|----------|-----------|
| `V01006` | Total de moradores |
| `V01022` | Feminino, 10-14 anos |
| `V01023` | Feminino, 15-19 anos |
| `V01024` | Feminino, 20-24 anos |
| `V01025` | Feminino, 25-29 anos |
| `V01026` | Feminino, 30-39 anos |
| `V01027` | Feminino, 40-49 anos |
| `V01031` | Total, 0-4 anos (proxy para <1 ano) |
| `V01033` | Total, 10-14 anos |
| `V01034` | Total, 15-19 anos |
| `V01040` | Total, 60-69 anos |
| `V01041` | Total, 70+ anos |

### municipio_cor_raca.csv
| Variável | Descrição |
|----------|-----------|
| `V01318` | Pessoas de cor/raça preta |
| `V01320` | Pessoas de cor/raça parda |

---

## Dimensões e indicadores implementados

### D1 — Condição Socioeconômica (peso 50%)
| Indicador | Fórmula | Fonte |
|-----------|---------|-------|
| `D1_analf` | V00901 / (V00900 + V00901) × 100 | IBGE Censo 2022 |
| `D1_negros` | (V01318 + V01320) / V01006 × 100 | IBGE Censo 2022 |

### D2 — Habitação e Saneamento (peso 30%)
| Indicador | Fórmula | Fonte |
|-----------|---------|-------|
| `D2_sem_saneam` | (V00001 − V00309 − V00310) / V00001 × 100 | IBGE Censo 2022 |
| `D2_sem_lixo` | (V00001 − V00397 − V00398) / V00001 × 100 | IBGE Censo 2022 |

### D3 — Capital Social (peso 10%) — INVERSO
| Indicador | Fórmula | Fonte |
|-----------|---------|-------|
| `D3_osc_per1k` | count_osc_no_território / pop_total × 1000 | OpenStreetMap via Overpass API |

- Tipos contados: `amenity=community_centre`, `amenity=social_centre`, `office=ngo`, `office=association`
- 43 pontos georeferenciados; 22 dos 55 territórios com ao menos 1 entidade
- **Normalização invertida**: 1 − normalizar_mm(D3_osc_per1k) → mais OSC = menos vulnerável

### D4 — Saúde do Adolescente (peso 8%)
| Indicador | Fórmula | Fonte |
|-----------|---------|-------|
| `D4_adol_fem` | (V01022 + V01023) / V01006 × 100 | IBGE Censo 2022 |

- Proxy para risco de maternidade adolescente
- SINASC 2022 confirma 7,9% das mães em Pelotas têm ≤19 anos (344/4.369 nascidos)
- Sem CEP individual no DataSUS público → variação espacial vem do Censo

### D5 — Perfil Demográfico (peso 2%)
| Indicador | Fórmula | Fonte |
|-----------|---------|-------|
| `D5_menor1` | (V01031 / 5) / V01006 × 100 | IBGE Censo 2022 |
| `D5_adol` | (V01033 + V01034) / V01006 × 100 | IBGE Censo 2022 |
| `D5_mif` | (V01022+V01023+V01024+V01025+V01026+V01027) / V01006 × 100 | IBGE Censo 2022 |
| `D5_idosos` | (V01040 + V01041) / V01006 × 100 | IBGE Censo 2022 |

---

## Metodologia de cálculo

### 1. Agregação setor → território (spatial join ponderado por área)
```
fração = área_interseção(setor, território) / área_total(setor)
valor_território = Σ(valor_setor × fração)
```
Projeção usada nos cálculos de área: EPSG:32722 (UTM 22S).

### 2. Normalização min-max
```
score = (valor − min) / (max − min)      [0, 1]
```
Aplicada por indicador, dentro do universo de Pelotas.

### 3. Score por dimensão
Média simples dos indicadores normalizados da dimensão.
D3 é invertido: `score_D3 = 1 − normalizar_mm(D3_osc_per1k)`

### 4. IVS
```
IVS = (D1 × 0,50 + D2 × 0,30 + D3 × 0,10 + D4 × 0,08 + D5 × 0,02)
```

### Classes de vulnerabilidade
| Classe | Intervalo |
|--------|-----------|
| Baixa | IVS < 0,33 |
| Média | 0,33 ≤ IVS < 0,66 |
| Alta | IVS ≥ 0,66 |

---

## Inventário de dados disponíveis

### Baixados e integrados ao IVS
| Dado | Arquivo | Status |
|------|---------|--------|
| Setores censitários (geometria) | `ibge_setores/setores_municipio.geojson` | ✅ Integrado |
| Territórios Voronoi 55 UBS | `processed/territorios_voronoi_ubs.geojson` | ✅ Integrado |
| IBGE domicílios 1 e 2 | `ibge_universo/municipio_domicilio*.csv` | ✅ Integrado |
| IBGE alfabetização | `ibge_universo/municipio_alfabetizacao.csv` | ✅ Integrado |
| IBGE pessoa01 (demografia) | `ibge_universo/municipio_pessoa01.csv` | ✅ Integrado |
| IBGE cor/raça | `ibge_universo/municipio_cor_raca.csv` | ✅ Integrado |
| Entidades OSM | `osc_municipio_osm.json` | ✅ Integrado |

### Baixados, não integrados (limitações conhecidas)
| Dado | Arquivo | Limitação | Potencial |
|------|---------|-----------|-----------|
| SINASC 2021-2022 | `sinasc/nascidos_municipio_*.csv` | Sem CEP individual (privacidade DataSUS) | D4 calibrado |
| SIM 2021-2023 | `sim/obitos_municipio_*.csv` | Sem CEP individual | D1 mortes violentas |
| PBF 202312 | `pbf/pbf_municipio_202312.csv` | Apenas agregado municipal (4 linhas) | D1 renda |
| Cobertura ESF | `esf/cobertura-aps-05-03-2026.xlsx` | Apenas nível municipal, sem CNES | D2 cobertura |
| Censo Escolar 2025 | `censo_escolar/Tabela_Matricula_2025.csv` | Falta geocodificação das escolas | D2 evasão/creche |
| SINAN sífilis | `sinan/sifilis_municipio.csv` | Sem CEP | D4 |

### Não baixados — fontes prioritárias
| Dado | Fonte | Indicador alvo |
|------|-------|---------------|
| CRAS/CREAS (SUAS) | `aplicacoes.mds.gov.br/sagi` | D3 (melhor que OSM) |
| PBF com CEP | Portal da Transparência API | D1 renda por território |
| Risco ambiental | CEMADEN + CPRM shapefile RS | D1.5 novo |
| Cobertura ESF por CNES | e-Gestor APS | D2 |
| SISAB/e-SUS por CNES | `sisab.saude.gov.br` | D4 gestantes adolescentes |

---

## APIs e URLs utilizadas

| Serviço | URL | Uso |
|---------|-----|-----|
| IBGE setores | `https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022/` | Geometria dos setores |
| IBGE universo | `https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Resultados_do_Universo/Agregados_por_Setores_Censitarios/` | Dados socioeconômicos |
| CNES API | `https://cnes.datasus.gov.br/services/estabelecimentos-lite?municipio=4314407&tipoUnidade=1` | UBS geocodificadas |
| Overpass API | `https://overpass.kumi.systems/api/interpreter` | Entidades comunitárias OSM |
| DataSUS FTP | `ftp://ftp.datasus.gov.br/dissemin/publicos/` | SIM, SINASC, SINAN |
| Portal Transparência | `https://api.portaldatransparencia.gov.br/api-de-dados/bolsa-familia-por-municipio` | PBF |
| e-Gestor APS | `https://egestorab.saude.gov.br` | Cobertura ESF |
| CEMADEN | `http://www.cemaden.gov.br/mapainterativo/` | Risco ambiental |

---

## Convenções de código

- CRS geográfico: `EPSG:4674` (SIRGAS 2000)
- CRS para cálculo de áreas: `EPSG:32722` (UTM Zona 22S)
- Coluna de ID de UBS: `id_ubs` (= CNES como string)
- Separador CSV IBGE: pode ser `,` ou `;` — `_read_ibge()` detecta automaticamente
- Coluna setor nos CSVs IBGE: normalizada para `CD_setor` (pode vir como `CD_SETOR` ou `setor`)
- Valores ausentes IBGE: `"X"` → convertido para `NaN`
- Python: `.venv/` com Python 3.10

---

## Referências metodológicas

- SMS Porto Alegre / DGVS — IVSaúde (2019), referência para pesos e dimensões
- Dahlgren, G. & Whitehead, M. (1991). *Policies and strategies to promote social equity in health.* Stockholm: Institute for Future Studies.
- Atlas da Vulnerabilidade Social (IVS) — IPEA: `https://ivs.ipea.gov.br`
