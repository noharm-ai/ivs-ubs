# IVS UBS NoHarm — Índice de Vulnerabilidade em Saúde

Metodologia para cálculo do **Índice de Vulnerabilidade em Saúde (IVS)** por
território de UBS, com base em Determinantes Sociais da Saúde e dados públicos.

Referência conceitual: Dahlgren & Whitehead (1991).

---

## Estrutura do projeto

```
ivs-ubs/
├── src/
│   ├── download_municipio.py        # download e preparação das fontes por município
│   ├── gerar_voronoi.py             # geração de territórios Voronoi
│   ├── calcular_ivs_municipio.py    # cálculo dos indicadores D1-D5
│   ├── gerar_pagina_municipio.py    # geração de mapa + tabela + JSON.gz
│   ├── gerar_lista_municipios.py    # gera lista de municípios com UBS (batch)
│   ├── batch_ivs.py                 # processa múltiplos municípios em sequência
│   └── data/
│       └── municipios_com_ubs.csv   # lista de municípios para o batch
├── data/
│   ├── BASE_DE_DADOS_CNES_AAAAMM/  # base completa do CNES (download manual, ver abaixo)
│   │   ├── tbEstabelecimento*.csv   # estabelecimentos de saúde (usado pelo batch)
│   │   └── ...
│   ├── Agregados_por_Setores_Censitarios/  # ZIPs IBGE BR (cache compartilhado)
│   ├── <UF>_setores_CD2022/         # shapefile de setores por UF (cache compartilhado)
│   ├── <UF>/
│   │   ├── _cache/
│   │   │   ├── sim/                 # parquets SIM por UF (cache compartilhado)
│   │   │   ├── sinasc/              # parquets SINASC por UF (cache compartilhado)
│   │   │   └── cnes_ubs_<UF>.csv    # UBS filtradas do CNES por UF
│   │   └── ivs_<slug>/              # dados brutos e processados por município
│   │       ├── data/raw/
│   │       └── data/processed/
│   └── _cache/
│       └── censo_escolar/           # ZIP do Censo Escolar (cache compartilhado)
├── docs/                            # GitHub Pages
│   ├── index.html                   # listagem com filtro por UF/região e busca
│   ├── mapa.html                    # mapa + tabela por município (?m=UF/slug)
│   ├── favicon.ico
│   └── data/
│       ├── municipios.json.gz       # manifesto de municípios publicados
│       └── <UF>/
│           └── <slug>.json.gz       # dados IVS por município (gzip)
└── README.md
```

---

## Instalação

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

---

## Pré-requisito: BASE_DE_DADOS_CNES

O pipeline de batch depende da base completa do CNES para identificar quais
municípios possuem UBS cadastrada e suas coordenadas.

**Download manual:**

1. Acesse [cnes.datasus.gov.br](https://cnes.datasus.gov.br) → **Relatórios → Arquivos de Disseminação**
2. Baixe o arquivo `BASE_DE_DADOS_CNES_AAAAMM.zip` (competência mais recente)
3. Extraia na pasta `data/` do projeto:

```
ivs-ubs/data/BASE_DE_DADOS_CNES_202601/
    tbEstabelecimento202601.csv   ← principal (602 mil estabelecimentos)
    tbMunicipio202601.csv
    ...
```

O arquivo `tbEstabelecimento*.csv` é a única tabela obrigatória para o batch.
As demais são usadas opcionalmente pelo `download_municipio.py`.

---

## Pipeline de execução — município único

```bash
source .venv/bin/activate

# 1. Baixar dados do município
python src/download_municipio.py --municipio-ibge 4314407 --uf RS --cidade Pelotas

# 2. Calcular indicadores por território de UBS
python src/calcular_ivs_municipio.py --base-dir data/RS/ivs_pelotas --slug pelotas

# 3. Gerar JSON.gz para o GitHub Pages
python src/gerar_pagina_municipio.py --base-dir data/RS/ivs_pelotas --slug pelotas \
    --cidade Pelotas --uf RS --ibge 4314407 --no-html
```

Os dados do município são armazenados em `data/<UF>/ivs_<slug>/`.
Arquivos de grande porte (ZIPs IBGE, parquets SIM/SINASC, shapefile de setores) são
baixados uma única vez em caches compartilhados por UF ou nacionais (ver estrutura acima).

Executar apenas uma etapa do download:

```bash
python src/download_municipio.py --municipio-ibge 4314407 --uf RS --cidade Pelotas --only ibge
```

Opções de `--only`: `cnes`, `voronoi`, `ibge`, `esf`, `sim`, `sinasc`, `sinan`, `censo_escolar`, `escolas_geo`, `cnpj_osc`, `geocodificar_ceps`, `pbf`.

---

## Pipeline de execução — batch (múltiplos municípios)

### 1. Gerar lista de municípios com UBS

Requer a BASE_DE_DADOS_CNES na pasta `data/`:

```bash
# Brasil completo (~5.567 municípios com UBS)
python src/gerar_lista_municipios.py --cnes-dir data/BASE_DE_DADOS_CNES_202601

# Filtrar por estado
python src/gerar_lista_municipios.py --cnes-dir data/BASE_DE_DADOS_CNES_202601 --uf RS

# Mínimo de UBS por município
python src/gerar_lista_municipios.py --cnes-dir data/BASE_DE_DADOS_CNES_202601 --min-ubs 5
```

Resultado salvo em `src/data/municipios_com_ubs.csv`.

### 2. Executar o batch em background

```bash
# Iniciar em background (retoma automaticamente se interrompido)
nohup python src/batch_ivs.py > batch.log 2>&1 &

# Acompanhar progresso em tempo real
tail -f batch.log

# Filtros úteis
python src/batch_ivs.py --uf RR         # apenas municípios de Roraima
python src/batch_ivs.py --skip-large    # pula IBGE/Censo Escolar (teste rápido)
python src/batch_ivs.py --limit 10      # processa apenas 10 municípios
python src/batch_ivs.py --force         # reprocessa mesmo os já concluídos
```

O batch exibe logs em tempo real (stdout do subprocesso) e salva o status de cada
município em `batch_status.csv`. Se interrompido, retoma do ponto onde parou.

Caches compartilhados por UF evitam re-downloads redundantes: o shapefile de setores,
os parquets SIM/SINASC e o CSV filtrado do CNES são baixados uma única vez por UF.

---

## Metodologia padrão

### Dimensões e pesos

| Dimensão | Peso | Indicadores implementados |
|----------|------|---------------------------|
| D1 — Condição Socioeconômica | 0,50 | `% analfabetismo 15+`, `% população preta+parda` |
| D2 — Habitação e Saneamento | 0,30 | `% sem saneamento adequado`, `% sem coleta de lixo`, `% crianças 0-3 fora da creche`, `% crianças 5-14 fora do fundamental` |
| D3 — Capital Social | 0,10 | `entidades comunitárias por 1.000 hab` *(invertido)* |
| D4 — Saúde do Adolescente | 0,08 | `% população feminina 10-19 anos` *(proxy)* |
| D5 — Perfil Demográfico | 0,02 | `% <1 ano (proxy)`, `% adolescentes`, `% mulheres 10-49`, `% idosos 60+` |

### Etapas de cálculo

1. Agregação setor censitário → território por interseção espacial ponderada por área.
2. Cálculo dos indicadores brutos por território.
3. Normalização min-max por indicador no intervalo `[0,1]`.
4. Inversão de indicadores protetivos (D3).
5. Score por dimensão: média simples dos indicadores da dimensão.
6. IVS: média ponderada das dimensões.

### Fórmulas principais

Agregação espacial:

```text
fração = área_interseção(setor, território) / área_total(setor)
valor_território = Σ(valor_setor × fração)
```

Normalização:

```text
score = (valor - min) / (max - min)
```

IVS:

```text
IVS = D1×0,50 + D2×0,30 + D3×0,10 + D4×0,08 + D5×0,02
```

Classes de vulnerabilidade:

- Baixa: `IVS < 0,33`
- Média: `0,33 <= IVS < 0,66`
- Alta: `IVS >= 0,66`

---

## Entradas e saídas principais

Entradas mínimas esperadas:

- Setores censitários (geometria)
- Agregados do Censo por setor
- Pontos de UBS para geração de Voronoi
- Entidades comunitárias (OSM/Overpass)

Saídas por município:

- `data/<UF>/ivs_<slug>/data/processed/territorios_voronoi_ubs.geojson`
- `data/<UF>/ivs_<slug>/data/processed/ibge_por_ubs.csv`
- `data/<UF>/ivs_<slug>/data/processed/ivs_*.csv`
- `docs/data/<UF>/<slug>.json.gz` — dados para o GitHub Pages
- `docs/data/municipios.json.gz` — manifesto atualizado automaticamente

## GitHub Pages — desenvolvimento local

Os arquivos `.json.gz` são servidos sem `Content-Encoding` no GitHub Pages.
Para desenvolvimento local, use `python -m http.server` a partir da pasta `docs/`:

```bash
cd docs && python -m http.server 8000
# Acesse: http://localhost:8000
```

> **Atenção:** o VSCode Live Preview recomprime automaticamente as respostas via
> `Content-Encoding: gzip`, o que impede a descompressão manual no cliente.
> Use sempre o `http.server` do Python para testes locais.

---

## Fontes de dados

| Dado | Fonte | Acesso |
|------|-------|--------|
| Setores censitários e agregados do universo | IBGE Censo 2022 | Download automático |
| Estabelecimentos e localização de UBS | CNES / DataSUS | Download automático (por município) ou BASE_DE_DADOS_CNES (batch) |
| SIM, SINASC, SINAN | DataSUS | Download automático via pysus |
| Cobertura APS/ESF | e-Gestor APS | Download automático |
| Benefícios de transferência de renda | Portal da Transparência | Download automático (API) |
| Matrículas escolares | INEP | Download automático |
| Entidades comunitárias | OpenStreetMap (Overpass API) | Download automático |
| Entidades comunitárias (OSC) — alternativa | [CNPJ / Receita Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) | Download automático (ZIPs em `arquivos.receitafederal.gov.br/CNPJ/`) |

---

## Limitações conhecidas

- Parte das bases de saúde públicas não possui geocódigo individual.
- Algumas fontes podem estar disponíveis apenas em nível agregado.
- Territórios Voronoi são aproximações quando o limite oficial não é fornecido.
- O IVS gerado nesta versão é parcial e depende da disponibilidade local de dados.

---

## Referência metodológica

> Dahlgren, G. & Whitehead, M. (1991). *Policies and strategies to promote
> social equity in health.* Stockholm: Institute for Future Studies.
