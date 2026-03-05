# IVSaúde — Índice de Vulnerabilidade em Saúde

Metodologia para cálculo do **Índice de Vulnerabilidade em Saúde (IVS)** por
território de UBS, com base em Determinantes Sociais da Saúde e dados públicos.

Referência conceitual: Dahlgren & Whitehead (1991).

---

## Estrutura do projeto

```
ivs-ubs/
├── src/
│   ├── download_*.py                # download e preparação das fontes
│   ├── gerar_voronoi.py             # geração de territórios Voronoi
│   ├── calcular_ivs_*.py            # cálculo dos indicadores D1-D5
│   └── gerar_pagina_*.py            # geração de index.html (mapa + tabela)
├── <base_dir>/
│   ├── data/raw/                    # dados brutos
│   └── data/processed/              # dados processados e resultados
├── index.html                       # página final
└── README.md
```

---

## Instalação

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

---

## Pipeline de execução

### 1. Baixar e preparar dados

```bash
python src/download_<pipeline>.py
```

Executar apenas uma etapa:

```bash
python src/download_<pipeline>.py --only ibge
```

Opções disponíveis em `--only`:
`cnes`, `voronoi`, `ibge`, `esf`, `sim`, `sinasc`, `sinan`, `censo_escolar`, `pbf`.

### 2. Calcular indicadores por território

```bash
python src/calcular_ivs_<pipeline>.py
```

### 3. Gerar página HTML final

```bash
python src/gerar_pagina_<pipeline>.py
```

Substitua `<pipeline>` pelo sufixo disponível no diretório `src/`.

---

## Metodologia padrão

### Dimensões e pesos

| Dimensão | Peso | Indicadores implementados |
|----------|------|---------------------------|
| D1 — Condição Socioeconômica | 0,20 | `% analfabetismo 15+`, `% população preta+parda` |
| D2 — Habitação e Saneamento | 0,20 | `% sem saneamento adequado`, `% sem coleta de lixo` |
| D3 — Capital Social | 0,20 | `entidades comunitárias por 1.000 hab` *(invertido)* |
| D4 — Saúde do Adolescente | 0,20 | `% população feminina 10-19 anos` *(proxy)* |
| D5 — Perfil Demográfico | 0,20 | `% <1 ano (proxy)`, `% adolescentes`, `% mulheres 10-49`, `% idosos 60+` |

### Etapas de cálculo

1. Agregação setor censitário → território por interseção espacial ponderada por área.
2. Cálculo dos indicadores brutos por território.
3. Normalização min-max por indicador no intervalo `[0,1]`.
4. Inversão de indicadores protetivos (D3).
5. Score por dimensão: média simples dos indicadores da dimensão.
6. IVS parcial: média ponderada das dimensões.

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

IVS parcial:

```text
IVS = D1×0,20 + D2×0,20 + D3×0,20 + D4×0,20 + D5×0,20
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

Saídas:

- `<base_dir>/data/processed/territorios_voronoi_ubs.geojson`
- `<base_dir>/data/processed/ibge_por_ubs.csv`
- `<base_dir>/data/processed/ivs_*.csv`
- `<base_dir>/data/processed/status_downloads.csv`
- `index.html`

---

## Fontes de dados

| Dado | Fonte |
|------|-------|
| Setores censitários e agregados do universo | IBGE Censo 2022 |
| Estabelecimentos e localização de UBS | CNES / DataSUS |
| SIM, SINASC, SINAN | DataSUS |
| Cobertura APS/ESF | e-Gestor APS |
| Benefícios de transferência de renda | Portal da Transparência |
| Matrículas escolares | INEP |
| Entidades comunitárias | OpenStreetMap (Overpass API) |

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
