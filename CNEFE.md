# Análise CNEFE — Impacto na ponderação setor → território (Porto Alegre)

## Contexto

O CNEFE (Cadastro Nacional de Endereços para Fins Estatísticos) do Censo 2022
contém ~106 milhões de endereços geocodificados, organizados por município em
arquivos ZIP no FTP do IBGE.

A hipótese testada: substituir a ponderação por **fração de área** pela ponderação
por **contagem de endereços CNEFE** na agregação de setores censitários para
territórios Voronoi de UBS melhora a correlação do IVS calculado com a referência
SMS-POA 2019?

---

## Metodologia

### Fração de área (método padrão)

```
fração_área = área_interseção(setor, território) / área_total(setor)
valor_território = Σ(valor_setor × fração_área)
```

### Fração CNEFE (método alternativo)

```
fração_cnefe = n_endereços_do_setor_dentro_do_território / n_endereços_totais_do_setor
valor_território = Σ(valor_setor × fração_cnefe)
```

Apenas endereços com `COD_ESPECIE` em `{"1", "2"}` (domicílios particulares
permanentes e improvisados) foram incluídos.

**Detalhe técnico:** os códigos de setor no CNEFE possuem 16 caracteres com sufixo
literal (ex.: `431440705180052P`), enquanto os shapefiles do IBGE usam 15 dígitos.
A solução foi truncar para `str[:15]` antes do join.

### Cobertura CNEFE em Porto Alegre

| Métrica | Valor |
|---------|-------|
| Setores com ao menos 1 endereço CNEFE alocado a um território | ~83% |
| Endereços filtrados (espécie 1 ou 2) | ~500 mil |
| Setores sem cobertura (fallback para área) | ~17% |

---

## Resultados — Correlação com referência SMS-POA 2019

Comparação realizada sobre Porto Alegre (55 UBS calculadas vs. 140 na referência,
matchadas por similaridade de nome com threshold 0,55 → 48 pares válidos).

### IVS geral

| Método | Pearson r | Spearman ρ | Δ vs. área |
|--------|-----------|------------|------------|
| Fração de área | +0,7515 *** | +0,7540 *** | — |
| Fração CNEFE   | +0,7521 *** | +0,7531 *** | +0,0006 / −0,0009 |

### Por dimensão

| Dim | Peso | Pearson (área) | Pearson (CNEFE) | Δ |
|-----|------|---------------|-----------------|---|
| D1  | 50%  | +0,64 **  | +0,64 **  | ~0 |
| D2  | 30%  | +0,51 **  | +0,52 **  | +0,01 |
| D3  | 10%  | +0,29 *   | +0,29 *   | ~0 |
| D4  | 8%   | +0,18 ns  | +0,18 ns  | ~0 |
| D5  | 2%   | +0,41 **  | +0,41 **  | ~0 |

---

## Conclusão

O CNEFE **não melhora significativamente** a correlação com a referência 2019 em
Porto Alegre (Δr < 0,001). Isso era esperado: em municípios urbanos densos, os
setores censitários são pequenos o suficiente para que fração de área já seja uma
boa aproximação da distribuição populacional.

A principal fonte de divergência do nosso modelo em relação à referência é
**estrutural**, não metodológica:

| Classe IVS | Nosso modelo | Referência 2019 |
|------------|-------------|-----------------|
| Baixa (< 0,33) | 77 UBS | 10 UBS |
| Média (0,33–0,66) | 22 UBS | 75 UBS |
| Alta (≥ 0,66)  | 1 UBS  | 55 UBS |

O modelo atual usa 12 indicadores de 3 fontes (Censo, OSM, Censo Escolar),
enquanto a referência SMS-POA 2019 incorpora indicadores de renda (PBF, BPC),
violência, cobertura ESF e outros — que puxam a distribuição para valores mais
altos de vulnerabilidade.

---

## Decisão

O código CNEFE permanece implementado em `src/calcular_ivs_municipio.py` e
`src/download_municipio.py`, mas **desabilitado por padrão** (`USE_CNEFE = False`
em `main()`). Para reativar, basta alterar essa flag.

A prioridade para melhorar a correlação é **adicionar indicadores faltantes**
(especialmente renda e cobertura ESF), não refinar o método de ponderação espacial.
