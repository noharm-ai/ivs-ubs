# Análise de correlação — IVS UBS NoHarm vs. IVS IPEA

## Fonte de referência

**Atlas da Vulnerabilidade Social — IPEA**
Arquivo: `data/basecompletamunicipal/atlasivs_dadosbrutos_pt_v2.xlsx`
Anos disponíveis: 2000 e 2010 (nível municipal).

Script de comparação: `src/comparar_ivs_ipea.py`

---

## Estrutura do IVS IPEA

O IVS IPEA varia entre 0 e 1 (0 = menor vulnerabilidade) e é calculado como
média simples de 3 dimensões com peso 1/3 cada:

| Dimensão | Indicadores |
|---|---|
| Infraestrutura Urbana | % sem água/esgoto adequados, % sem coleta de lixo, % renda ≤ ½SM + deslocamento >1h |
| Capital Humano | Mortalidade infantil, crianças 0–5 e 6–14 fora da escola, mulheres 10–17 com filhos, mães chefes sem fundamental, analfabetismo 15+, domicílios sem fundamental, jovens NEET 15–24 |
| Renda e Trabalho | Renda per capita ≤ ½SM, desemprego 18+, sem fundamental em ocupação informal, dependentes de idosos, trabalho infantil 10–14 |

---

## Diferenças estruturais em relação ao IVS UBS NoHarm

| Aspecto | IVS UBS NoHarm (nosso) | IVS IPEA |
|---|---|---|
| Dados | Censo 2022 | Censo 2010 |
| Dimensões | 5 (D1–D5) | 3 |
| Pesos | D1=50%, D2=30%, D3=10%, D4=8%, D5=2% | 1/3 cada |
| Normalização | Min-max por município | Nacional |
| Indicador de renda | ❌ ausente | ✅ Renda e Trabalho (1/3) |
| Indicador de raça | D1_negros | ❌ ausente |

---

## Resultados — 169 municípios (batch parcial)

### Correlação com IVS IPEA 2010

Três variantes foram testadas:

| Variante | Normalização | Pesos | Pearson r | Spearman ρ | Interpretação |
|---|---|---|---|---|---|
| A) `ivs_medio` original | Por município | SMS-POA | +0.256 *** | +0.204 ** | fraca |
| B) `ivs_global_sms` | Global | SMS-POA | +0.757 *** | +0.786 *** | forte |
| C) `ivs_global_ipea` | Global | 3 × 1/3 | +0.552 *** | +0.548 *** | moderada |

### Correlação por dimensão (global vs. IPEA)

| Nossa dimensão | Dimensão IPEA | Pearson r | Spearman ρ |
|---|---|---|---|
| Infra (sem_saneam + sem_lixo) | Infraestrutura Urbana | +0.344 *** | +0.330 *** |
| CapHum (analf + escola + adol) | Capital Humano | **+0.868 ****** | **+0.882 ****** |
| Social (OSC + idosos) | Renda e Trabalho | −0.415 *** | −0.400 *** |

### Estatísticas descritivas

| Índice | min | max | média | dp |
|---|---|---|---|---|
| A ivs_medio | 0.329 | 0.651 | 0.485 | 0.059 |
| B ivs_global_sms | 0.165 | 0.804 | 0.462 | 0.147 |
| C ivs_global_ipea | 0.291 | 0.702 | 0.485 | 0.094 |
| IPEA 2010 | 0.135 | 0.773 | 0.375 | 0.132 |

---

## Diagnóstico

### 1. A normalização por município apaga variação entre municípios

A variante A (normalização por município) tem correlação fraca (ρ=0.20), mas a
variante B usa **exatamente os mesmos pesos** com normalização global e a
correlação salta para ρ=0.79. Isso mostra que quando normalizamos dentro de cada
município, o `ivs_medio` perde quase toda a variação absoluta entre municípios —
ficando comprimido entre 0.33–0.65 com dp=0.06.

### 2. Capital Humano é muito bem capturado

Nossa proxy de capital humano (analfabetismo + evasão escolar + adolescência) tem
correlação **muito forte** com a dimensão equivalente do IPEA (ρ=0.88). Isso
valida a escolha dos indicadores.

### 3. Dimensão "Social/Renda" correlaciona negativamente com IPEA Renda

Nossa proxy para renda (OSC por habitante + proporção de idosos) correlaciona
**negativamente** com Renda e Trabalho do IPEA (ρ=−0.40). Isso era esperado:
municípios mais ricos têm mais OSCs, e o indicador OSC captura capital social,
não renda. A dimensão D3 (OSC) foi pensada como capital social — não substituí
renda.

### 4. Infraestrutura tem correlação moderada-baixa

Saneamento e coleta de lixo têm correlação moderada com a Infraestrutura IPEA
(ρ=0.33). O IPEA também inclui mobilidade urbana (deslocamento >1h), que não
temos no Censo 2022 setorial.

---

## Conclusão e próximos passos

**Não foi feita mudança no pipeline principal.** A normalização por município
é mantida porque garante que o IVS mostre vulnerabilidade relativa dentro de
cada cidade — útil para gestores municipais identificarem quais UBSs priorizar.

Para comparação entre municípios (ex.: ranking nacional), usar a variante B
(`ivs_global_sms`) gerada pelo script de comparação.

**Principal lacuna para melhorar a correlação com IPEA**: indicador de renda.
Opções prioritárias:
- PBF por CEP (Portal da Transparência API) → D1 renda por território
- Variáveis de renda do Censo 2022 setorial (arquivo `domicilio3`, variáveis
  V00496–V00643, aguardando identificação no dicionário IBGE)
