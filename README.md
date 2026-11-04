# Pipeline Incremental de Movimentações de Cartão — Cooperativa NovaRota

Desafio técnico — Engenharia de Dados

## 1. Visão geral
## 2. Arquitetura (Landing → Bronze → Silver → Gold)
## 3. Como executar
### 3.1 Execução local com Docker
### 3.2 Execução local sem Docker
### 3.3 Execução no Databricks
## 4. Configuração declarativa (conf/pipeline.yml)
## 5. Decisões técnicas e justificativas
### 5.1 Chaves de negócio e granularidade
### 5.2 Estratégia de deduplicação
### 5.3 Regras de qualidade e severidade
### 5.4 Tratamento de estornos
### 5.5 Carga incremental e idempotência (MERGE)
### 5.6 Particionamento
### 5.7 Schema evolution
### 5.8 Reprocessamento e rastreabilidade
## 6. Massa de testes e defeitos plantados
## 7. Testes automatizados
## 8. Consultas SQL analíticas
## 9. Evidências de execução
## 10. Premissas assumidas
## 11. Limitações conhecidas e próximos passos