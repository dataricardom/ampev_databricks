# 🧩 Projeto AMPEV Databricks — Arquitetura Medalhão (Bronze, Silver e Gold)

O objetivo deste projeto é aplicar o modelo de **arquitetura medalhão (Medallion Architecture)** no Databricks para organizar, transformar e analisar dados de vendas, partindo de dados brutos até insights analíticos.

---

## 🔹 Camada Bronze
Nesta etapa, os dados brutos são **lidos diretamente de um arquivo CSV** armazenado em um bucket **S3** e salvos na camada **bronze** do Databricks.  

Além da ingestão dos dados originais, foi criada a coluna **`data_carga`**, que registra **a data e hora em que os dados foram carregados** no ambiente — facilitando o controle de versões e auditoria.

> **Objetivo:** armazenar os dados originais sem transformações, mantendo sua integridade e rastreabilidade.

---

## 🔸 Camada Silver
Na camada **silver**, são realizadas **transformações e padronizações**, como:
- Correção e conversão de tipos de dados das colunas (por exemplo: `string` → `int`, `string` → `decimal`);
- Padronização de nomes de colunas;
- Limpeza de valores inconsistentes;
- Inclusão da coluna **`data_carga`**, herdada ou atualizada para indicar quando os dados foram processados nesta camada.

> **Objetivo:** entregar dados prontos para análise e integração entre diferentes tabelas.

---

## 🟡 Camada Gold
A camada **gold** é responsável por gerar **tabelas analíticas e relatórios agregados** a partir dos dados refinados.  
Nesta etapa, foram desenvolvidas três consultas principais para responder às perguntas de negócio:

### 1️⃣ Quais as 5 empresas que mais compraram produtos?
```sql
SELECT es.local AS estabelecimentos,
       SUM(quantidade_vendida) AS compras_realizadas
FROM kpuudata.silver.silver_estabelecimentos es
JOIN kpuudata.silver.silver_pedidos pe
  ON es.estabelecimento_id = pe.estabelecimento_id
GROUP BY 1
ORDER BY SUM(quantidade_vendida) DESC
LIMIT 5;
