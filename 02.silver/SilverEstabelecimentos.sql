-- Databricks notebook source
CREATE OR REPLACE TABLE kpuudata.silver.silver_estabelecimentos AS
WITH estabelecimentos AS (
SELECT 
CAST(EstabelecimentoID AS INT) AS estabelecimento_id,
Local as local,
Email as email,
Telefone as telefone,
current_timestamp() - INTERVAL 3 HOURS AS data_carga
FROM kpuudata.bronze.estabelecimentos
)
SELECT * FROM estabelecimentos

