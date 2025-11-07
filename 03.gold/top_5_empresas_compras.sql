-- Databricks notebook source
WITH pedidos AS (
SELECT * FROM kpuudata.silver.silver_pedidos),
estabelecimentos AS (
  SELECT * FROM kpuudata.silver.silver_estabelecimentos
)
SELECT es.local AS estabelecimentos,
SUM(quantidade_vendida) AS compras_realizadas
FROM estabelecimentos es
JOIN pedidos pe
ON es.estabelecimento_id = pe.estabelecimento_id
GROUP BY estabelecimentos
ORDER BY SUM(quantidade_vendida) DESC
LIMIT 5

