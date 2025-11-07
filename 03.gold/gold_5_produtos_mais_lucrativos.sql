-- Databricks notebook source
WITH produtos AS (
SELECT * FROM kpuudata.silver.silver_pedidos)
SELECT produto,
sum(total_vendido) AS produtos_maior_faturamento
FROM produtos
GROUP BY produto
ORDER BY sum(total_vendido) DESC
LIMIT 5


