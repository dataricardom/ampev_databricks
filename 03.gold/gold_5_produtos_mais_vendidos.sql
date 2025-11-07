-- Databricks notebook source
WITH produtos AS (
SELECT * FROM kpuudata.silver.silver_pedidos)
SELECT produto,
sum(quantidade_vendida) AS qtde_produtos_vendidos
FROM produtos
GROUP BY produto
ORDER BY sum(quantidade_vendida) DESC
LIMIT 5

