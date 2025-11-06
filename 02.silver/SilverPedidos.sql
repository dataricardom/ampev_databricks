-- Databricks notebook source
WITH estabelecimentos AS (
  SELECT * FROM kpuudata.bronze.estabelecimentos
),
pedidos AS (
SELECT * FROM kpuudata.bronze.pedidos
)

SELECT * FROM estabelecimentos e INNER JOIN pedidos p ON e.EstabelecimentoID = p.EstabelecimentoID
