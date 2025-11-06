-- Databricks notebook source
CREATE OR REPLACE TABLE kpuudata.silver.silver_pedidos AS
WITH pedidos AS (
SELECT 
CAST(PedidoID AS INT) AS pedido_id,
CAST(EstabelecimentoID AS INT) AS estabelecimento_id,
CAST(quantidade_vendida AS INT) AS quantidade_vendida,
CAST(Preco_Unitario AS DECIMAL(10,2)) AS preco_unitario,
TO_DATE(data_venda, "yyyy-MM-dd") AS data_venda,
current_timestamp() - INTERVAL 3 HOURS AS data_carga
FROM kpuudata.bronze.pedidos
)
SELECT * FROM pedidos

