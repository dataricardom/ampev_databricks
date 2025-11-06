-- Databricks notebook source
CREATE OR REPLACE TABLE kpuudata.gold.gold_pedidos_estabelecimentos AS
SELECT p.pedido_id,
       p.estabelecimento_id,
       p.quantidade_vendida,
       p.preco_unitario,
       p.data_venda,
       e.local,
       e.email,
       e.telefone,
       current_timestamp() - INTERVAL 3 HOURS as data_carga
FROM kpuudata.silver.silver_pedidos p
JOIN kpuudata.silver.silver_estabelecimentos e
ON p.estabelecimento_id = e.estabelecimento_id
