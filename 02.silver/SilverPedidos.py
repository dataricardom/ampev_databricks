# Databricks notebook source
# MAGIC %sql
# MAGIC WITH estabelecimentos AS (
# MAGIC   SELECT * FROM kpuudata.bronze.estabelecimentos
# MAGIC ),
# MAGIC pedidos AS (
# MAGIC SELECT * FROM kpuudata.bronze.pedidos
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM estabelecimentos e INNER JOIN pedidos p ON e.EstabelecimentoID = p.EstabelecimentoID
