-- Databricks notebook source

CREATE OR REPLACE TABLE workspace.sales_lab.gold_daily_sales AS

SELECT
  o.order_date,
  c.segment,
  c.country,
  COUNT(*) AS completed_orders,
  COUNT(DISTINCT o.customer_id) AS purchasing_customers,
  SUM(o.quantity) AS units_sold,
  ROUND(SUM(o.order_amount), 2) AS gross_sales,
  ROUND(AVG(o.order_amount), 2) AS average_order_value

FROM workspace.sales_lab.silver_orders AS o

INNER JOIN workspace.sales_lab.silver_customers AS c
  ON o.customer_id = c.customer_id

WHERE o.order_status = 'completed'

GROUP BY
  o.order_date,
  c.segment,
  c.country;