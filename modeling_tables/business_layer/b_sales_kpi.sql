--Tabla de KPI de Ventas por País y Fecha:
CREATE TABLE business_layer.b_sales_kpi AS
SELECT
  transaction_country AS country,
  DATE(transaction_datetime) AS date,
  COUNT(DISTINCT transaction_id) AS transaction_count,
  SUM(income) AS total_income,
  AVG(income) AS average_income,
  SUM(income - campaign_cost) AS total_profit
FROM
  master_layer.m_data_model
GROUP BY
  transaction_country,
  DATE(transaction_datetime);

/*

Tabla de KPI de Ventas por País y Fecha (business_layer.b_sales_kpi):

Propósito: 
    Esta tabla proporciona métricas clave relacionadas con las ventas por país y fecha, 
    lo que permite analizar el rendimiento de las ventas a lo largo del tiempo y en diferentes países.

Campos:
    country: El país donde se realizó la transacción.
    date: La fecha de la transacción.
    transaction_count: La cantidad total de transacciones realizadas en ese país y fecha.
    total_income: El ingreso total generado en ese país y fecha.
    average_income: El ingreso promedio por transacción en ese país y fecha.
    total_profit: La ganancia total (ingresos - costo de campañas) generada en ese país y fecha.

*/