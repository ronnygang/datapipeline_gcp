--Tabla de KPI de Retención de Clientes por País y Fecha:
CREATE TABLE business_layer.b_customer_retention AS
SELECT
  transaction_country AS country,
  DATE(transaction_datetime) AS date,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT CASE WHEN income > 0 THEN customer_id ELSE NULL END) AS paying_customers,
  COUNT(DISTINCT CASE WHEN income = 0 THEN customer_id ELSE NULL END) AS non_paying_customers
FROM
  master_layer.m_data_model
GROUP BY
  transaction_country,
  DATE(transaction_datetime);

/*

Tabla de KPI de Retención de Clientes por País y Fecha (business_layer.b_customer_retention):

Propósito: 
    Esta tabla proporciona métricas relacionadas con la retención de clientes por país y fecha, 
    lo que permite evaluar la lealtad de los clientes y su comportamiento de pago.
Campos:
    country: El país donde se realizó la transacción.
    date: La fecha de la transacción.
    unique_customers: El número total de clientes únicos en ese país y fecha.
    paying_customers: El número de clientes que realizaron una transacción y generaron ingresos en ese país y fecha.
    non_paying_customers: El número de clientes que realizaron una transacción pero no generaron ingresos en ese país y fecha.

*/