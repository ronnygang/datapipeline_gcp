--business_layer.b_product_performance:
CREATE TABLE business_layer.b_product_performance AS
SELECT
  product_id,
  transaction_country AS country,
  DATE(transaction_datetime) AS date,
  COUNT(DISTINCT transaction_id) AS transaction_count,
  SUM(quantity) AS total_quantity,
  SUM(income) AS total_income,
  AVG(income) AS average_income,
  SUM(income - campaign_cost) AS total_profit,
  SUM(income) / COUNT(DISTINCT transaction_id) AS average_transaction_value
FROM
  master_layer.m_data_model
GROUP BY
  product_id,
  transaction_country,
  DATE(transaction_datetime);

/*

En esta tabla de rendimiento del producto, estoy utilizando la tabla master_layer.m_data_model como 
origen de datos y aplicando funciones de agregación para calcular los KPI e indicadores clave de 
rendimiento del producto por país y fecha. Los campos relevantes y valiosos que se incluyen en esta 
tabla son:
    product_id: Identificador del producto.
    country: País de la transacción.
    date: Fecha de la transacción.
    transaction_count: Cantidad de transacciones realizadas para ese producto, país y fecha.
    total_quantity: Cantidad total del producto vendido en ese país y fecha.
    total_income: Total de ingresos generados por ese producto en ese país y fecha.
    average_income: Ingreso promedio por transacción para ese producto, país y fecha.
    total_profit: Ganancia total (ingresos - costo de campañas) generada por ese producto en ese país y fecha.
    average_transaction_value: Valor promedio de la transacción para ese producto, país y fecha.
    
*/