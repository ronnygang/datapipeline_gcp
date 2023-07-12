CREATE TABLE business_layer.b_performance_metrics AS
SELECT
  transaction_country AS country,
  DATE(transaction_datetime) AS date,
  COUNT(DISTINCT transaction_id) AS transaction_count,
  SUM(income) AS total_income,
  SUM(campaign_cost) AS total_campaign_cost,
  AVG(income) AS average_income,
  AVG(campaign_cost) AS average_campaign_cost,
  SUM(income - campaign_cost) AS total_profit,
  SUM(income) / COUNT(DISTINCT transaction_id) AS average_transaction_value
FROM
  master_layer.m_data_model
GROUP BY
  transaction_country,
  DATE(transaction_datetime);

/*

tabla en el business_layer que contendrá campos valiosos y relevantes para calcular los KPI e 
indicadores de rendimiento de la empresa por país y fecha.

En esta tabla de rendimiento, estoy utilizando la tabla master_layer.m_data_model como origen de 
datos y aplicando funciones de agregación para calcular los KPI e indicadores clave de rendimiento. 
Los campos relevantes y valiosos que se incluyen en esta tabla son:

    country: País de la transacción.
    date: Fecha de la transacción.
    transaction_count: Cantidad de transacciones realizadas en ese país y fecha.
    total_income: Total de ingresos generados en ese país y fecha.
    total_campaign_cost: Costo total de las campañas en ese país y fecha.
    average_income: Ingreso promedio por transacción en ese país y fecha.
    average_campaign_cost: Costo promedio de campaña por transacción en ese país y fecha.
    total_profit: Ganancia total (ingresos - costo de campañas) en ese país y fecha.
    average_transaction_value: Valor promedio de la transacción en ese país y fecha.

Estos campos proporcionan una visión valiosa de los indicadores de rendimiento de la empresa por 
país y fecha, lo que permitirá tomar decisiones de negocio informadas.

*/