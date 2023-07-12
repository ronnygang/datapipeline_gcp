--Tabla de KPI de Rentabilidad por Producto y País:
CREATE TABLE business_layer.b_profitability_kpi AS
SELECT
  product_id,
  transaction_country AS country,
  SUM(quantity) AS total_quantity,
  SUM(income) AS total_income,
  AVG(income) AS average_income,
  SUM(income - campaign_cost) AS total_profit
FROM
  master_layer.m_data_model
GROUP BY
  product_id,
  transaction_country;

/*

Tabla de KPI de Rentabilidad por Producto y País (business_layer.b_profitability_kpi):

Propósito: 
    Esta tabla proporciona métricas relacionadas con la rentabilidad de los productos por país, 
    lo que permite evaluar la rentabilidad de los productos en diferentes mercados.

Campos:
    product_id: El identificador del producto.
    country: El país donde se realizó la transacción.
    total_quantity: La cantidad total del producto vendido en ese país.
    total_income: El ingreso total generado por ese producto en ese país.
    average_income: El ingreso promedio por transacción para ese producto y país.
    total_profit: La ganancia total (ingresos - costo de campañas) generada por ese producto en ese país.
    
*/