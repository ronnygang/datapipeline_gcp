CREATE TABLE master_layer.m_data_model AS
SELECT
  c.campaign_id,
  c.cost AS campaign_cost,
  t.transaction_id,
  t.income,
  t.country AS transaction_country,
  a.customer_id,
  a.product_id,
  a.quantity,
  a.price,
  a.category,
  t.date_time AS transaction_datetime
FROM
  raw_layer.r_campaigns AS c
JOIN
  raw_layer.r_transactions AS t ON c.country = t.country AND c.date_time = t.date_time
JOIN
  raw_layer.r_additional_data AS a ON t.transaction_id = a.transaction_id;

/*

En este modelo de datos, estoy seleccionando las columnas relevantes de las tres tablas y 
realizando las uniones necesarias utilizando las cláusulas JOIN. El modelo resultante 
incluirá las siguientes columnas:

    campaign_id: Identificador de la campaña.
    campaign_cost: Costo de la campaña.
    transaction_id: Identificador de la transacción.
    income: Ingresos de la transacción.
    transaction_country: País de la transacción.
    customer_id: Identificador del cliente.
    product_id: Identificador del producto.
    quantity: Cantidad de productos comprados.
    price: Precio unitario del producto.
    category: Categoría del producto.
    transaction_datetime: Fecha y hora de la transacción.

Este modelo de datos combina la información de las tres tablas en una sola, 
proporcionando una vista integrada de los datos de campañas, transacciones y datos adicionales. 
De esta manera, podrás realizar análisis y consultas más completas y obtener información valiosa.

*/