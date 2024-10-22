-- 1. Obtener el promedio de precios por cada categoría de producto.
SELECT c.category_name, p.product_name, p.unit_price,
	AVG(p.unit_price) OVER (PARTITION BY p.category_id) AS avgpricebycategory
FROM products p
LEFT JOIN categories c
ON p.category_id = c.category_id

-- 2. Obtener el promedio de venta de cada cliente
SELECT 
	AVG(od.quantity) OVER (PARTITION BY o.customer_id) AS avgorderamount,
	*
FROM orders o
LEFT JOIN order_details od
ON o.order_id = od.order_id