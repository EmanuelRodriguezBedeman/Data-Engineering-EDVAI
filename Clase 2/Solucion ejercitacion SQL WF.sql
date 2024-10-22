-- 1. Obtener el promedio de precios por cada categoría de producto.
SELECT c.category_name, p.product_name, p.unit_price,
	AVG(p.unit_price) OVER (PARTITION BY p.category_id) AS avgpricebycategory
FROM products p
LEFT JOIN categories c
ON p.category_id = c.category_id

-- 2. Obtener el promedio de venta de cada cliente
SELECT 
	AVG(od.unit_price * od.quantity) OVER (PARTITION BY o.customer_id) AS avgorderamount,
	o.order_id,
	o.customer_id,
	od.product_id	
FROM orders o
LEFT JOIN order_details od
ON o.order_id = od.order_id

-- 3. Obtener el promedio de cantidad de productos vendidos por categoría y ordenarlo por nombre de la categoría y nombre del producto
SELECT 
	p.product_name,
	c.category_name,
	p.quantity_per_unit,
	od.unit_price,
	od.quantity,
	AVG(od.quantity) OVER (PARTITION BY c.category_id) AS avgquantity
FROM products p
INNER JOIN categories c ON p.category_id = c.category_id
INNER JOIN order_details od ON od.product_id = p.product_id
ORDER BY c.category_name, p.product_name

-- 4. Selecciona el ID del cliente, la fecha de la orden y la fecha más antigua de la orden para cada cliente de la tabla 'Orders'
SELECT
	o.customer_id,
	o.order_date,
	MIN(o.order_date) OVER (PARTITION BY o.customer_id)
FROM orders o

-- 5. Seleccione el id de producto, el nombre de producto, el precio unitario, el id de categoría y el precio unitario máximo para cada categoría de la tabla Products.
SELECT 
	p.product_id,
	p.product_name,
	p.unit_price,
	p.category_id,
	MAX(p.unit_price) OVER (PARTITION BY p.category_id)
FROM products p