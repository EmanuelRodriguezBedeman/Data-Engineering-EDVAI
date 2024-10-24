-- 1. Obtener el promedio de precios por cada categoría de producto.
SELECT c.category_name, p.product_name, p.unit_price,
	AVG(p.unit_price) OVER (PARTITION BY p.category_id) AS avgpricebycategory
FROM products p
LEFT JOIN categories c
ON p.category_id = c.category_id;

-- 2. Obtener el promedio de venta de cada cliente
SELECT 
	AVG(od.unit_price * od.quantity) OVER (PARTITION BY o.customer_id) AS avgorderamount,
	o.order_id,
	o.customer_id,
	od.product_id	
FROM orders o
LEFT JOIN order_details od
ON o.order_id = od.order_id;

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
ORDER BY c.category_name, p.product_name;

-- 4. Selecciona el ID del cliente, la fecha de la orden y la fecha más antigua de la orden para cada cliente de la tabla 'Orders'
SELECT
	o.customer_id,
	o.order_date,
	MIN(o.order_date) OVER (PARTITION BY o.customer_id) AS earliestorderdate
FROM orders o;

-- 5. Seleccione el id de producto, el nombre de producto, el precio unitario, el id de categoría y el precio unitario máximo para cada categoría de la tabla Products.
SELECT 
	p.product_id,
	p.product_name,
	p.unit_price,
	p.category_id,
	MAX(p.unit_price) OVER (PARTITION BY p.category_id) as maxunitprice   
FROM products p;

-- 6. Obtener el ranking de los productos más vendidos
SELECT 
    ROW_NUMBER() OVER (ORDER BY SUM(od.quantity) DESC) AS ranking,
    p.product_name,
    SUM(od.quantity)
FROM order_details od
INNER JOIN products p
ON p.product_id = od.product_id
GROUP BY p.product_name 
ORDER BY SUM(od.quantity) DESC;

-- 7. Asignar numeros de fila para cada cliente, ordenados por customer_id
SELECT 
    ROW_NUMBER() OVER (ORDER BY c.customer_id),
    *
FROM customers c;

-- 8. Obtener el ranking de los empleados más jóvenes () ranking, nombre y apellido del empleado, fecha de nacimiento)
SELECT 
    ROW_NUMBER() OVER (ORDER BY e.birth_date DESC) ranking,
    CONCAT(e.first_name, ' ' ,e.last_name) AS employeename,
    e.birth_date 
FROM employees e;

-- 9. Obtener la suma de venta de cada cliente
SELECT
    SUM(od.quantity * od.unit_price) OVER (PARTITION BY o.customer_id) AS sumorderamount,
    *
FROM orders o
INNER JOIN order_details od
ON o.order_id = od.order_id;

-- 10. Obtener la suma total de ventas por categoría de producto
SELECT 
    c.category_name,
    p.product_name,
    od.unit_price,
    od.quantity,
    SUM(od.unit_price * od.quantity) OVER (PARTITION BY c.category_name) AS totalsales
FROM order_details od
INNER JOIN products p ON od.product_id = p.product_id
INNER JOIN categories c ON p.category_id = c.category_id
ORDER BY c.category_name, p.product_name;

-- 11. Calcular la suma total de gastos de envío por país de destino, luego ordenarlo por país y por orden de manera ascendente
SELECT 
    o.ship_country,
    o.order_id,
    o.shipped_date,
    o.freight,
    SUM(o.freight) OVER (PARTITION BY o.ship_country) AS totalshippingcosts
FROM orders o 
ORDER BY o.ship_country, o.order_id

-- 12. Ranking de ventas por cliente
SELECT 
    c.customer_id,
    c.company_name,
    SUM(od.quantity * od.unit_price),
    RANK() OVER (ORDER BY SUM(od.quantity * od.unit_price) DESC) AS Rank
FROM customers c 
INNER JOIN orders o ON c.customer_id = o.customer_id
INNER JOIN order_details od ON od.order_id = o.order_id
GROUP BY c.customer_id
ORDER BY SUM(od.quantity * od.unit_price) DESC