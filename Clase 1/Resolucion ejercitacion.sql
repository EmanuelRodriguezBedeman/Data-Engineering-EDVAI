-- 1. Obtener una lista de todas las categorías distintas
SELECT DISTINCT c.category_name
FROM categories c;

-- 2. Obtener una lista de todas las regiones distintas para los clientes
SELECT DISTINCT c.region
FROM customers c;

-- 3. Obtener una lista de todos los títulos de contacto distintos
SELECT DISTINCT c.contact_title
FROM customers c;

-- 4. Obtener una lista de todos los clientes, ordenados por país
SELECT *
FROM customers c
ORDER BY c.country;

-- 5. Obtener una lista de todos los pedidos, ordenados por id del empleado y fecha del pedido:
SELECT *
FROM orders o
ORDER BY o.employee_id, o.order_date; 

-- 6. Insertar un nuevo cliente en la tabla Customers
INSERT INTO customers
VALUES (
	'GRLA',
	'Grela company',
	'Juan Grela',
	'Jefe',
	'Paseo de la Grela 2491',
	'Grelia',
	NULL,
	666,
	'Grelolandia',
	'666-666-666',
	NULL
);

-- 7. Insertar una nueva region en la tabla Region
INSERT INTO region
VALUES (6, 'Northeast');

-- 8. Obtener todos los clientes de la tabla Customers donde el campo Región es NULL
SELECT *
FROM customers c
WHERE c.region  ISNULL;

-- 9. Obtener Product_Name y Unit_Price de la tabla Products, y si Unit_Price es NULL, use el precio estándar de $10 en su lugar
SELECT p.product_name, COALESCE(p.unit_price, 10) AS price
FROM products p;

-- 10. Obtener el nombre de la empresa, el nombre del contacto y la fecha del pedido de todos los pedidos
SELECT c.company_name, c.contact_name, o.order_date
FROM customers c
INNER JOIN ORDERS O
ON c.customer_id = o.customer_id;

-- 11. Obtener la identificación del pedido, el nombre del producto y el descuento de todos los detalles del pedido y productos
SELECT od.order_id, p.product_name, od.discount
FROM order_details od
INNER JOIN products p
ON od.product_id = p.product_id;

-- 12. Obtener el identificador del cliente, el nombre de la compañía, el identificador y la fecha de la orden de todas las órdenes y aquellos clientes que hagan match
SELECT c.customer_id, c.company_name, o.order_id, o.order_date
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id;

-- 13. Obtener el identificador del empleados, apellido, identificador de territorio y descripción del territorio de todos los empleados y aquellos que hagan match en territorios
SELECT  e.employee_id,
		e.last_name,
		t.territory_id,
		t.territory_description
FROM 
	employees e
LEFT JOIN 
	employee_territories et ON e.employee_id = et.employee_id
LEFT JOIN 
	territories t ON et.territory_id = t.territory_id;
	
-- 14. Obtener el identificador de la orden y el nombre de la empresa de todos las órdenes y aquellos clientes que hagan match
SELECT o.order_id, c.company_name 
FROM customers c
LEFT JOIN orders o
ON o.customer_id = c.customer_id;

-- 15. Obtener el identificador de la orden, y el nombre de la compañía de todas las órdenes y aquellos clientes que hagan match
SELECT o.order_id, c.company_name 
FROM orders o
RIGHT JOIN customers c
ON o.customer_id = c.customer_id;

-- 16. Obtener el nombre de la compañía, y la fecha de la orden de todas las órdenes y aquellos transportistas que hagan match. Solamente para aquellas ordenes del año 1996
SELECT s.company_name, o.order_date 
FROM orders o
RIGHT JOIN shippers s
ON o.ship_via = s.shipper_id
WHERE EXTRACT(YEAR FROM o.order_date) = 1996;

-- 17. Obtener nombre y apellido del empleados y el identificador de territorio, de todos los empleados y aquellos que hagan match o no de employee_territories
SELECT e.first_name, e.last_name, et.territory_id 
FROM employees e
FULL OUTER JOIN employee_territories et
ON e.employee_id = et.employee_id;

-- 18. Obtener el identificador de la orden, precio unitario, cantidad y total de todas las órdenes y aquellas órdenes detalles que hagan match o no
SELECT o.order_id, od.unit_price, od.quantity, od.unit_price * od.quantity AS total 
FROM orders o
FULL OUTER JOIN order_details od
ON o.order_id = od.order_id;

-- 19. Obtener la lista de todos los nombres de los clientes y los nombres de los proveedores
SELECT c.company_name
FROM customers c
UNION
SELECT s.company_name
FROM suppliers s;

-- 20. Obtener la lista de los nombres de todos los empleados y los nombres de los gerentes de departamento
SELECT e.first_name
FROM employees e
UNION
SELECT e.first_name
FROM employees e
WHERE e.title = 'Sales Manager';

-- 21. Obtener los productos del stock que han sido vendidos
SELECT p.product_name, p.product_id
FROM products p
WHERE p.product_id IN (
	SELECT od.product_id
	FROM order_details od
	WHERE od.quantity > 0
);

-- 22. Obtener los clientes que han realizado un pedido con destino a Argentina
SELECT c.company_name
FROM customers c
WHERE c.customer_id IN (
	SELECT o.customer_id
	FROM orders o
	WHERE o.ship_country = 'Argentina'
);

-- 23. Obtener el nombre de los productos que nunca han sido pedidos por clientes de Francia
SELECT P.product_name
FROM products p
WHERE P.product_id NOT IN (
	SELECT od.product_id
	FROM order_details od
	WHERE od.order_id IN (
		SELECT O.order_id
		FROM orders o
		WHERE o.ship_country = 'France'
	)
);

-- 24. Obtener la cantidad de productos vendidos por identificador de orden
SELECT od.order_id, SUM(od.quantity) AS sum
FROM order_details od
GROUP BY od.order_id;

-- 25. Obtener el promedio de productos en stock por producto
SELECT p.product_name, AVG(p.units_in_stock) AS Promedio
FROM products p
GROUP BY p.product_name;

-- 26. Cantidad de productos en stock por producto, donde haya más de 100 productos en stock
SELECT p.product_name, SUM(p.units_in_stock) AS STOCK
FROM products p
GROUP BY p.product_name
HAVING SUM(p.units_in_stock) > 100;

-- 27. Obtener el promedio de pedidos por cada compañía y solo mostrar aquellas con un promedio de pedidos superior a 10
SELECT c.company_name, avg(o.order_id) AS averageorders
FROM orders o
LEFT JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.company_name
HAVING AVG(o.order_id) > 10;

-- 28. Obtener el nombre del producto y su categoría, pero muestre "Discontinued" en lugar del nombre de la categoría si el producto ha sido descontinuado
SELECT p.product_name,
CASE
	WHEN p.discontinued = 1 THEN 'Discontinued'
ELSE
	c.category_name	
END AS product_category
FROM products p
LEFT JOIN categories c
ON p.category_id = c.category_id;

-- 29. Obtener el nombre del empleado y su título, pero muestre "Gerente de Ventas" en lugar del título si el empleado es un gerente de ventas (Sales Manager)
SELECT e.first_name, e.last_name,
CASE
	WHEN e.title = 'Sales Manager' THEN 'Gerente de Ventas'
ELSE
	e.title
END AS job_title
FROM employees e;