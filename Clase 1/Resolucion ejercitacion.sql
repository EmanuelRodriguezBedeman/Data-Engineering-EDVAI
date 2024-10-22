-- 1. Obtener una lista de todos los títulos de contacto distintos
SELECT DISTINCT category_name
FROM CATEGORIES C;

-- 2. Obtener una lista de todos los títulos de contacto distintos
SELECT DISTINCT region
FROM CUSTOMERS C;

-- 3. Obtener una lista de todos los títulos de contacto distintos
SELECT DISTINCT CONTACT_TITLE 
FROM CUSTOMERS C;

-- 4. Obtener una lista de todos los títulos de contacto distintos
SELECT *
FROM CUSTOMERS C 
ORDER BY c.country;

-- 5. Obtener una lista de todos los pedidos, ordenados por id del empleado y fecha del pedido
SELECT *
FROM ORDERS O
ORDER BY o.EMPLOYEE_ID, o.ORDER_DATE; 

-- 6. Insertar un nuevo cliente en la tabla Customers
INSERT INTO CUSTOMERS
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
WHERE c.REGION  ISNULL;

-- 9. Obtener Product_Name y Unit_Price de la tabla Products, y si Unit_Price es NULL, use el precio estándar de $10 en su lugar
SELECT p.PRODUCT_NAME, COALESCE(p.UNIT_PRICE, 10) AS price
FROM PRODUCTS p;

-- 10. Obtener el nombre de la empresa, el nombre del contacto y la fecha del pedido de todos los pedidos
SELECT c.COMPANY_NAME, c.CONTACT_NAME, o.ORDER_DATE
FROM CUSTOMERS C 
INNER JOIN ORDERS O
ON c.CUSTOMER_ID = o.CUSTOMER_ID;

-- 11. Obtener la identificación del pedido, el nombre del producto y el descuento de todos los detalles del pedido y productos
SELECT od.ORDER_ID, p.PRODUCT_NAME, od.DISCOUNT
FROM ORDER_DETAILS OD
INNER JOIN PRODUCTS P
ON od.PRODUCT_ID = p.PRODUCT_ID;

-- 12. Obtener el identificador del cliente, el nombre de la compañía, el identificador y la fecha de la orden de todas las órdenes y aquellos clientes que hagan match
SELECT c.CUSTOMER_ID, c.COMPANY_NAME, o.ORDER_ID, o.ORDER_DATE
FROM CUSTOMERS C
LEFT JOIN ORDERS O
ON c.CUSTOMER_ID = o.CUSTOMER_ID;

-- 13. Obtener el identificador del empleados, apellido, identificador de territorio y descripción del territorio de todos los empleados y aquellos que hagan match en territorios
SELECT  e.EMPLOYEE_ID, 
		e.LAST_NAME, 
		t.TERRITORY_ID, 
		t.TERRITORY_DESCRIPTION 
FROM 
	EMPLOYEES e
LEFT JOIN 
	EMPLOYEE_TERRITORIES et ON e.EMPLOYEE_ID = et.EMPLOYEE_ID
LEFT JOIN 
	TERRITORIES t ON et.TERRITORY_ID = t.TERRITORY_ID;
	
-- 14. Obtener el identificador de la orden y el nombre de la empresa de todos las órdenes y aquellos clientes que hagan match
SELECT o.ORDER_ID, c.COMPANY_NAME 
FROM customers c
LEFT JOIN ORDERS o
ON o.CUSTOMER_ID = c.CUSTOMER_ID;

-- 15. Obtener el identificador de la orden, y el nombre de la compañía de todas las órdenes y aquellos clientes que hagan match
SELECT o.ORDER_ID, c.COMPANY_NAME 
FROM ORDERS o
RIGHT JOIN customers c
ON o.CUSTOMER_ID = c.CUSTOMER_ID;

-- 16. Obtener el nombre de la compañía, y la fecha de la orden de todas las órdenes y aquellos transportistas que hagan match. Solamente para aquellas ordenes del año 1996
SELECT s.COMPANY_NAME, o.ORDER_DATE 
FROM ORDERS o
RIGHT JOIN SHIPPERS s
ON o.SHIP_VIA = s.SHIPPER_ID
WHERE EXTRACT(YEAR FROM o.ORDER_DATE) = 1996;

-- 17. Obtener nombre y apellido del empleados y el identificador de territorio, de todos los empleados y aquellos que hagan match o no de employee_territories
SELECT e.FIRST_NAME, e.LAST_NAME, et.TERRITORY_ID 
FROM EMPLOYEES e
FULL OUTER JOIN EMPLOYEE_TERRITORIES et
ON e.EMPLOYEE_ID = et.EMPLOYEE_ID;

-- 18. Obtener el identificador de la orden, precio unitario, cantidad y total de todas las órdenes y aquellas órdenes detalles que hagan match o no
SELECT o.order_id, od.unit_price, od.quantity, od.UNIT_PRICE * od.QUANTITY AS total 
FROM ORDERS o
FULL OUTER JOIN ORDER_DETAILS od
ON o.order_id = od.order_id;

-- 19. Obtener la lista de todos los nombres de los clientes y los nombres de los proveedores
SELECT c.COMPANY_NAME 
FROM CUSTOMERS c
UNION
SELECT s.company_name
FROM SUPPLIERS s

-- 20. Obtener la lista de los nombres de todos los empleados y los nombres de los gerentes de departamento
SELECT e.FIRST_NAME 
FROM EMPLOYEES e
UNION
SELECT e.FIRST_NAME
FROM employees e
WHERE e.title = 'Sales Manager'

-- 21. Obtener los productos del stock que han sido vendidos
SELECT p.PRODUCT_NAME, p.PRODUCT_ID
FROM PRODUCTS p
WHERE p.PRODUCT_ID IN (
	SELECT od.product_id
	FROM ORDER_DETAILS od
	WHERE od.quantity > 0
)

-- 22. Obtener los clientes que han realizado un pedido con destino a Argentina
SELECT c.COMPANY_NAME
FROM customers c
WHERE c.CUSTOMER_ID IN (
	SELECT o.customer_id
	FROM orders o
	WHERE o.ship_country = 'Argentina'
)

-- 23. Obtener el nombre de los productos que nunca han sido pedidos por clientes de Francia
SELECT P.PRODUCT_NAME
FROM PRODUCTS P
WHERE P.PRODUCT_ID NOT IN (
	SELECT OD.product_id
	FROM ORDER_DETAILS OD
	WHERE OD.order_id IN (
		SELECT O.order_id
		FROM ORDERS O 
		WHERE o.ship_country = 'France'
	)
)

-- 24. Obtener la cantidad de productos vendidos por identificador de orden
SELECT OD.ORDER_ID, SUM(OD.QUANTITY) AS sum
FROM ORDER_DETAILS OD 
GROUP BY OD.ORDER_ID 

-- 25. Obtener el promedio de productos en stock por producto
SELECT P.PRODUCT_NAME, AVG(P.UNITS_IN_STOCK) AS Promedio
FROM PRODUCTS P 
GROUP BY P.PRODUCT_NAME 

-- 26. Cantidad de productos en stock por producto, donde haya más de 100 productos en stock
SELECT P.PRODUCT_NAME, SUM(P.UNITS_IN_STOCK) AS STOCK
FROM PRODUCTS P 
GROUP BY P.PRODUCT_NAME 
HAVING SUM(P.UNITS_IN_STOCK) > 100

-- 27. Obtener el promedio de pedidos por cada compañía y solo mostrar aquellas con un promedio de pedidos superior a 10
SELECT c.company_name, avg(o.order_id) AS averageorders
FROM orders o
LEFT JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.company_name
HAVING avg(o.order_id) > 10

-- 28. Obtener el nombre del producto y su categoría, pero muestre "Discontinued" en lugar del nombre de la categoría si el producto ha sido descontinuado
SELECT p.product_name,
CASE
	WHEN p.discontinued = 1 THEN 'Discontinued'
ELSE
	c.category_name	
END AS product_category
FROM products p
LEFT JOIN categories c
ON p.category_id = c.category_id

-- 29. Obtener el nombre del empleado y su título, pero muestre "Gerente de Ventas" en lugar del título si el empleado es un gerente de ventas (Sales Manager)
SELECT e.first_name, e.last_name,
CASE
	WHEN e.title = 'Sales Manager' THEN 'Gerente de Ventas'
ELSE
	e.title
END AS job_title
FROM employees e