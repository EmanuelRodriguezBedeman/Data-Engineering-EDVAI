# Lista de clientes junto a la cantidad de productos vendidos ordenados de mayor a menor
# (campos customer_id, company_name, productos_vendidos)

SELECT c.customer_id, c.company_name, od.quantity AS productos_vendidos
FROM customers c
INNER JOIN orders o ON o.customer_id = c.customer_id
INNER JOIN order_details od ON od.order_id = o.order_id
ORDER BY od.quantity DESC;

# lista de órdenes junto a qué empresa realizó cada pedido 
# (campos order_id, shipped_date, company_name, phone)

SELECT o.order_id, o.shipped_date, c.company_name, c.phone
FROM orders o
INNER JOIN customers c ON c.customer_id = o.customer_id;

# lista de detalles de órdenes 
# (campos order_id, unit_price, quantity, discount)

SELECT od.order_id, od.unit_price, od.quantity, od.discount
FROM order_details od;