TRUNCATE TABLE CUSTOMERS;
TRUNCATE TABLE ORDERS;
TRUNCATE TABLE ORDER_ITEMS;

INSERT INTO customers (full_name, status_code) VALUES ('Alice', 1);
INSERT INTO customers (full_name, status_code) VALUES ('Bob', 2);
COMMIT;

INSERT INTO orders (customer_id, comment_text, amount)
VALUES (13, 'Заказ от Alice', 150);
INSERT INTO orders (customer_id, comment_text, amount)
VALUES (14, 'Заказ от Bob', 250);
COMMIT;

INSERT INTO order_items (order_id, product_name, qty)
VALUES (15, 'Товар A', 2);
INSERT INTO order_items (order_id, product_name, qty)
VALUES (16, 'Товар B', 5);
COMMIT;

UPDATE customers
   SET full_name = 'Alice Updated'
 WHERE customer_id = 13;

INSERT INTO orders (customer_id, comment_text, amount)
VALUES (13, 'Дополнительный заказ', 300);

DELETE FROM order_items
 WHERE order_item_id = 15;

COMMIT;

SELECT * FROM customers;
SELECT * FROM orders;
SELECT * FROM order_items;

BEGIN
  rollback_pkg.rollback(TIMESTAMP '2025-05-03 09:42:17');
END;


BEGIN
  rollback_pkg.rollback(10000);
END;

COMMIT;


BEGIN
  report_pkg.create_report(TIMESTAMP '2025-05-03 09:46:30');
END;


BEGIN
  report_pkg.create_report();
END;