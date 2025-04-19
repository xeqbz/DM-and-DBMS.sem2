-- Тестовые данные --
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE order_items CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE orders CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE products CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE employee_projects CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE projects CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE employees CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE departments CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE customers CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE departments (
  dept_id   NUMBER PRIMARY KEY,
  dept_name VARCHAR2(100) NOT NULL
);
/

CREATE TABLE employees (
  emp_id   NUMBER PRIMARY KEY,
  emp_name VARCHAR2(100) NOT NULL,
  salary   NUMBER,
  dept_id  NUMBER,
  CONSTRAINT fk_emp_dept FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);
/

CREATE TABLE projects (
  proj_id   NUMBER PRIMARY KEY,
  proj_name VARCHAR2(100) NOT NULL,
  start_date DATE,
  end_date   DATE
);
/

CREATE TABLE employee_projects (
  emp_id NUMBER,
  proj_id NUMBER,
  assignment_date DATE DEFAULT SYSDATE,
  CONSTRAINT pk_emp_proj PRIMARY KEY (emp_id, proj_id),
  CONSTRAINT fk_ep_emp FOREIGN KEY (emp_id) REFERENCES employees(emp_id),
  CONSTRAINT fk_ep_proj FOREIGN KEY (proj_id) REFERENCES projects(proj_id)
);
/

CREATE TABLE customers (
  cust_id   NUMBER PRIMARY KEY,
  cust_name VARCHAR2(100) NOT NULL,
  contact_info VARCHAR2(200)
);
/

CREATE TABLE orders (
  order_id   NUMBER PRIMARY KEY,
  cust_id    NUMBER,
  order_date DATE DEFAULT SYSDATE,
  status     VARCHAR2(50),
  CONSTRAINT fk_order_cust FOREIGN KEY (cust_id) REFERENCES customers(cust_id)
);
/

CREATE TABLE products (
  prod_id   NUMBER PRIMARY KEY,
  prod_name VARCHAR2(100) NOT NULL,
  price     NUMBER
);
/

CREATE TABLE order_items (
  order_id NUMBER,
  prod_id  NUMBER,
  quantity NUMBER,
  CONSTRAINT pk_order_item PRIMARY KEY (order_id, prod_id),
  CONSTRAINT fk_oi_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
  CONSTRAINT fk_oi_prod FOREIGN KEY (prod_id) REFERENCES products(prod_id)
);
/

INSERT INTO departments (dept_id, dept_name) VALUES (10, 'Sales');
INSERT INTO departments (dept_id, dept_name) VALUES (20, 'HR');
INSERT INTO departments (dept_id, dept_name) VALUES (30, 'IT');
COMMIT;
/

INSERT INTO employees (emp_id, emp_name, salary, dept_id) VALUES (1, 'John Doe', 1500, 10);
INSERT INTO employees (emp_id, emp_name, salary, dept_id) VALUES (2, 'Jane Smith', 900, 20);
INSERT INTO employees (emp_id, emp_name, salary, dept_id) VALUES (3, 'Alice Brown', 2000, 30);
INSERT INTO employees (emp_id, emp_name, salary, dept_id) VALUES (4, 'Bob Johnson', 1200, 10);
COMMIT;
/

INSERT INTO projects (proj_id, proj_name, start_date, end_date)
  VALUES (101, 'Project A', TO_DATE('2025-01-01','YYYY-MM-DD'), TO_DATE('2025-06-30','YYYY-MM-DD'));
INSERT INTO projects (proj_id, proj_name, start_date, end_date)
  VALUES (102, 'Project B', TO_DATE('2025-03-01','YYYY-MM-DD'), TO_DATE('2025-09-30','YYYY-MM-DD'));
COMMIT;
/

INSERT INTO employee_projects (emp_id, proj_id, assignment_date) VALUES (1, 101, SYSDATE);
INSERT INTO employee_projects (emp_id, proj_id, assignment_date) VALUES (3, 101, SYSDATE);
INSERT INTO employee_projects (emp_id, proj_id, assignment_date) VALUES (4, 102, SYSDATE);
COMMIT;
/

INSERT INTO customers (cust_id, cust_name, contact_info) VALUES (1001, 'Acme Corp', 'acme@example.com');
INSERT INTO customers (cust_id, cust_name, contact_info) VALUES (1002, 'Globex Inc', 'globex@example.com');
COMMIT;
/

INSERT INTO orders (order_id, cust_id, order_date, status) VALUES (5001, 1001, SYSDATE, 'New');
INSERT INTO orders (order_id, cust_id, order_date, status) VALUES (5002, 1002, SYSDATE, 'Processing');
COMMIT;
/

INSERT INTO products (prod_id, prod_name, price) VALUES (2001, 'Product X', 99.99);
INSERT INTO products (prod_id, prod_name, price) VALUES (2002, 'Product Y', 149.99);
COMMIT;
/

INSERT INTO order_items (order_id, prod_id, quantity) VALUES (5001, 2001, 2);
INSERT INTO order_items (order_id, prod_id, quantity) VALUES (5001, 2002, 1);
INSERT INTO order_items (order_id, prod_id, quantity) VALUES (5002, 2001, 5);
COMMIT;
/