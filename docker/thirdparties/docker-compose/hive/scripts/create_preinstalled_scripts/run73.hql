CREATE TABLE employees (
    id INT,
    name VARCHAR(100),
    department VARCHAR(100),
    salary DECIMAL(10,2),
    hire_date DATE
);


INSERT INTO employees VALUES
    (1, 'John Doe', 'IT', 75000.00, '2020-01-15'),
    (2, 'Jane Smith', 'HR', 65000.00, '2019-03-20'),
    (3, 'Bob Johnson', 'IT', 80000.00, '2021-05-10'),
    (4, 'Alice Brown', 'Finance', 70000.00, '2020-11-30'),
    (5, 'Charlie Wilson', 'HR', 62000.00, '2022-01-05');



msck repair table employees;
