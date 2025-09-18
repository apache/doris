create database if not exists demo.test_db;
use demo.test_db;


-- Create the initial Iceberg table
CREATE TABLE iceberg_full_schema_change_parquet (
    id int,
    map_column map < string,
    struct < name: string,
    age: int > >,
    struct_column struct < city: string,
    population: int >,
    array_column array < struct < product: string,
    price: float > >
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        0,
        map('person0', struct('zero', 2)),
        struct('cn', 1000000),
        array(struct('Apple', 1.99), struct('Banana', 0.99))
    );

-- Schema Change 1: Add 'address' string to map_column's struct
ALTER TABLE
    iceberg_full_schema_change_parquet
ADD
    COLUMN map_column.value.address string;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        1,
        map('person1', struct('Alice', 25, '123 Main St')),
        struct('New York', 8000000),
        array(struct('Apple', 1.99), struct('Banana', 0.99))
    );

-- Schema Change 2: Rename 'city' to 'location' in struct_column
ALTER TABLE
    iceberg_full_schema_change_parquet RENAME COLUMN struct_column.city TO location;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        2,
        map('person2', struct('Bob', 30, '456 Elm St')),
        struct('Los Angeles', 4000000),
        array(struct('Orange', 2.49), struct('Grape', 3.99))
    );

-- Schema Change 3: Delete 'price' from array_column's struct
ALTER TABLE
    iceberg_full_schema_change_parquet DROP COLUMN array_column.element.price;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        3,
        map('person3', struct('Charlie', 28, '789 Oak St')),
        struct('Chicago', 2700000),
        array(struct('Pear'), struct('Mango'))
    );

ALTER TABLE
    iceberg_full_schema_change_parquet
ALTER COLUMN
    map_column.value.age first;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        4,
        map('person4', struct(35, 'David', '101 Pine St')),
        struct('Houston', 2300000),
        array(struct('Kiwi'), struct('Pineapple'))
    );

-- Schema Change 5: Add 'country' string to struct_column
ALTER TABLE
    iceberg_full_schema_change_parquet
ADD
    COLUMN struct_column.country string;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        5,
        map('person5', struct(40, 'Eve', '202 Birch St')),
        struct('Phoenix', 1600000, 'USA'),
        array(struct('Lemon'), struct('Lime'))
    );

-- Schema Change 6: Rename 'product' to 'item' in array_column's struct
ALTER TABLE
    iceberg_full_schema_change_parquet RENAME COLUMN array_column.element.product TO item;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        6,
        map('person6', struct(22, 'Frank', '303 Cedar St')),
        struct('Philadelphia', 1500000, 'USA'),
        array(struct('Watermelon'), struct('Strawberry'))
    );

-- Schema Change 7: Delete 'address' from map_column's struct
ALTER TABLE
    iceberg_full_schema_change_parquet DROP COLUMN map_column.value.address;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        7,
        map('person7', struct(27, 'Grace')),
        struct('San Antonio', 1400000, 'USA'),
        array(struct('Blueberry'), struct('Raspberry'))
    );

-- Schema Change 8: Add 'quantity' int to array_column's struct
ALTER TABLE
    iceberg_full_schema_change_parquet
ADD
    COLUMN array_column.element.quantity int;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        8,
        map('person8', struct(32, 'Hank')),
        struct('San Diego', 1300000, 'USA'),
        array(struct('Cherry', 5), struct('Plum', 3))
    );

-- Schema Change 9: Swap 'location' and 'country' in struct_column
ALTER TABLE
    iceberg_full_schema_change_parquet
ALTER COLUMN
    struct_column.location
after
    country;

ALTER TABLE
    iceberg_full_schema_change_parquet
ALTER COLUMN
    struct_column.country first;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        9,
        map('person9', struct(29, 'Ivy')),
        struct('USA', 1200000, 'Dallas'),
        array(struct('Peach', 4), struct('Apricot', 2))
    );

-- Schema Change 10: Rename 'name' to 'full_name' in map_column's struct
ALTER TABLE
    iceberg_full_schema_change_parquet RENAME COLUMN map_column.value.name TO full_name;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        10,
        map('person10', struct(26, 'Jack')),
        struct('USA', 1100000, 'Austin'),
        array(struct('Fig', 6), struct('Date', 7))
    );

-- Schema Change 11: Add 'gender' string to map_column's struct
ALTER TABLE
    iceberg_full_schema_change_parquet
ADD
    COLUMN map_column.value.gender string;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        11,
        map('person11', struct(31, 'Karen', 'Female')),
        struct('USA', 1000000, 'Seattle'),
        array(struct('Coconut', 1), struct('Papaya', 2))
    );

-- Schema Change 12: Delete 'population' from struct_column
ALTER TABLE
    iceberg_full_schema_change_parquet DROP COLUMN struct_column.population;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        12,
        map('person12', struct(24, 'Leo', 'Male')),
        struct('USA', 'Portland'),
        array(struct('Guava', 3), struct('Lychee', 4))
    );

-- Schema Change 13: Add 'category' string to array_column's struct
ALTER TABLE
    iceberg_full_schema_change_parquet
ADD
    COLUMN array_column.element.category string;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        13,
        map('person13', struct(33, 'Mona', 'Female')),
        struct('USA', 'Denver'),
        array(
            struct('Avocado', 2, 'Fruit'),
            struct('Tomato', 5, 'Vegetable')
        )
    );

-- Schema Change 14: Rename 'location' to 'city' in struct_column
ALTER TABLE
    iceberg_full_schema_change_parquet RENAME COLUMN struct_column.location TO city;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        14,
        map('person14', struct(28, 'Nina', 'Female')),
        struct('USA', 'Miami'),
        array(
            struct('Cucumber', 6, 'Vegetable'),
            struct('Carrot', 7, 'Vegetable')
        )
    );

alter table
    iceberg_full_schema_change_parquet RENAME COLUMN map_column to new_map_column;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        15,
        map('person15', struct(30, 'Emma Smith', 'Female')),
        struct('USA', 'New York'),
        array(
            struct('Banana', 3, 'Fruit'),
            struct('Potato', 8, 'Vegetable')
        )
    );

alter table
    iceberg_full_schema_change_parquet
ADD
    COLUMN new_struct_column struct < a: struct < aa :int,
    bb :string >,
    b: struct < cc :string,
    dd :int >,
    c int >;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        16,
        map('person16', struct(28, 'Liam Brown', 'Male')),
        struct('UK', 'London'),
        array(
            struct('Bread', 2, 'Food'),
            struct('Milk', 1, 'Dairy')
        ),
        struct(
            struct(50, 'NestedBB'),
            struct('NestedCC', 75),
            9
        )
    );

alter table
    iceberg_full_schema_change_parquet rename column new_struct_column.a.aa to new_aa;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        17,
        map('person17', struct(40, 'Olivia Davis', 'Female')),
        struct('Australia', 'Sydney'),
        array(
            struct('Orange', 4, 'Fruit'),
            struct('Broccoli', 6, 'Vegetable')
        ),
        struct(
            struct(60, 'UpdatedBB'),
            struct('UpdatedCC', 88),
            12
        )
    );

alter table
    iceberg_full_schema_change_parquet rename column new_struct_column.a to new_a;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        18,
        map('person18', struct(33, 'Noah Wilson', 'Male')),
        struct('Germany', 'Berlin'),
        array(
            struct('Cheese', 2, 'Dairy'),
            struct('Lettuce', 5, 'Vegetable')
        ),
        struct(
            struct(70, 'NestedBB18'),
            struct('NestedCC18', 95),
            15
        )
    );

alter table
    iceberg_full_schema_change_parquet
ALTER COLUMN
    new_struct_column.b first;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        19,
        map('person19', struct(29, 'Ava Martinez', 'Female')),
        struct('France', 'Paris'),
        array(
            struct('Strawberry', 12, 'Fruit'),
            struct('Spinach', 7, 'Vegetable')
        ),
        struct(
            struct('ReorderedCC', 101),
            struct(85, 'ReorderedBB'),
            18
        )
    );

alter table
    iceberg_full_schema_change_parquet rename column new_struct_column.b.dd to new_dd;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        20,
        map('person20', struct(38, 'James Lee', 'Male')),
        struct('Japan', 'Osaka'),
        array(
            struct('Mango', 6, 'Fruit'),
            struct('Onion', 3, 'Vegetable')
        ),
        struct(
            struct('FinalCC', 110),
            struct(95, 'FinalBB'),
            21
        )
    );

alter table
    iceberg_full_schema_change_parquet rename column new_struct_column to struct_column2;

INSERT INTO
    iceberg_full_schema_change_parquet
VALUES
    (
        21,
        map('person21', struct(45, 'Sophia White', 'Female')),
        struct('Italy', 'Rome'),
        array(
            struct('Pasta', 4, 'Food'),
            struct('Olive', 9, 'Food')
        ),
        struct(
            struct('ExampleCC', 120),
            struct(100, 'ExampleBB'),
            25
        )
    );

-- spark-sql (iceberg)> select * from iceberg_full_schema_change_parquet order by id ;
-- 0       {"person0":{"age":2,"full_name":"zero","gender":null}}  {"country":null,"city":"cn"}    [{"item":"Apple","quantity":null,"category":null},{"item":"Banana","quantity":null,"category":null}]    NULL
-- 1       {"person1":{"age":25,"full_name":"Alice","gender":null}}        {"country":null,"city":"New York"}      [{"item":"Apple","quantity":null,"category":null},{"item":"Banana","quantity":null,"category":null}]    NULL
-- 2       {"person2":{"age":30,"full_name":"Bob","gender":null}}  {"country":null,"city":"Los Angeles"}   [{"item":"Orange","quantity":null,"category":null},{"item":"Grape","quantity":null,"category":null}]    NULL
-- 3       {"person3":{"age":28,"full_name":"Charlie","gender":null}}      {"country":null,"city":"Chicago"}       [{"item":"Pear","quantity":null,"category":null},{"item":"Mango","quantity":null,"category":null}]      NULL
-- 4       {"person4":{"age":35,"full_name":"David","gender":null}}        {"country":null,"city":"Houston"}       [{"item":"Kiwi","quantity":null,"category":null},{"item":"Pineapple","quantity":null,"category":null}]  NULL
-- 5       {"person5":{"age":40,"full_name":"Eve","gender":null}}  {"country":"USA","city":"Phoenix"}      [{"item":"Lemon","quantity":null,"category":null},{"item":"Lime","quantity":null,"category":null}]      NULL
-- 6       {"person6":{"age":22,"full_name":"Frank","gender":null}}        {"country":"USA","city":"Philadelphia"} [{"item":"Watermelon","quantity":null,"category":null},{"item":"Strawberry","quantity":null,"category":null}]   NULL
-- 7       {"person7":{"age":27,"full_name":"Grace","gender":null}}        {"country":"USA","city":"San Antonio"}  [{"item":"Blueberry","quantity":null,"category":null},{"item":"Raspberry","quantity":null,"category":null}]     NULL
-- 8       {"person8":{"age":32,"full_name":"Hank","gender":null}} {"country":"USA","city":"San Diego"}    [{"item":"Cherry","quantity":5,"category":null},{"item":"Plum","quantity":3,"category":null}]   NULL
-- 9       {"person9":{"age":29,"full_name":"Ivy","gender":null}}  {"country":"USA","city":"Dallas"}       [{"item":"Peach","quantity":4,"category":null},{"item":"Apricot","quantity":2,"category":null}] NULL
-- 10      {"person10":{"age":26,"full_name":"Jack","gender":null}}        {"country":"USA","city":"Austin"}       [{"item":"Fig","quantity":6,"category":null},{"item":"Date","quantity":7,"category":null}]      NULL
-- 11      {"person11":{"age":31,"full_name":"Karen","gender":"Female"}}   {"country":"USA","city":"Seattle"}      [{"item":"Coconut","quantity":1,"category":null},{"item":"Papaya","quantity":2,"category":null}]        NULL
-- 12      {"person12":{"age":24,"full_name":"Leo","gender":"Male"}}       {"country":"USA","city":"Portland"}     [{"item":"Guava","quantity":3,"category":null},{"item":"Lychee","quantity":4,"category":null}]  NULL
-- 13      {"person13":{"age":33,"full_name":"Mona","gender":"Female"}}    {"country":"USA","city":"Denver"}       [{"item":"Avocado","quantity":2,"category":"Fruit"},{"item":"Tomato","quantity":5,"category":"Vegetable"}]      NULL
-- 14      {"person14":{"age":28,"full_name":"Nina","gender":"Female"}}    {"country":"USA","city":"Miami"}        [{"item":"Cucumber","quantity":6,"category":"Vegetable"},{"item":"Carrot","quantity":7,"category":"Vegetable"}] NULL
-- 15      {"person15":{"age":30,"full_name":"Emma Smith","gender":"Female"}}      {"country":"USA","city":"New York"}     [{"item":"Banana","quantity":3,"category":"Fruit"},{"item":"Potato","quantity":8,"category":"Vegetable"}]       NULL
-- 16      {"person16":{"age":28,"full_name":"Liam Brown","gender":"Male"}}        {"country":"UK","city":"London"}        [{"item":"Bread","quantity":2,"category":"Food"},{"item":"Milk","quantity":1,"category":"Dairy"}]       {"b":{"cc":"NestedCC","new_dd":75},"new_a":{"new_aa":50,"bb":"NestedBB"},"c":9}
-- 17      {"person17":{"age":40,"full_name":"Olivia Davis","gender":"Female"}}    {"country":"Australia","city":"Sydney"} [{"item":"Orange","quantity":4,"category":"Fruit"},{"item":"Broccoli","quantity":6,"category":"Vegetable"}]     {"b":{"cc":"UpdatedCC","new_dd":88},"new_a":{"new_aa":60,"bb":"UpdatedBB"},"c":12}
-- 18      {"person18":{"age":33,"full_name":"Noah Wilson","gender":"Male"}}       {"country":"Germany","city":"Berlin"}   [{"item":"Cheese","quantity":2,"category":"Dairy"},{"item":"Lettuce","quantity":5,"category":"Vegetable"}]      {"b":{"cc":"NestedCC18","new_dd":95},"new_a":{"new_aa":70,"bb":"NestedBB18"},"c":15}
-- 19      {"person19":{"age":29,"full_name":"Ava Martinez","gender":"Female"}}    {"country":"France","city":"Paris"}     [{"item":"Strawberry","quantity":12,"category":"Fruit"},{"item":"Spinach","quantity":7,"category":"Vegetable"}] {"b":{"cc":"ReorderedCC","new_dd":101},"new_a":{"new_aa":85,"bb":"ReorderedBB"},"c":18}
-- 20      {"person20":{"age":38,"full_name":"James Lee","gender":"Male"}} {"country":"Japan","city":"Osaka"}      [{"item":"Mango","quantity":6,"category":"Fruit"},{"item":"Onion","quantity":3,"category":"Vegetable"}] {"b":{"cc":"FinalCC","new_dd":110},"new_a":{"new_aa":95,"bb":"FinalBB"},"c":21}
-- 21      {"person21":{"age":45,"full_name":"Sophia White","gender":"Female"}}    {"country":"Italy","city":"Rome"}       [{"item":"Pasta","quantity":4,"category":"Food"},{"item":"Olive","quantity":9,"category":"Food"}]       {"b":{"cc":"ExampleCC","new_dd":120},"new_a":{"new_aa":100,"bb":"ExampleBB"},"c":25}
