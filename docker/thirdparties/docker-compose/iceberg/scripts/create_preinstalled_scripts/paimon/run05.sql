use paimon;

create database if not exists test_paimon_schema_change;

use test_paimon_schema_change;



CREATE TABLE paimon_full_schema_change_orc (
  id int,
  map_column map<string, struct<name: string, age: int>>,
  struct_column struct<city: string, population: int>,
  array_column array<struct<product: string, price: float>>
) 
USING paimon
TBLPROPERTIES ("file.format" = "orc");
;

INSERT INTO paimon_full_schema_change_orc VALUES (
  0,
  map('person0', struct('zero', 2)),
  struct('cn', 1000000),
  array(struct('Apple', 1.99), struct('Banana', 0.99))
);


-- Schema Change 1: Add 'address' string to map_column's struct
ALTER TABLE paimon_full_schema_change_orc ADD COLUMN map_column.value.address string;
INSERT INTO paimon_full_schema_change_orc VALUES (
  1,
  map('person1', struct('Alice', 25, '123 Main St')),
  struct('New York', 8000000),
  array(struct('Apple', 1.99), struct('Banana', 0.99))
);


ALTER TABLE paimon_full_schema_change_orc RENAME COLUMN map_column.value.name TO full_name;


INSERT INTO paimon_full_schema_change_orc VALUES (
  2,
  map('person2', struct('Bob Smith', 30, '456 Oak Ave')),
  struct('Los Angeles', 4000000),
  array(struct('Orange', 2.49), struct('Grapes', 3.99))
);



ALTER TABLE paimon_full_schema_change_orc RENAME COLUMN struct_column.city TO location;

INSERT INTO paimon_full_schema_change_orc VALUES (
  3,
  map('person3', struct('Charlie', 28, '789 Pine Rd')),
  struct('Chicago', 2700000),
  array(struct('Mango', 2.99), struct('Peach', 1.49))
);



ALTER TABLE paimon_full_schema_change_orc DROP COLUMN struct_column.population;

INSERT INTO paimon_full_schema_change_orc VALUES (
  4,
  map('person4', struct('Diana Green', 35, '321 Elm St')),
  struct('San Francisco'),
  array(struct('Strawberry', 4.29), struct('Blueberry', 5.19))
);



ALTER TABLE paimon_full_schema_change_orc ADD COLUMN struct_column.population bigint;

INSERT INTO paimon_full_schema_change_orc VALUES (
  5,
  map('person5', struct('Edward Black', 42, '987 Willow Ln')),
  struct('Seattle', 750000),
  array(struct('Kiwi', 1.79), struct('Pineapple', 3.49))
);


ALTER TABLE paimon_full_schema_change_orc RENAME COLUMN array_column.element.product to new_product;

INSERT INTO paimon_full_schema_change_orc VALUES (
  6,
  map('person6', struct('Fiona Lake', 29, '654 Birch Ln')),
  struct('Austin', 950000),
  array(struct('Pineapple', 3.99), struct('Kiwi', 2.59))
);




alter table paimon_full_schema_change_orc ADD COLUMN new_struct_column struct<a: struct<aa:int, bb:string> ,  b int> ;

INSERT INTO paimon_full_schema_change_orc VALUES (
  7,
  map('person7', struct('George Hall', 41, '987 Elm St')),
  struct('Seattle', 730000),
  array(struct('Mango', 4.49), struct('Papaya', 3.75)),
  struct(struct(1001, 'inner string'), 2025)
);




alter table paimon_full_schema_change_orc rename column new_struct_column.a.aa to new_aa;


INSERT INTO paimon_full_schema_change_orc VALUES (
  8,
  map('person8', struct('Hannah King', 34, '321 Oak Blvd')),
  struct('Boston', 690000),
  array(struct('Dragonfruit', 5.25), struct('Lychee', 4.10)),
  struct(struct(2002, 'deep inner string'), 3025)
);



alter table paimon_full_schema_change_orc ADD COLUMN new_struct_column.c  struct<cc:string ,dd:int>;


INSERT INTO paimon_full_schema_change_orc VALUES (
  9,
  map('person9', struct('Ian Moore', 38, '888 Maple Way')),
  struct('Denver', 620000),
  array(struct('Peach', 2.89), struct('Plum', 2.45)),
  struct(
    struct(3003, 'nested value'),
    4025,
    struct('extra info', 123)
  )
);



alter table paimon_full_schema_change_orc rename column new_struct_column.a to new_a;


INSERT INTO paimon_full_schema_change_orc VALUES (
  10,
  map('person10', struct('Julia Nash', 27, '456 Cedar Ct')),
  struct('Phoenix', 820000),
  array(struct('Cherry', 3.15), struct('Apricot', 2.95)),
  struct(
    struct(4004, 'renamed inner value'),
    5025,
    struct('details', 456)
  )
);




alter table paimon_full_schema_change_orc rename column new_struct_column.c.dd to new_dd;

INSERT INTO paimon_full_schema_change_orc VALUES (
  11,
  map('person11', struct('Kevin Orr', 45, '789 Spruce Dr')),
  struct('San Diego', 770000),
  array(struct('Nectarine', 3.60), struct('Coconut', 4.20)),
  struct(
    struct(5005, 'final structure'),
    6025,
    struct('notes', 789)
  )
);



alter table paimon_full_schema_change_orc rename column new_struct_column to struct_column2;


INSERT INTO paimon_full_schema_change_orc VALUES (
  12,
  map('person12', struct('Laura Price', 36, '1010 Aspen Way')),
  struct('Dallas', 880000),
  array(struct('Cranberry', 3.30), struct('Fig', 2.70)),
  struct(
    struct(6006, 'finalized'),
    7025,
    struct('metadata', 321)
  )
);

alter table paimon_full_schema_change_orc change column struct_column2 after struct_column;

INSERT INTO paimon_full_schema_change_orc VALUES (
  13,
  map('person13', struct('Michael Reed', 31, '2020 Pine Cir')),
  struct('Atlanta', 810000),
  struct(
    struct(7007, 'relocated field'),
    8025,
    struct('info', 654)
  ),
  array(struct('Guava', 3.95), struct('Passionfruit', 4.60))
);


CREATE TABLE paimon_full_schema_change_parquet (
  id int,
  map_column map<string, struct<name: string, age: int>>,
  struct_column struct<city: string, population: int>,
  array_column array<struct<product: string, price: float>>
) 
USING paimon
TBLPROPERTIES ("file.format" = "parquet");
;

INSERT INTO paimon_full_schema_change_parquet VALUES (
  0,
  map('person0', struct('zero', 2)),
  struct('cn', 1000000),
  array(struct('Apple', 1.99), struct('Banana', 0.99))
);


-- Schema Change 1: Add 'address' string to map_column's struct
ALTER TABLE paimon_full_schema_change_parquet ADD COLUMN map_column.value.address string;
INSERT INTO paimon_full_schema_change_parquet VALUES (
  1,
  map('person1', struct('Alice', 25, '123 Main St')),
  struct('New York', 8000000),
  array(struct('Apple', 1.99), struct('Banana', 0.99))
);


ALTER TABLE paimon_full_schema_change_parquet RENAME COLUMN map_column.value.name TO full_name;


INSERT INTO paimon_full_schema_change_parquet VALUES (
  2,
  map('person2', struct('Bob Smith', 30, '456 Oak Ave')),
  struct('Los Angeles', 4000000),
  array(struct('Orange', 2.49), struct('Grapes', 3.99))
);



ALTER TABLE paimon_full_schema_change_parquet RENAME COLUMN struct_column.city TO location;

INSERT INTO paimon_full_schema_change_parquet VALUES (
  3,
  map('person3', struct('Charlie', 28, '789 Pine Rd')),
  struct('Chicago', 2700000),
  array(struct('Mango', 2.99), struct('Peach', 1.49))
);



ALTER TABLE paimon_full_schema_change_parquet DROP COLUMN struct_column.population;

INSERT INTO paimon_full_schema_change_parquet VALUES (
  4,
  map('person4', struct('Diana Green', 35, '321 Elm St')),
  struct('San Francisco'),
  array(struct('Strawberry', 4.29), struct('Blueberry', 5.19))
);



ALTER TABLE paimon_full_schema_change_parquet ADD COLUMN struct_column.population bigint;

INSERT INTO paimon_full_schema_change_parquet VALUES (
  5,
  map('person5', struct('Edward Black', 42, '987 Willow Ln')),
  struct('Seattle', 750000),
  array(struct('Kiwi', 1.79), struct('Pineapple', 3.49))
);


ALTER TABLE paimon_full_schema_change_parquet RENAME COLUMN array_column.element.product to new_product;

INSERT INTO paimon_full_schema_change_parquet VALUES (
  6,
  map('person6', struct('Fiona Lake', 29, '654 Birch Ln')),
  struct('Austin', 950000),
  array(struct('Pineapple', 3.99), struct('Kiwi', 2.59))
);




alter table paimon_full_schema_change_parquet ADD COLUMN new_struct_column struct<a: struct<aa:int, bb:string> ,  b int> ;

INSERT INTO paimon_full_schema_change_parquet VALUES (
  7,
  map('person7', struct('George Hall', 41, '987 Elm St')),
  struct('Seattle', 730000),
  array(struct('Mango', 4.49), struct('Papaya', 3.75)),
  struct(struct(1001, 'inner string'), 2025)
);




alter table paimon_full_schema_change_parquet rename column new_struct_column.a.aa to new_aa;


INSERT INTO paimon_full_schema_change_parquet VALUES (
  8,
  map('person8', struct('Hannah King', 34, '321 Oak Blvd')),
  struct('Boston', 690000),
  array(struct('Dragonfruit', 5.25), struct('Lychee', 4.10)),
  struct(struct(2002, 'deep inner string'), 3025)
);



alter table paimon_full_schema_change_parquet ADD COLUMN new_struct_column.c  struct<cc:string ,dd:int>;


INSERT INTO paimon_full_schema_change_parquet VALUES (
  9,
  map('person9', struct('Ian Moore', 38, '888 Maple Way')),
  struct('Denver', 620000),
  array(struct('Peach', 2.89), struct('Plum', 2.45)),
  struct(
    struct(3003, 'nested value'),
    4025,
    struct('extra info', 123)
  )
);



alter table paimon_full_schema_change_parquet rename column new_struct_column.a to new_a;


INSERT INTO paimon_full_schema_change_parquet VALUES (
  10,
  map('person10', struct('Julia Nash', 27, '456 Cedar Ct')),
  struct('Phoenix', 820000),
  array(struct('Cherry', 3.15), struct('Apricot', 2.95)),
  struct(
    struct(4004, 'renamed inner value'),
    5025,
    struct('details', 456)
  )
);




alter table paimon_full_schema_change_parquet rename column new_struct_column.c.dd to new_dd;

INSERT INTO paimon_full_schema_change_parquet VALUES (
  11,
  map('person11', struct('Kevin Orr', 45, '789 Spruce Dr')),
  struct('San Diego', 770000),
  array(struct('Nectarine', 3.60), struct('Coconut', 4.20)),
  struct(
    struct(5005, 'final structure'),
    6025,
    struct('notes', 789)
  )
);



alter table paimon_full_schema_change_parquet rename column new_struct_column to struct_column2;


INSERT INTO paimon_full_schema_change_parquet VALUES (
  12,
  map('person12', struct('Laura Price', 36, '1010 Aspen Way')),
  struct('Dallas', 880000),
  array(struct('Cranberry', 3.30), struct('Fig', 2.70)),
  struct(
    struct(6006, 'finalized'),
    7025,
    struct('metadata', 321)
  )
);

alter table paimon_full_schema_change_parquet change column struct_column2 after struct_column;

INSERT INTO paimon_full_schema_change_parquet VALUES (
  13,
  map('person13', struct('Michael Reed', 31, '2020 Pine Cir')),
  struct('Atlanta', 810000),
  struct(
    struct(7007, 'relocated field'),
    8025,
    struct('info', 654)
  ),
  array(struct('Guava', 3.95), struct('Passionfruit', 4.60))
);

-- spark-sql (test_paimon_schema_change)> 
--                                      > select * from paimon_full_schema_change_orc order by id;
-- 0       {"person0":{"full_name":"zero","age":2,"address":null}} {"location":"cn","population":null}     NULL    [{"new_product":"Apple","price":1.99},{"new_product":"Banana","price":0.99}]
-- 1       {"person1":{"full_name":"Alice","age":25,"address":"123 Main St"}}      {"location":"New York","population":null}       NULL    [{"new_product":"Apple","price":1.99},{"new_product":"Banana","price":0.99}]
-- 2       {"person2":{"full_name":"Bob Smith","age":30,"address":"456 Oak Ave"}}  {"location":"Los Angeles","population":null}    NULL    [{"new_product":"Orange","price":2.49},{"new_product":"Grapes","price":3.99}]
-- 3       {"person3":{"full_name":"Charlie","age":28,"address":"789 Pine Rd"}}    {"location":"Chicago","population":null}        NULL    [{"new_product":"Mango","price":2.99},{"new_product":"Peach","price":1.49}]
-- 4       {"person4":{"full_name":"Diana Green","age":35,"address":"321 Elm St"}} {"location":"San Francisco","population":null}  NULL    [{"new_product":"Strawberry","price":4.29},{"new_product":"Blueberry","price":5.19}]
-- 5       {"person5":{"full_name":"Edward Black","age":42,"address":"987 Willow Ln"}}     {"location":"Seattle","population":750000}      NULL    [{"new_product":"Kiwi","price":1.79},{"new_product":"Pineapple","price":3.49}]
-- 6       {"person6":{"full_name":"Fiona Lake","age":29,"address":"654 Birch Ln"}}        {"location":"Austin","population":950000}       NULL    [{"new_product":"Pineapple","price":3.99},{"new_product":"Kiwi","price":2.59}]
-- 7       {"person7":{"full_name":"George Hall","age":41,"address":"987 Elm St"}} {"location":"Seattle","population":730000}      {"new_a":{"new_aa":1001,"bb":"inner string"},"b":2025,"c":null} [{"new_product":"Mango","price":4.49},{"new_product":"Papaya","price":3.75}]
-- 8       {"person8":{"full_name":"Hannah King","age":34,"address":"321 Oak Blvd"}}       {"location":"Boston","population":690000}       {"new_a":{"new_aa":2002,"bb":"deep inner string"},"b":3025,"c":null}    [{"new_product":"Dragonfruit","price":5.25},{"new_product":"Lychee","price":4.1}]
-- 9       {"person9":{"full_name":"Ian Moore","age":38,"address":"888 Maple Way"}}        {"location":"Denver","population":620000}       {"new_a":{"new_aa":3003,"bb":"nested value"},"b":4025,"c":{"cc":"extra info","new_dd":123}}     [{"new_product":"Peach","price":2.89},{"new_product":"Plum","price":2.45}]
-- 10      {"person10":{"full_name":"Julia Nash","age":27,"address":"456 Cedar Ct"}}       {"location":"Phoenix","population":820000}      {"new_a":{"new_aa":4004,"bb":"renamed inner value"},"b":5025,"c":{"cc":"details","new_dd":456}} [{"new_product":"Cherry","price":3.15},{"new_product":"Apricot","price":2.95}]
-- 11      {"person11":{"full_name":"Kevin Orr","age":45,"address":"789 Spruce Dr"}}       {"location":"San Diego","population":770000}    {"new_a":{"new_aa":5005,"bb":"final structure"},"b":6025,"c":{"cc":"notes","new_dd":789}}       [{"new_product":"Nectarine","price":3.6},{"new_product":"Coconut","price":4.2}]
-- 12      {"person12":{"full_name":"Laura Price","age":36,"address":"1010 Aspen Way"}}    {"location":"Dallas","population":880000}       {"new_a":{"new_aa":6006,"bb":"finalized"},"b":7025,"c":{"cc":"metadata","new_dd":321}}  [{"new_product":"Cranberry","price":3.3},{"new_product":"Fig","price":2.7}]
-- 13      {"person13":{"full_name":"Michael Reed","age":31,"address":"2020 Pine Cir"}}    {"location":"Atlanta","population":810000}      {"new_a":{"new_aa":7007,"bb":"relocated field"},"b":8025,"c":{"cc":"info","new_dd":654}}        [{"new_product":"Guava","price":3.95},{"new_product":"Passionfruit","price":4.6}]
-- Time taken: 0.991 seconds, Fetched 14 row(s)