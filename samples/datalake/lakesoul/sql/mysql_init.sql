CREATE TABLE customer (
    c_custkey INT NOT NULL,
    c_name VARCHAR(25),
    c_address VARCHAR(40),
    c_nationkey INT,
    c_phone CHAR(15),
    c_acctbal DECIMAL(15,2),
    c_mktsegment VARCHAR(10),
    c_comment VARCHAR(117),
    PRIMARY KEY (c_custkey)
);

LOAD DATA INFILE '/data/tpch/customer.tbl' 
INTO TABLE customer 
FIELDS TERMINATED BY '|' 
LINES TERMINATED BY '\n';

CREATE TABLE orders (
    o_orderkey INT NOT NULL,
    o_custkey INT,
    o_orderstatus CHAR(1),
    o_totalprice DECIMAL(15,2),
    o_orderdate DATE,
    o_orderpriority CHAR(15),
    o_clerk CHAR(15),
    o_shippriority INT,
    o_comment VARCHAR(79),
    PRIMARY KEY (o_orderkey),
    FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey)
);

LOAD DATA INFILE '/data/tpch/orders.tbl' 
INTO TABLE orders 
FIELDS TERMINATED BY '|' 
LINES TERMINATED BY '\n';