use paimon;

create database if not exists test_paimon_time_travel_db;

use test_paimon_time_travel_db;
drop table if exists tbl_time_travel;
CREATE TABLE test_paimon_time_travel_db.tbl_time_travel (
  order_id BIGINT NOT NULL ,
  customer_id BIGINT NOT NULL,
  order_date DATE,
  total_amount DECIMAL(12,2),
  shipping_address STRING,
  order_status STRING,
  is_paid BOOLEAN)
USING paimon
TBLPROPERTIES (
  'bucket' = '3',
  'primary-key' = 'order_id',
  'file.format' = 'parquet'
 );

 -- insert into data snapshotid 1
INSERT INTO test_paimon_time_travel_db.tbl_time_travel VALUES
(1001, 2001, '2024-01-15', 299.99, '123 Maple Street, Springfield, IL 62701', 'COMPLETED', true),
(1002, 2002, '2024-01-16', 156.50, '456 Oak Avenue, Riverside, CA 92507', 'PROCESSING', false),
(1003, 2003, '2024-01-17', 89.00, '789 Pine Boulevard, Greenfield, TX 75001', 'SHIPPED', true);

-- insert into data snpashotid 2
INSERT INTO test_paimon_time_travel_db.tbl_time_travel VALUES
(2001, 3001, '2024-01-18', 445.75, '321 Cedar Lane, Millbrook, NY 12545', 'PENDING', false),
(2002, 3002, '2024-01-19', 67.25, '654 Birch Drive, Lakewood, CO 80226', 'COMPLETED', true),
(2003, 3003, '2024-01-20', 188.90, '987 Elm Court, Fairview, OR 97024', 'CANCELLED', false);

-- insert into data snpashotid 3
INSERT INTO test_paimon_time_travel_db.tbl_time_travel VALUES
(3001, 4001, '2024-01-21', 325.40, '159 Willow Street, Brookdale, FL 33602', 'SHIPPED', true),
(3002, 4002, '2024-01-22', 99.85, '753 Aspen Road, Clearwater, WA 98012', 'PROCESSING', true),
(3003, 4003, '2024-01-23', 512.30, '264 Chestnut Avenue, Westfield, MI 48097', 'COMPLETED', false);

--   create a tag based on the latest snapshot id, run the following sql
CALL sys.create_tag(table => 'test_paimon_time_travel_db.tbl_time_travel', tag => 't_1',snapshot => 1);
--  create a tag based on the  snapshot id 1, run the following sql
CALL sys.create_tag(table => 'test_paimon_time_travel_db.tbl_time_travel', tag => 't_2', snapshot => 2);
--  create a tag based on the  snapshot id 2, run the following sql
CALL sys.create_tag(table => 'test_paimon_time_travel_db.tbl_time_travel', tag => 't_3', snapshot => 3);

-- insert into data snpashotid 4
INSERT INTO test_paimon_time_travel_db.tbl_time_travel VALUES
(5001, 6001, '2024-01-24', 278.60, '842 Hickory Lane, Stonewood, GA 30309', 'PENDING', true),
(5002, 6002, '2024-01-25', 134.75, '417 Poplar Street, Ridgefield, NV 89109', 'SHIPPED', false),
(5003, 6003, '2024-01-26', 389.20, '695 Sycamore Drive, Maplewood, AZ 85001', 'COMPLETED', true);

CALL sys.create_tag(table => 'test_paimon_time_travel_db.tbl_time_travel', tag => 't_4', snapshot => 4);


-- create branch 1
CALL sys.create_branch('test_paimon_time_travel_db.tbl_time_travel', 'b_1');
-- create branch 2
CALL sys.create_branch('test_paimon_time_travel_db.tbl_time_travel', 'b_2');

INSERT INTO test_paimon_time_travel_db.`tbl_time_travel$branch_b_1` VALUES
(10001, 7001, '2024-01-27', 156.30, '108 Magnolia Street, Riverside, KY 40475', 'PROCESSING', true),
(10002, 7002, '2024-01-28', 423.80, '572 Dogwood Avenue, Pleasantville, NJ 08232', 'COMPLETED', false),
(10003, 7003, '2024-01-29', 89.45, '946 Redwood Boulevard, Hillcrest, SC 29526', 'SHIPPED', true),
(10004, 7004, '2024-01-30', 267.90, '213 Spruce Lane, Oceanview, ME 04401', 'PENDING', false),
(10005, 7005, '2024-01-31', 345.15, '687 Walnut Drive, Parkside, VT 05672', 'CANCELLED', true);


INSERT INTO test_paimon_time_travel_db.`tbl_time_travel$branch_b_1` VALUES
(10006, 7001, '2024-01-27', 156.30, '108 Magnolia Street, Riverside, KY 40475', 'PROCESSING', true),
(10007, 7002, '2024-01-28', 423.80, '572 Dogwood Avenue, Pleasantville, NJ 08232', 'COMPLETED', false),
(10008, 7003, '2024-01-29', 89.45, '946 Redwood Boulevard, Hillcrest, SC 29526', 'SHIPPED', true),
(10009, 7004, '2024-01-30', 267.90, '213 Spruce Lane, Oceanview, ME 04401', 'PENDING', false),
(10010, 7005, '2024-01-31', 345.15, '687 Walnut Drive, Parkside, VT 05672', 'CANCELLED', true);

INSERT INTO test_paimon_time_travel_db.`tbl_time_travel$branch_b_2` VALUES
(20001, 8001, '2024-02-01', 198.75, '741 Rosewood Court, Summerville, AL 35148', 'COMPLETED', true),
(20002, 8002, '2024-02-02', 456.20, '329 Cypress Street, Brookhaven, MS 39601', 'PROCESSING', false),
(20003, 8003, '2024-02-03', 67.85, '852 Juniper Lane, Fairfield, OH 45014', 'SHIPPED', true),
(20004, 8004, '2024-02-04', 312.40, '614 Peach Avenue, Greenville, TN 37743', 'PENDING', true),
(20005, 8005, '2024-02-05', 129.90, '507 Cherry Drive, Lakeside, UT 84040', 'CANCELLED', false);

INSERT INTO test_paimon_time_travel_db.`tbl_time_travel$branch_b_2` VALUES
(20006, 8006, '2024-02-06', 234.60, '183 Bluebell Street, Millfield, ID 83262', 'COMPLETED', true),
(20007, 8007, '2024-02-07', 378.45, '465 Lavender Avenue, Thorndale, WY 82201', 'PROCESSING', false),
(20008, 8008, '2024-02-08', 92.30, '729 Iris Lane, Riverside, MN 55987', 'SHIPPED', true),
(20009, 8009, '2024-02-09', 445.80, '856 Tulip Boulevard, Sunnydale, ND 58301', 'PENDING', false),
(20010, 8010, '2024-02-10', 167.25, '392 Daisy Court, Meadowbrook, SD 57401', 'CANCELLED', true);


-- time travle schema change
ALTER TABLE test_paimon_time_travel_db.tbl_time_travel ADD COLUMNS (
    new_col1 INT
);

-- - snpashot 5
INSERT INTO test_paimon_time_travel_db.tbl_time_travel VALUES
(6001, 9001, '2024-02-11', 456.80, '123 New Street, Downtown, CA 90210', 'COMPLETED', true, 100),
(6002, 9002, '2024-02-12', 289.45, '456 Updated Ave, Midtown, NY 10001', 'PROCESSING', false, 200),
(6003, 9003, '2024-02-13', 378.90, '789 Modern Blvd, Uptown, TX 75201', 'SHIPPED', true, 300);

CALL sys.create_tag(table => 'test_paimon_time_travel_db.tbl_time_travel', tag => 't_5', snapshot => 5);

-- - snapshot 6
INSERT INTO test_paimon_time_travel_db.tbl_time_travel VALUES
(6004, 9004, '2024-02-14', 199.99, '321 Future Lane, Innovation, WA 98001', 'PENDING', true, 400),
(6005, 9005, '2024-02-15', 567.25, '654 Progress Drive, Tech City, OR 97201', 'COMPLETED', false, 500),
(6006, 9006, '2024-02-16', 123.75, '987 Advanced Court, Silicon Valley, CA 94301', 'CANCELLED', true, 600);

CALL sys.create_tag(table => 'test_paimon_time_travel_db.tbl_time_travel', tag => 't_6', snapshot => 6);
