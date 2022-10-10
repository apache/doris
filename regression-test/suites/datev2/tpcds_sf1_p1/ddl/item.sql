CREATE TABLE IF NOT EXISTS item (
    i_item_sk bigint,
    i_item_id char(16),
    i_rec_start_date datev2,
    i_rec_end_date datev2,
    i_item_desc varchar(200),
    i_current_price decimal(7,2),
    i_wholesale_cost decimal(7,2),
    i_brand_id integer,
    i_brand char(50),
    i_class_id integer,
    i_class char(50),
    i_category_id integer,
    i_category char(50),
    i_manufact_id integer,
    i_manufact char(50),
    i_size char(20),
    i_formulation char(20),
    i_color char(20),
    i_units char(10),
    i_container char(10),
    i_manager_id integer,
    i_product_name char(50)
)
DUPLICATE KEY(i_item_sk, i_item_id)
DISTRIBUTED BY HASH(i_item_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)
