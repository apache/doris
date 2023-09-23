CREATE TABLE IF NOT EXISTS item (
    i_item_sk bigint,
    i_item_id char(16),
    i_rec_start_date date,
    i_rec_end_date date,
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
    i_product_name char(50),
    INDEX i_rec_start_date_idx(i_rec_start_date) USING INVERTED COMMENT "i_rec_start_date index",
    INDEX i_item_sk_idx(i_item_sk) USING INVERTED COMMENT "i_item_sk index",
    INDEX i_item_desc_idx(i_item_desc) USING INVERTED PROPERTIES("parser"="english") COMMENT "i_item_desc index",
    INDEX i_brand_idx(i_brand) USING INVERTED COMMENT "i_brand index",
    INDEX i_rec_end_date_idx(i_rec_end_date) USING INVERTED COMMENT "i_rec_end_date index",
    INDEX i_category_idx(i_category) USING INVERTED PROPERTIES("parser"="standard") COMMENT "i_category index"
)
DUPLICATE KEY(i_item_sk, i_item_id)
DISTRIBUTED BY HASH(i_item_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)
