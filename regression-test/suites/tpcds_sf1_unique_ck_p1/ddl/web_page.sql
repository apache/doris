CREATE TABLE IF NOT EXISTS web_page (
        wp_web_page_sk bigint,
        wp_web_page_id char(16),
        wp_rec_start_date date,
        wp_rec_end_date date,
        wp_creation_date_sk bigint,
        wp_access_date_sk bigint,
        wp_autogen_flag char(1),
        wp_customer_sk bigint,
        wp_url varchar(100),
        wp_type char(50),
        wp_char_count integer,
        wp_link_count integer,
        wp_image_count integer,
        wp_max_ad_count integer
)
UNIQUE KEY(wp_web_page_sk)
CLUSTER BY(wp_web_page_id, wp_web_page_sk)
DISTRIBUTED BY HASH(wp_web_page_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)
