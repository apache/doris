CREATE TABLE IF NOT EXISTS promotion (
    p_promo_sk bigint,
    p_promo_id char(16),
    p_start_date_sk bigint,
    p_end_date_sk bigint,
    p_item_sk bigint,
    p_cost decimal(15,2),
    p_response_targe integer,
    p_promo_name char(50),
    p_channel_dmail char(1),
    p_channel_email char(1),
    p_channel_catalog char(1),
    p_channel_tv char(1),
    p_channel_radio char(1),
    p_channel_press char(1),
    p_channel_event char(1),
    p_channel_demo char(1),
    p_channel_details varchar(100),
    p_purpose char(15),
    p_discount_active char(1)
)
UNIQUE KEY(p_promo_sk)
CLUSTER BY(p_promo_id, p_promo_name)
DISTRIBUTED BY HASH(p_promo_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)

