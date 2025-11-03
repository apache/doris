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
    p_discount_active char(1),
    INDEX p_promo_sk_idx(p_promo_sk) USING INVERTED COMMENT "p_promo_sk index",
    INDEX p_promo_id_idx(p_promo_id) USING INVERTED COMMENT "p_promo_id index",
    INDEX p_start_date_sk_idx(p_start_date_sk) USING INVERTED COMMENT "p_start_date_sk index",
    INDEX p_item_sk_idx(p_item_sk) USING INVERTED COMMENT "p_item_sk index",
    INDEX p_channel_dmail_idx(p_channel_dmail) USING INVERTED COMMENT "p_channel_dmail index",
    INDEX p_channel_details_idx(p_channel_details) USING INVERTED PROPERTIES("parser"="english") COMMENT "p_channel_details index"
)
DUPLICATE KEY(p_promo_sk, p_promo_id)
DISTRIBUTED BY HASH(p_promo_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)

