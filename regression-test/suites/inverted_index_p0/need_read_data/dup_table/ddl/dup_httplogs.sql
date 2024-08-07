CREATE TABLE IF NOT EXISTS dup_httplogs
(   
    `id`          bigint NOT NULL AUTO_INCREMENT(100),
     `timestamp` int(11) NULL,
    `clientip`   varchar(20) NULL,
    `request`    text NULL,
    `status`     int(11) NULL,
    `size`       int(11) NULL,
    INDEX        clientip_idx (`clientip`) USING INVERTED COMMENT '',
    INDEX        request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
    INDEX        status_idx (`status`) USING INVERTED COMMENT '',
    INDEX        size_idx (`size`) USING INVERTED COMMENT ''
) DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH (`id`) BUCKETS 32
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"compaction_policy" = "time_series",
"inverted_index_storage_format" = "v2",
"compression" = "ZSTD"
);