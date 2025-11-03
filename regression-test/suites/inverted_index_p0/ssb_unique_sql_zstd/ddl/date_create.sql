CREATE TABLE IF NOT EXISTS `date` (
  `d_datekey` int(11) NOT NULL COMMENT "",
  `d_date` varchar(20) NOT NULL COMMENT "",
  `d_dayofweek` varchar(10) NOT NULL COMMENT "",
  `d_month` varchar(11) NOT NULL COMMENT "",
  `d_year` int(11) NOT NULL COMMENT "",
  `d_yearmonthnum` int(11) NOT NULL COMMENT "",
  `d_yearmonth` varchar(9) NOT NULL COMMENT "",
  `d_daynuminweek` int(11) NOT NULL COMMENT "",
  `d_daynuminmonth` int(11) NOT NULL COMMENT "",
  `d_daynuminyear` int(11) NOT NULL COMMENT "",
  `d_monthnuminyear` int(11) NOT NULL COMMENT "",
  `d_weeknuminyear` int(11) NOT NULL COMMENT "",
  `d_sellingseason` varchar(14) NOT NULL COMMENT "",
  `d_lastdayinweekfl` int(11) NOT NULL COMMENT "",
  `d_lastdayinmonthfl` int(11) NOT NULL COMMENT "",
  `d_holidayfl` int(11) NOT NULL COMMENT "",
  `d_weekdayfl` int(11) NOT NULL COMMENT "",
  INDEX d_datekey_idx(d_datekey) USING INVERTED COMMENT 'd_datekey index',
  INDEX d_date_idx(d_date) USING INVERTED COMMENT 'd_date index',
  INDEX d_dayofweek_idx(d_dayofweek) USING INVERTED COMMENT 'd_dayofweek index',
  INDEX d_month_idx(d_month) USING INVERTED COMMENT 'd_month index',
  INDEX d_year_idx(d_year) USING INVERTED COMMENT 'd_year index'
)
UNIQUE KEY (`d_datekey`)
DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1
PROPERTIES (
"compression"="zstd",
"replication_num" = "1",
"enable_unique_key_merge_on_write" = "true"
);
