use `default`;

CREATE TABLE `orc_tiny_stripes`(
    col1 bigint,
    col2 string,
    col3 bigint
)
STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc/orc_tiny_stripes';

msck repair table orc_tiny_stripes;
