CREATE TABLE dup_tbl_array
(
    k00 INT                    NOT NULL,
    k01 array<BOOLEAN>         NULL,
    k02 array<TINYINT>         NULL,
    k03 array<SMALLINT>        NULL,
    k04 array<INT>             NULL,
    k05 array<BIGINT>          NULL,
    k06 array<LARGEINT>        NULL,
    k07 array<FLOAT>           NULL,
    k08 array<DOUBLE>          NULL,
    k09 array<DECIMAL>         NULL,
    k10 array<DECIMALV3>       NULL,
    k11 array<DATE>            NULL,
    k12 array<DATETIME>        NULL,
    k13 array<DATEV2>          NULL,
    k14 array<DATETIMEV2>      NULL,
    k15 array<CHAR>            NULL,
    k16 array<VARCHAR>         NULL,
    k17 array<STRING>          NULL,
    kd01 array<BOOLEAN>         NOT NULL DEFAULT "[]",
    kd02 array<TINYINT>         NOT NULL DEFAULT "[]",
    kd03 array<SMALLINT>        NOT NULL DEFAULT "[]",
    kd04 array<INT>             NOT NULL DEFAULT "[]",
    kd05 array<BIGINT>          NOT NULL DEFAULT "[]",
    kd06 array<LARGEINT>        NOT NULL DEFAULT "[]",
    kd07 array<FLOAT>           NOT NULL DEFAULT "[]",
    kd08 array<DOUBLE>          NOT NULL DEFAULT "[]",
    kd09 array<DECIMAL>         NOT NULL DEFAULT "[]",
    kd10 array<DECIMALV3>       NOT NULL DEFAULT "[]",
    kd11 array<DATE>            NOT NULL DEFAULT "[]",
    kd12 array<DATETIME>        NOT NULL DEFAULT "[]",
    kd13 array<DATEV2>          NOT NULL DEFAULT "[]",
    kd14 array<DATETIMEV2>      NOT NULL DEFAULT "[]",
    kd15 array<CHAR>            NOT NULL DEFAULT "[]",
    kd16 array<VARCHAR>         NOT NULL DEFAULT "[]",
)
    DUPLICATE KEY(k00)
DISTRIBUTED BY HASH(k00) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
