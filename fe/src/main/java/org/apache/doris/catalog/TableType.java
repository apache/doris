
package org.apache.doris.catalog;

public enum TableType {
    OLAP,
    SCHEMA,
    MYSQL,
    OLAP_EXTERNAL,
    BROKER,
    ELASTICSEARCH,
    HIVE,
    ICEBERG,
    HUDI,
    JDBC,
    TEST_EXTERNAL,
    PAIMON,
    FLUSS, // Added for Fluss integration
    MAX_VALUE
}
