package org.apache.doris.job.extensions.cdc.utils;

public class CdcLoadConstants {
    public static final String DB_SOURCE_TYPE = "db_source_type";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String DATABASE_NAME = "database_name";
    public static final String TABLE_NAME = "table_name";

    public static final String INCLUDE_TABLES_LIST = "include_tables_list";
    public static final String EXCLUDE_TABLES_LIST = "exclude_tables_list";
    public static final String TABLE_PROPS_PREFIX = "table.create.properties.";
    public static final String MAX_BATCH_INTERVAL = "max_batch_interval";
    public static final String MAX_BATCH_ROWS = "max_batch_rows";
    public static final String MAX_BATCH_SIZE = "max_batch_size";
}
