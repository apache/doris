package org.apache.doris.job.extensions.cdc;

import java.util.Arrays;

public enum DataSourceType {

    MYSQL("mysql", "com.mysql.cj.jdbc.Driver", "mysql-connector-java-8.0.25.jar");

    private String type;
    private String driverClass;
    private String driverUrl;

    DataSourceType(String type, String driverClass, String driverUrl) {
        this.type = type;
        this.driverClass = driverClass;
        this.driverUrl = driverUrl;
    }

    public String getType() {
        return type;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public String getDriverUrl() {
        return driverUrl;
    }

    public static DataSourceType getByType(String type) {
        return Arrays.stream(DataSourceType.values())
            .filter(config -> config.getType().equals(type))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unsupport datasource " + type));
    }
}
