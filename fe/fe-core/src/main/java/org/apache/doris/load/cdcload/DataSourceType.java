package org.apache.doris.load.cdcload;

import java.util.Arrays;

public enum DataSourceType {

    MYSQL("mysql", "com.mysql.cj.jdbc.Driver");

    private String type;
    private String driverUrl;

    DataSourceType(String type, String driverUrl) {
        this.type = type;
        this.driverUrl = driverUrl;
    }

    public String getType() {
        return type;
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
