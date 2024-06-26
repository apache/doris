package org.apache.doris.cdcloader.mysql.utils;

import org.apache.doris.cdcloader.mysql.constants.LoadConstants;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;

import java.util.Map;

public class ConfigUtil {

    public static MySqlSourceConfig generateMySqlConfig(Map<String, String> cdcConfig) {
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
        configFactory.hostname(cdcConfig.get(LoadConstants.HOST));
        configFactory.port(Integer.valueOf(cdcConfig.get(LoadConstants.PORT)));
        configFactory.username(cdcConfig.get(LoadConstants.USERNAME));
        configFactory.password(cdcConfig.get(LoadConstants.PASSWORD));

        String databaseName = cdcConfig.get(LoadConstants.DATABASE_NAME);
        configFactory.databaseList(databaseName);
        String includingTables = cdcConfig.get(LoadConstants.INCLUDE_TABLES_LIST);
        String includingPattern = String.format("(%s)\\.(%s)", databaseName, includingTables);
        configFactory.tableList(includingPattern);
        configFactory.includeSchemaChanges(false);

        //setting startMode
        String startupMode = cdcConfig.get(MySqlSourceOptions.SCAN_STARTUP_MODE.key());
        if ("initial".equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.initial());
        } else if ("earliest-offset".equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.earliest());
        } else if ("latest-offset".equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.latest());
        } else if ("specific-offset".equalsIgnoreCase(startupMode)) {
            BinlogOffsetBuilder offsetBuilder = BinlogOffset.builder();
            String file = cdcConfig.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE.key());
            Long pos = Long.valueOf(cdcConfig.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS.key()));
            if (file != null && pos != null) {
                offsetBuilder.setBinlogFilePosition(file, pos);
            }else{
                offsetBuilder.setBinlogFilePosition("", 0);
            }

            if(cdcConfig.containsKey(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS.key())){
                long skipEvents = Long.parseLong(cdcConfig.getOrDefault(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS.key(), "0"));
                offsetBuilder.setSkipEvents(skipEvents);
            }
            if(cdcConfig.containsKey(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS.key())){
                long skipRows = Long.parseLong(cdcConfig.getOrDefault(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS.key(), "0"));
                offsetBuilder.setSkipRows(skipRows);
            }
            configFactory.startupOptions(StartupOptions.specificOffset(offsetBuilder.build()));
        } else if ("timestamp".equalsIgnoreCase(startupMode)) {
            Long ts = Long.parseLong(cdcConfig.get(MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS.key()));
            configFactory.startupOptions(StartupOptions.timestamp(ts));
        }

        // for debug
        // configFactory.splitSize(1);
        return configFactory.createConfig(0);
    }
}
