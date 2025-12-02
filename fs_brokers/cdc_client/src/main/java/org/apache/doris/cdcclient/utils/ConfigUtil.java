// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.cdcclient.utils;

import com.mysql.cj.conf.ConnectionUrl;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.cdcclient.constants.LoadConstants;
import org.apache.doris.cdcclient.model.JobConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;

public class ConfigUtil {

    public static MySqlSourceConfig generateMySqlConfig(JobConfig config) {
        Map<String, String> cdcConfig = config.getConfig();
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();

        ConnectionUrl cu =
                ConnectionUrl.getConnectionUrlInstance(cdcConfig.get(LoadConstants.JDBC_URL), null);
        configFactory.hostname(cu.getMainHost().getHost());
        configFactory.port(cu.getMainHost().getPort());
        configFactory.username(cdcConfig.get(LoadConstants.USER));
        configFactory.password(cdcConfig.get(LoadConstants.PASSWORD));
        String databaseName = cdcConfig.get(LoadConstants.DATABASE);
        configFactory.databaseList(databaseName);
        configFactory.serverId(String.valueOf(Math.abs(config.getJobId().hashCode())));

        configFactory.includeSchemaChanges(false);

        String includingTables = cdcConfig.getOrDefault(LoadConstants.INCLUDE_TABLES, ".*");
        String includingPattern = String.format("(%s)\\.(%s)", databaseName, includingTables);
        String excludingTables = cdcConfig.get(LoadConstants.EXCLUDE_TABLES);
        if (StringUtils.isEmpty(excludingTables)) {
            configFactory.tableList(includingPattern);
        } else {
            String excludingPattern =
                    String.format("?!(%s\\.(%s))$", databaseName, excludingTables);
            String tableList = String.format("(%s)(%s)", excludingPattern, includingPattern);
            configFactory.tableList(tableList);
        }

        // setting startMode
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
            Long pos =
                    Long.valueOf(
                            cdcConfig.get(
                                    MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS.key()));
            if (file != null && pos != null) {
                offsetBuilder.setBinlogFilePosition(file, pos);
            } else {
                offsetBuilder.setBinlogFilePosition("", 0);
            }

            if (cdcConfig.containsKey(
                    MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS.key())) {
                long skipEvents =
                        Long.parseLong(
                                cdcConfig.getOrDefault(
                                        MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS
                                                .key(),
                                        "0"));
                offsetBuilder.setSkipEvents(skipEvents);
            }
            if (cdcConfig.containsKey(
                    MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS.key())) {
                long skipRows =
                        Long.parseLong(
                                cdcConfig.getOrDefault(
                                        MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS
                                                .key(),
                                        "0"));
                offsetBuilder.setSkipRows(skipRows);
            }
            configFactory.startupOptions(StartupOptions.specificOffset(offsetBuilder.build()));
        } else if ("timestamp".equalsIgnoreCase(startupMode)) {
            Long ts =
                    Long.parseLong(
                            cdcConfig.get(MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS.key()));
            configFactory.startupOptions(StartupOptions.timestamp(ts));
        }

        Properties jdbcProperteis = new Properties();
        jdbcProperteis.putAll(cu.getOriginalProperties());
        configFactory.jdbcProperties(jdbcProperteis);

        if (cdcConfig.containsKey(LoadConstants.SPLIT_SIZE)) {
            configFactory.splitSize(Integer.parseInt(cdcConfig.get(LoadConstants.SPLIT_SIZE)));
        }

        return configFactory.createConfig(0);
    }
}
