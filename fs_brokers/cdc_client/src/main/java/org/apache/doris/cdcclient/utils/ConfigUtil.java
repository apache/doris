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

import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.JobBaseConfig;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffsetUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.cj.conf.ConnectionUrl;
import io.debezium.connector.mysql.MySqlConnection;

public class ConfigUtil {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static String getServerId(long jobId) {
        return String.valueOf(Math.abs(String.valueOf(jobId).hashCode()));
    }

    public static MySqlSourceConfig generateMySqlConfig(JobBaseConfig config) {
        Map<String, String> cdcConfig = config.getConfig();
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();

        ConnectionUrl cu =
                ConnectionUrl.getConnectionUrlInstance(
                        cdcConfig.get(DataSourceConfigKeys.JDBC_URL), null);
        configFactory.hostname(cu.getMainHost().getHost());
        configFactory.port(cu.getMainHost().getPort());
        configFactory.username(cdcConfig.get(DataSourceConfigKeys.USER));
        configFactory.password(cdcConfig.get(DataSourceConfigKeys.PASSWORD));
        String databaseName = cdcConfig.get(DataSourceConfigKeys.DATABASE);
        configFactory.databaseList(databaseName);
        configFactory.serverId(getServerId(config.getJobId()));

        configFactory.includeSchemaChanges(false);

        String includingTables = cdcConfig.get(DataSourceConfigKeys.INCLUDE_TABLES);
        String[] includingTbls =
                Arrays.stream(includingTables.split(","))
                        .map(t -> databaseName + "." + t.trim())
                        .toArray(String[]::new);
        configFactory.tableList(includingTbls);

        String excludingTables = cdcConfig.get(DataSourceConfigKeys.EXCLUDE_TABLES);
        if (StringUtils.isNotEmpty(excludingTables)) {
            String excludingTbls =
                    Arrays.stream(excludingTables.split(","))
                            .map(t -> databaseName + "." + t.trim())
                            .toString();
            configFactory.excludeTableList(excludingTbls);
        }

        // setting startMode
        String startupMode = cdcConfig.get(DataSourceConfigKeys.OFFSET);
        if (DataSourceConfigKeys.OFFSET_INITIAL.equalsIgnoreCase(startupMode)) {
            // do not need set offset when initial
            // configFactory.startupOptions(StartupOptions.initial());
        } else if (DataSourceConfigKeys.OFFSET_EARLIEST.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.earliest());
            BinlogOffset binlogOffset =
                    initializeEffectiveOffset(
                            configFactory, StartupOptions.earliest().binlogOffset);
            configFactory.startupOptions(StartupOptions.specificOffset(binlogOffset));
        } else if (DataSourceConfigKeys.OFFSET_LATEST.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.latest());
            BinlogOffset binlogOffset =
                    initializeEffectiveOffset(configFactory, StartupOptions.latest().binlogOffset);
            configFactory.startupOptions(StartupOptions.specificOffset(binlogOffset));
        } else if (isJson(startupMode)) {
            // start from specific offset
            Map<String, String> offsetMap = toStringMap(startupMode);
            if (MapUtils.isEmpty(offsetMap)) {
                throw new RuntimeException("Incorrect offset " + startupMode);
            }
            if (offsetMap.containsKey(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY)
                    && offsetMap.containsKey(BinlogOffset.BINLOG_POSITION_OFFSET_KEY)) {
                BinlogOffset binlogOffset = new BinlogOffset(offsetMap);
                configFactory.startupOptions(StartupOptions.specificOffset(binlogOffset));
            } else {
                throw new RuntimeException("Incorrect offset " + startupMode);
            }
        } else if (is13Timestamp(startupMode)) {
            // start from timestamp
            Long ts = Long.parseLong(startupMode);
            BinlogOffset binlogOffset =
                    initializeEffectiveOffset(
                            configFactory, StartupOptions.timestamp(ts).binlogOffset);
            configFactory.startupOptions(StartupOptions.specificOffset(binlogOffset));
        } else {
            throw new RuntimeException("Unknown offset " + startupMode);
        }

        Properties jdbcProperteis = new Properties();
        jdbcProperteis.putAll(cu.getOriginalProperties());
        configFactory.jdbcProperties(jdbcProperteis);

        // configFactory.heartbeatInterval(Duration.ofMillis(1));
        if (cdcConfig.containsKey(DataSourceConfigKeys.SPLIT_SIZE)) {
            configFactory.splitSize(
                    Integer.parseInt(cdcConfig.get(DataSourceConfigKeys.SPLIT_SIZE)));
        }

        return configFactory.createConfig(0);
    }

    private static BinlogOffset initializeEffectiveOffset(
            MySqlSourceConfigFactory configFactory, BinlogOffset binlogOffset) {
        MySqlSourceConfig config = configFactory.createConfig(0);
        try (MySqlConnection connection = DebeziumUtils.createMySqlConnection(config)) {
            return BinlogOffsetUtils.initializeEffectiveOffset(binlogOffset, connection, config);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean is13Timestamp(String s) {
        return s != null && s.matches("\\d{13}");
    }

    private static boolean isJson(String str) {
        if (str == null || str.trim().isEmpty()) {
            return false;
        }
        try {
            JsonNode node = mapper.readTree(str);
            return node.isObject();
        } catch (Exception e) {
            return false;
        }
    }

    private static Map<String, String> toStringMap(String json) {
        if (!isJson(json)) {
            return null;
        }

        try {
            return mapper.readValue(json, new TypeReference<Map<String, String>>() {});
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
