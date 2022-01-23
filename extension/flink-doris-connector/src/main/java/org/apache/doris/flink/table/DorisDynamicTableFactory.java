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

package org.apache.doris.flink.table;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_BATCH_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_EXEC_MEM_LIMIT_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_DEFAULT;

/**
 * The {@link DorisDynamicTableFactory} translates the catalog table to a table source.
 *
 * <p>Because the table source requires a decoding format, we are discovering the format using the
 * provided {@link FactoryUtil} for convenience.
 */
public final class DorisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final ConfigOption<String> FENODES = ConfigOptions.key("fenodes").stringType().noDefaultValue().withDescription("doris fe http address.");
    public static final ConfigOption<String> TABLE_IDENTIFIER = ConfigOptions.key("table.identifier").stringType().noDefaultValue().withDescription("the jdbc table name.");
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username").stringType().noDefaultValue().withDescription("the jdbc user name.");
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue().withDescription("the jdbc password.");
    // Prefix for Doris StreamLoad specific properties.
    public static final String STREAM_LOAD_PROP_PREFIX = "sink.properties.";
    // doris options
    private static final ConfigOption<String> DORIS_READ_FIELD = ConfigOptions
            .key("doris.read.field")
            .stringType()
            .noDefaultValue()
            .withDescription("List of column names in the Doris table, separated by commas");
    private static final ConfigOption<String> DORIS_FILTER_QUERY = ConfigOptions
            .key("doris.filter.query")
            .stringType()
            .noDefaultValue()
            .withDescription("Filter expression of the query, which is transparently transmitted to Doris. Doris uses this expression to complete source-side data filtering");
    private static final ConfigOption<Integer> DORIS_TABLET_SIZE = ConfigOptions
            .key("doris.request.tablet.size")
            .intType()
            .defaultValue(DORIS_TABLET_SIZE_DEFAULT)
            .withDescription("");
    private static final ConfigOption<Integer> DORIS_REQUEST_CONNECT_TIMEOUT_MS = ConfigOptions
            .key("doris.request.connect.timeout.ms")
            .intType()
            .defaultValue(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
            .withDescription("");
    private static final ConfigOption<Integer> DORIS_REQUEST_READ_TIMEOUT_MS = ConfigOptions
            .key("doris.request.read.timeout.ms")
            .intType()
            .defaultValue(DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
            .withDescription("");
    private static final ConfigOption<Integer> DORIS_REQUEST_QUERY_TIMEOUT_S = ConfigOptions
            .key("doris.request.query.timeout.s")
            .intType()
            .defaultValue(DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
            .withDescription("");
    private static final ConfigOption<Integer> DORIS_REQUEST_RETRIES = ConfigOptions
            .key("doris.request.retries")
            .intType()
            .defaultValue(DORIS_REQUEST_RETRIES_DEFAULT)
            .withDescription("");
    private static final ConfigOption<Boolean> DORIS_DESERIALIZE_ARROW_ASYNC = ConfigOptions
            .key("doris.deserialize.arrow.async")
            .booleanType()
            .defaultValue(DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
            .withDescription("");
    private static final ConfigOption<Integer> DORIS_DESERIALIZE_QUEUE_SIZE = ConfigOptions
            .key("doris.request.retriesdoris.deserialize.queue.size")
            .intType()
            .defaultValue(DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
            .withDescription("");
    private static final ConfigOption<Integer> DORIS_BATCH_SIZE = ConfigOptions
            .key("doris.batch.size")
            .intType()
            .defaultValue(DORIS_BATCH_SIZE_DEFAULT)
            .withDescription("");
    private static final ConfigOption<Long> DORIS_EXEC_MEM_LIMIT = ConfigOptions
            .key("doris.exec.mem.limit")
            .longType()
            .defaultValue(DORIS_EXEC_MEM_LIMIT_DEFAULT)
            .withDescription("");
    // flink write config options
    private static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
            .key("sink.batch.size")
            .intType()
            .defaultValue(100)
            .withDescription("the flush max size (includes all append, upsert and delete records), over this number" +
                    " of records, will flush data. The default value is 100.");
    private static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
            .key("sink.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("the max retry times if writing records to database failed.");
    private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
            .key("sink.batch.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription("the flush interval mills, over this time, asynchronous threads will flush data. The " +
                    "default value is 1s.");
    private static final ConfigOption<Boolean> SINK_ENABLE_DELETE = ConfigOptions
            .key("sink.enable-delete")
            .booleanType()
            .defaultValue(true)
            .withDescription("whether to enable the delete function");

    @Override
    public String factoryIdentifier() {
        return "doris"; // used for matching to `connector = '...'`
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FENODES);
        options.add(TABLE_IDENTIFIER);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FENODES);
        options.add(TABLE_IDENTIFIER);
        options.add(USERNAME);
        options.add(PASSWORD);

        options.add(DORIS_READ_FIELD);
        options.add(DORIS_FILTER_QUERY);
        options.add(DORIS_TABLET_SIZE);
        options.add(DORIS_REQUEST_CONNECT_TIMEOUT_MS);
        options.add(DORIS_REQUEST_READ_TIMEOUT_MS);
        options.add(DORIS_REQUEST_QUERY_TIMEOUT_S);
        options.add(DORIS_REQUEST_RETRIES);
        options.add(DORIS_DESERIALIZE_ARROW_ASYNC);
        options.add(DORIS_DESERIALIZE_QUEUE_SIZE);
        options.add(DORIS_BATCH_SIZE);
        options.add(DORIS_EXEC_MEM_LIMIT);

        options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_BUFFER_FLUSH_INTERVAL);
        options.add(SINK_ENABLE_DELETE);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // validate all options
        helper.validateExcept(STREAM_LOAD_PROP_PREFIX);
        // get the validated options
        final ReadableConfig options = helper.getOptions();
        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        // create and return dynamic table source
        return new DorisDynamicTableSource(
                getDorisOptions(helper.getOptions()),
                getDorisReadOptions(helper.getOptions()),
                physicalSchema);
    }

    private DorisOptions getDorisOptions(ReadableConfig readableConfig) {
        final String fenodes = readableConfig.get(FENODES);
        final DorisOptions.Builder builder = DorisOptions.builder()
                .setFenodes(fenodes)
                .setTableIdentifier(readableConfig.get(TABLE_IDENTIFIER));

        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

    private DorisReadOptions getDorisReadOptions(ReadableConfig readableConfig) {
        final DorisReadOptions.Builder builder = DorisReadOptions.builder();
        builder.setDeserializeArrowAsync(readableConfig.get(DORIS_DESERIALIZE_ARROW_ASYNC))
                .setDeserializeQueueSize(readableConfig.get(DORIS_DESERIALIZE_QUEUE_SIZE))
                .setExecMemLimit(readableConfig.get(DORIS_EXEC_MEM_LIMIT))
                .setFilterQuery(readableConfig.get(DORIS_FILTER_QUERY))
                .setReadFields(readableConfig.get(DORIS_READ_FIELD))
                .setRequestQueryTimeoutS(readableConfig.get(DORIS_REQUEST_QUERY_TIMEOUT_S))
                .setRequestBatchSize(readableConfig.get(DORIS_BATCH_SIZE))
                .setRequestConnectTimeoutMs(readableConfig.get(DORIS_REQUEST_CONNECT_TIMEOUT_MS))
                .setRequestReadTimeoutMs(readableConfig.get(DORIS_REQUEST_READ_TIMEOUT_MS))
                .setRequestRetries(readableConfig.get(DORIS_REQUEST_RETRIES))
                .setRequestTabletSize(readableConfig.get(DORIS_TABLET_SIZE));
        return builder.build();
    }

    private DorisExecutionOptions getDorisExecutionOptions(ReadableConfig readableConfig, Properties streamLoadProp) {
        final DorisExecutionOptions.Builder builder = DorisExecutionOptions.builder();
        builder.setBatchSize(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        builder.setMaxRetries(readableConfig.get(SINK_MAX_RETRIES));
        builder.setBatchIntervalMs(readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        builder.setStreamLoadProp(streamLoadProp);
        builder.setEnableDelete(readableConfig.get(SINK_ENABLE_DELETE));
        return builder.build();
    }

    private Properties getStreamLoadProp(Map<String, String> tableOptions) {
        final Properties streamLoadProp = new Properties();

        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (entry.getKey().startsWith(STREAM_LOAD_PROP_PREFIX)) {
                String subKey = entry.getKey().substring(STREAM_LOAD_PROP_PREFIX.length());
                streamLoadProp.put(subKey, entry.getValue());
            }
        }
        return streamLoadProp;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // validate all options
        helper.validateExcept(STREAM_LOAD_PROP_PREFIX);

        Properties streamLoadProp = getStreamLoadProp(context.getCatalogTable().getOptions());
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        // create and return dynamic table source
        return new DorisDynamicTableSink(
                getDorisOptions(helper.getOptions()),
                getDorisReadOptions(helper.getOptions()),
                getDorisExecutionOptions(helper.getOptions(), streamLoadProp),
                physicalSchema
        );
    }
}
