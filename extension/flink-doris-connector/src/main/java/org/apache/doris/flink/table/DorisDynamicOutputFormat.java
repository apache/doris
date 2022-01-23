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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.StreamLoadException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.table.data.RowData.createFieldGetter;


/**
 * DorisDynamicOutputFormat
 **/
public class DorisDynamicOutputFormat<T> extends RichOutputFormat<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisDynamicOutputFormat.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String COLUMNS_KEY = "columns";
    private static final String FIELD_DELIMITER_KEY = "column_separator";
    private static final String FIELD_DELIMITER_DEFAULT = "\t";
    private static final String LINE_DELIMITER_KEY = "line_delimiter";
    private static final String LINE_DELIMITER_DEFAULT = "\n";
    private static final String FORMAT_KEY = "format";
    private static final String FORMAT_JSON_VALUE = "json";
    private static final String NULL_VALUE = "\\N";
    private static final String ESCAPE_DELIMITERS_KEY = "escape_delimiters";
    private static final String ESCAPE_DELIMITERS_DEFAULT = "false";
    private static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    private static final String UNIQUE_KEYS_TYPE = "UNIQUE_KEYS";
    private final String[] fieldNames;
    private final boolean jsonFormat;
    private final RowData.FieldGetter[] fieldGetters;
    private final List batch = new ArrayList<>();
    private String fieldDelimiter;
    private String lineDelimiter;
    private DorisOptions options;
    private DorisReadOptions readOptions;
    private DorisExecutionOptions executionOptions;
    private DorisStreamLoad dorisStreamLoad;
    private String keysType;

    private transient volatile boolean closed = false;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    public DorisDynamicOutputFormat(DorisOptions option,
                                    DorisReadOptions readOptions,
                                    DorisExecutionOptions executionOptions,
                                    LogicalType[] logicalTypes,
                                    String[] fieldNames) {
        this.options = option;
        this.readOptions = readOptions;
        this.executionOptions = executionOptions;
        this.fieldNames = fieldNames;
        this.jsonFormat = FORMAT_JSON_VALUE.equals(executionOptions.getStreamLoadProp().getProperty(FORMAT_KEY));
        this.keysType = parseKeysType();

        handleStreamloadProp();
        this.fieldGetters = new RowData.FieldGetter[logicalTypes.length];
        for (int i = 0; i < logicalTypes.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[i], i);
        }
    }

    /**
     * parse table keysType
     *
     * @return keysType
     */
    private String parseKeysType() {
        try {
            Schema schema = RestService.getSchema(options, readOptions, LOG);
            return schema.getKeysType();
        } catch (DorisException e) {
            throw new RuntimeException("Failed fetch doris table schema: " + options.getTableIdentifier());
        }
    }

    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    private void handleStreamloadProp() {
        Properties streamLoadProp = executionOptions.getStreamLoadProp();
        boolean ifEscape = Boolean.parseBoolean(streamLoadProp.getProperty(ESCAPE_DELIMITERS_KEY, ESCAPE_DELIMITERS_DEFAULT));
        if (ifEscape) {
            this.fieldDelimiter = escapeString(streamLoadProp.getProperty(FIELD_DELIMITER_KEY,
                    FIELD_DELIMITER_DEFAULT));
            this.lineDelimiter = escapeString(streamLoadProp.getProperty(LINE_DELIMITER_KEY,
                    LINE_DELIMITER_DEFAULT));

            if (streamLoadProp.contains(ESCAPE_DELIMITERS_KEY)) {
                streamLoadProp.remove(ESCAPE_DELIMITERS_KEY);
            }
        } else {
            this.fieldDelimiter = streamLoadProp.getProperty(FIELD_DELIMITER_KEY,
                    FIELD_DELIMITER_DEFAULT);
            this.lineDelimiter = streamLoadProp.getProperty(LINE_DELIMITER_KEY,
                    LINE_DELIMITER_DEFAULT);
        }

        //add column key when fieldNames is not empty
        if (!streamLoadProp.containsKey(COLUMNS_KEY) && fieldNames != null && fieldNames.length > 0) {
            String columns = String.join(",", Arrays.stream(fieldNames).map(item -> String.format("`%s`", item.trim().replace("`", ""))).collect(Collectors.toList()));
            if (enableBatchDelete()) {
                columns = String.format("%s,%s", columns, DORIS_DELETE_SIGN);
            }
            streamLoadProp.put(COLUMNS_KEY, columns);
        }
    }

    private String escapeString(String s) {
        Pattern p = Pattern.compile("\\\\x(\\d{2})");
        Matcher m = p.matcher(s);

        StringBuffer buf = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(buf, String.format("%s", (char) Integer.parseInt(m.group(1))));
        }
        m.appendTail(buf);
        return buf.toString();
    }

    private boolean enableBatchDelete() {
        return executionOptions.getEnableDelete() || UNIQUE_KEYS_TYPE.equals(keysType);
    }

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        dorisStreamLoad = new DorisStreamLoad(
                getBackend(),
                options.getTableIdentifier().split("\\.")[0],
                options.getTableIdentifier().split("\\.")[1],
                options.getUsername(),
                options.getPassword(),
                executionOptions.getStreamLoadProp());
        LOG.info("Streamload BE:{}", dorisStreamLoad.getLoadUrlStr());

        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("doris-streamload-output" +
                    "-format"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (DorisDynamicOutputFormat.this) {
                    if (!closed) {
                        try {
                            flush();
                        } catch (Exception e) {
                            flushException = e;
                        }
                    }
                }
            }, executionOptions.getBatchIntervalMs(), executionOptions.getBatchIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to streamload failed.", flushException);
        }
    }

    @Override
    public synchronized void writeRecord(T row) throws IOException {
        checkFlushException();
        addBatch(row);
        if (executionOptions.getBatchSize() > 0 && batch.size() >= executionOptions.getBatchSize()) {
            flush();
        }
    }

    private void addBatch(T row) {
        if (row instanceof RowData) {
            RowData rowData = (RowData) row;
            Map<String, String> valueMap = new HashMap<>();
            StringJoiner value = new StringJoiner(this.fieldDelimiter);
            for (int i = 0; i < rowData.getArity() && i < fieldGetters.length; ++i) {
                Object field = fieldGetters[i].getFieldOrNull(rowData);
                if (jsonFormat) {
                    String data = field != null ? field.toString() : null;
                    valueMap.put(this.fieldNames[i], data);
                } else {
                    String data = field != null ? field.toString() : NULL_VALUE;
                    value.add(data);
                }
            }
            // add doris delete sign
            if (enableBatchDelete()) {
                if (jsonFormat) {
                    valueMap.put(DORIS_DELETE_SIGN, parseDeleteSign(rowData.getRowKind()));
                } else {
                    value.add(parseDeleteSign(rowData.getRowKind()));
                }
            }
            Object data = jsonFormat ? valueMap : value.toString();
            batch.add(data);
        } else if (row instanceof String) {
            batch.add(row);
        } else {
            throw new RuntimeException("The type of element should be 'RowData' or 'String' only.");
        }
    }

    private String parseDeleteSign(RowKind rowKind) {
        if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
            return "0";
        } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
            return "1";
        } else {
            throw new RuntimeException("Unrecognized row kind:" + rowKind.toString());
        }
    }


    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            try {
                flush();
            } catch (Exception e) {
                LOG.warn("Writing records to doris failed.", e);
                throw new RuntimeException("Writing records to doris failed.", e);
            } finally {
                this.dorisStreamLoad.close();
            }
        }
        checkFlushException();
    }

    public synchronized void flush() throws IOException {
        checkFlushException();
        if (batch.isEmpty()) {
            return;
        }
        String result;
        if (jsonFormat) {
            if (batch.get(0) instanceof String) {
                result = batch.toString();
            } else {
                result = OBJECT_MAPPER.writeValueAsString(batch);
            }
        } else {
            result = String.join(this.lineDelimiter, batch);
        }
        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                dorisStreamLoad.load(result);
                batch.clear();
                break;
            } catch (StreamLoadException e) {
                LOG.error("doris sink error, retry times = {}", i, e);
                if (i >= executionOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    dorisStreamLoad.setHostPort(getBackend());
                    LOG.warn("streamload error,switch be: {}", dorisStreamLoad.getLoadUrlStr(), e);
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    private String getBackend() throws IOException {
        try {
            //get be url from fe
            return RestService.randomBackend(options, readOptions, LOG);
        } catch (IOException | DorisException e) {
            LOG.error("get backends info fail");
            throw new IOException(e);
        }
    }

    /**
     * Builder for {@link DorisDynamicOutputFormat}.
     */
    public static class Builder {
        private DorisOptions.Builder optionsBuilder;
        private DorisReadOptions readOptions;
        private DorisExecutionOptions executionOptions;
        private DataType[] fieldDataTypes;
        private String[] fieldNames;

        public Builder() {
            this.optionsBuilder = DorisOptions.builder();
        }

        public Builder setFenodes(String fenodes) {
            this.optionsBuilder.setFenodes(fenodes);
            return this;
        }

        public Builder setUsername(String username) {
            this.optionsBuilder.setUsername(username);
            return this;
        }

        public Builder setPassword(String password) {
            this.optionsBuilder.setPassword(password);
            return this;
        }

        public Builder setTableIdentifier(String tableIdentifier) {
            this.optionsBuilder.setTableIdentifier(tableIdentifier);
            return this;
        }

        public Builder setReadOptions(DorisReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public Builder setExecutionOptions(DorisExecutionOptions executionOptions) {
            this.executionOptions = executionOptions;
            return this;
        }

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldDataTypes(DataType[] fieldDataTypes) {
            this.fieldDataTypes = fieldDataTypes;
            return this;
        }

        public DorisDynamicOutputFormat build() {
            final LogicalType[] logicalTypes =
                    Arrays.stream(fieldDataTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new);
            return new DorisDynamicOutputFormat(
                    optionsBuilder.build(), readOptions, executionOptions, logicalTypes, fieldNames
            );
        }


    }
}
