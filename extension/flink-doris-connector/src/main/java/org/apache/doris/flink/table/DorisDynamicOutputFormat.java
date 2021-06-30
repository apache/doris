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
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.StreamLoadException;
import org.apache.doris.flink.rest.RestService;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


/**
 * DorisDynamicOutputFormat
 **/
public class DorisDynamicOutputFormat extends RichOutputFormat<RowData>  {

    private static final Logger LOG = LoggerFactory.getLogger(DorisDynamicOutputFormat.class);

    private  DorisOptions options ;
    private  DorisReadOptions readOptions;
    private  DorisExecutionOptions executionOptions;
    private DorisStreamLoad dorisStreamLoad;
    private final String fieldDelimiter = "\t";
    private final String lineDelimiter = "\n";
    private final List<String> batch = new ArrayList<>();
    private transient volatile boolean closed = false;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    public DorisDynamicOutputFormat(DorisOptions option,DorisReadOptions readOptions,DorisExecutionOptions executionOptions) {
        this.options = option;
        this.readOptions = readOptions;
        this.executionOptions = executionOptions;
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
                options.getPassword());
        LOG.info("Streamload BE:{}",dorisStreamLoad.getLoadUrlStr());

        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("doris-streamload-output-format"));
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
    public synchronized void writeRecord(RowData row) throws IOException {
        checkFlushException();

        addBatch(row);
        if (executionOptions.getBatchSize() > 0 && batch.size() >= executionOptions.getBatchSize()) {
            flush();
        }
    }

    private void addBatch(RowData row) {
        StringJoiner value = new StringJoiner(this.fieldDelimiter);
        GenericRowData rowData = (GenericRowData) row;
        for(int i = 0; i < row.getArity(); ++i) {
            value.add(rowData.getField(i).toString());
        }
        batch.add(value.toString());
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
            }
        }
        checkFlushException();
    }

    public synchronized void flush() throws IOException {
        checkFlushException();
        if(batch.isEmpty()){
            return;
        }
        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                dorisStreamLoad.load(String.join(lineDelimiter,batch));
                batch.clear();
                break;
            } catch (StreamLoadException e) {
                LOG.error("doris sink error, retry times = {}", i, e);
                if (i >= executionOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    dorisStreamLoad.setHostPort(getBackend());
                    LOG.warn("streamload error,switch be: {}",dorisStreamLoad.getLoadUrlStr(), e);
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }


    private String getBackend() throws IOException{
        try {
            //get be url from fe
           return  RestService.randomBackend(options,readOptions, LOG);
        } catch (IOException | DorisException e) {
            LOG.error("get backends info fail");
            throw new IOException(e);
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

    /**
     * Builder for {@link DorisDynamicOutputFormat}.
     */
    public static class Builder {
        private DorisOptions.Builder optionsBuilder;
        private  DorisReadOptions readOptions;
        private  DorisExecutionOptions executionOptions;

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

        public DorisDynamicOutputFormat build() {
            return new DorisDynamicOutputFormat(
                    optionsBuilder.build(),readOptions,executionOptions
            );
        }
    }
}
