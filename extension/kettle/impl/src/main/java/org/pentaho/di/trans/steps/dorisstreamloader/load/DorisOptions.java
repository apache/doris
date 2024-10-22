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

package org.pentaho.di.trans.steps.dorisstreamloader.load;

import com.google.common.base.Preconditions;

import java.util.Properties;

public class DorisOptions {
    public static final int DEFAULT_BUFFER_FLUSH_MAX_BYTES = 100 * 1024 * 1024;
    public static final int DEFAULT_BUFFER_FLUSH_MAX_ROWS = 50000;
    public static final int DEFAULT_MAX_RETRIES = 3;

    private String fenodes;
    private String username;
    private String password;
    private String database;
    private String table;
    private long bufferFlushMaxRows;
    private long bufferFlushMaxBytes;
    private Properties streamLoadProp;
    private int maxRetries;

    public DorisOptions(String fenodes, String username, String password, String database, String table, long bufferFlushMaxRows, long bufferFlushMaxBytes, Properties streamLoadProp, int maxRetries) {
        this.fenodes = fenodes;
        this.username = username;
        this.password = password;
        this.database = database;
        this.table = table;
        this.bufferFlushMaxRows = bufferFlushMaxRows;
        this.bufferFlushMaxBytes = bufferFlushMaxBytes;
        this.streamLoadProp = streamLoadProp;
        this.maxRetries = maxRetries;
    }

    public String getFenodes() {
        return fenodes;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public long getBufferFlushMaxRows() {
        return bufferFlushMaxRows;
    }

    public long getBufferFlushMaxBytes() {
        return bufferFlushMaxBytes;
    }

    public Properties getStreamLoadProp() {
        return streamLoadProp;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String fenodes;
        private String username;
        private String password;
        private String database;
        private String table;
        private long bufferFlushMaxRows = DEFAULT_BUFFER_FLUSH_MAX_ROWS;
        private long bufferFlushMaxBytes = DEFAULT_BUFFER_FLUSH_MAX_BYTES;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private Properties streamLoadProp = new Properties();

        public Builder withFenodes(String fenodes) {
            this.fenodes = fenodes;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder withTable(String table) {
            this.table = table;
            return this;
        }

        public Builder withBufferFlushMaxRows(long bufferFlushMaxRows) {
            this.bufferFlushMaxRows = bufferFlushMaxRows;
            return this;
        }

        public Builder withBufferFlushMaxBytes(long bufferFlushMaxBytes) {
            this.bufferFlushMaxBytes = bufferFlushMaxBytes;
            return this;
        }

        public Builder withStreamLoadProp(Properties streamLoadProp) {
            this.streamLoadProp = streamLoadProp;
            return this;
        }

        public Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public DorisOptions build() {
            Preconditions.checkArgument(fenodes != null, "Fenodes must not be null");
            Preconditions.checkArgument(username != null, "Username must not be null");
            Preconditions.checkArgument(password != null, "Password must not be null");
            Preconditions.checkArgument(database != null, "Database must not be null");
            Preconditions.checkArgument(table != null, "Table must not be null");
            Preconditions.checkArgument(bufferFlushMaxRows >= 10000, "BufferFlushMaxRows must be greater than 10000");
            Preconditions.checkArgument(bufferFlushMaxBytes >= 10 * 1024 * 1024, "BufferFlushMaxBytes must be greater than 10MB");
            Preconditions.checkArgument(maxRetries >= 0, "MaxRetries must be greater than 0");
            return new DorisOptions(fenodes, username, password, database, table, bufferFlushMaxRows, bufferFlushMaxBytes, streamLoadProp, maxRetries);
        }
    }
}
