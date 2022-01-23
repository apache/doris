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
package org.apache.doris.flink.cfg;


import java.io.Serializable;
import java.util.Properties;


/**
 * Options for the Doris stream connector.
 */
public class DorisStreamOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private Properties prop;
    private DorisOptions options;
    private DorisReadOptions readOptions;

    public DorisStreamOptions(Properties prop) {
        this.prop = prop;
        init();
    }

    /**
     * convert DorisStreamOptions to DorisOptions and DorisReadOptions
     */
    private void init() {
        DorisOptions.Builder optionsBuilder = DorisOptions.builder()
                .setFenodes(prop.getProperty(ConfigurationOptions.DORIS_FENODES))
                .setUsername(prop.getProperty(ConfigurationOptions.DORIS_USER))
                .setPassword(prop.getProperty(ConfigurationOptions.DORIS_PASSWORD))
                .setTableIdentifier(prop.getProperty(ConfigurationOptions.TABLE_IDENTIFIER));

        DorisReadOptions.Builder readOptionsBuilder = DorisReadOptions.builder()
                .setDeserializeArrowAsync(Boolean.valueOf(prop.getProperty(ConfigurationOptions.DORIS_DESERIALIZE_ARROW_ASYNC, ConfigurationOptions.DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT.toString())))
                .setDeserializeQueueSize(Integer.valueOf(prop.getProperty(ConfigurationOptions.DORIS_DESERIALIZE_QUEUE_SIZE, ConfigurationOptions.DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT.toString())))
                .setExecMemLimit(Long.valueOf(prop.getProperty(ConfigurationOptions.DORIS_EXEC_MEM_LIMIT, ConfigurationOptions.DORIS_EXEC_MEM_LIMIT_DEFAULT.toString())))
                .setFilterQuery(prop.getProperty(ConfigurationOptions.DORIS_FILTER_QUERY))
                .setReadFields(prop.getProperty(ConfigurationOptions.DORIS_READ_FIELD))
                .setRequestQueryTimeoutS(Integer.valueOf(prop.getProperty(ConfigurationOptions.DORIS_REQUEST_QUERY_TIMEOUT_S, ConfigurationOptions.DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT.toString())))
                .setRequestBatchSize(Integer.valueOf(prop.getProperty(ConfigurationOptions.DORIS_BATCH_SIZE, ConfigurationOptions.DORIS_BATCH_SIZE_DEFAULT.toString())))
                .setRequestConnectTimeoutMs(Integer.valueOf(prop.getProperty(ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS, ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT.toString())))
                .setRequestReadTimeoutMs(Integer.valueOf(prop.getProperty(ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS, ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT.toString())))
                .setRequestRetries(Integer.valueOf(prop.getProperty(ConfigurationOptions.DORIS_REQUEST_RETRIES, ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT.toString())))
                .setRequestTabletSize(Integer.valueOf(prop.getProperty(ConfigurationOptions.DORIS_TABLET_SIZE, ConfigurationOptions.DORIS_TABLET_SIZE_DEFAULT.toString())));

        this.options = optionsBuilder.build();
        this.readOptions = readOptionsBuilder.build();

    }

    public DorisOptions getOptions() {
        return options;
    }

    public DorisReadOptions getReadOptions() {
        return readOptions;
    }
}
