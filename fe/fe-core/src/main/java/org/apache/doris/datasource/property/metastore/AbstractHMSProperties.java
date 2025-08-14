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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.common.Config;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;

import lombok.Getter;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;

public abstract class AbstractHMSProperties extends MetastoreProperties {

    @Getter
    protected HiveConf hiveConf;

    /**
     * Hadoop authenticator responsible for handling authentication with HDFS/HiveMetastore.
     *
     * By default, it uses simple authentication (HadoopSimpleAuthenticator with SimpleAuthenticationConfig).
     * If Kerberos is required (e.g., when connecting to secure Hive or HDFS clusters), this field must be
     * replaced with a proper Kerberos-based implementation during initialization.
     *
     * Note: In certain environments (such as AWS Glue, which doesn't require Kerberos), the default simple
     * implementation is sufficient and no replacement is needed.
     */
    @Getter
    protected ExecutionAuthenticator executionAuthenticator = new ExecutionAuthenticator() {};


    @Getter
    protected boolean hmsEventsIncrementalSyncEnabled = Config.enable_hms_events_incremental_sync;

    @Getter
    protected int hmsEventsBatchSizePerRpc = Config.hms_events_batch_size_per_rpc;

    /**
     * Base constructor for subclasses to initialize the common state.
     *
     * @param type      metastore type
     * @param origProps original configuration
     */
    protected AbstractHMSProperties(Type type, Map<String, String> origProps) {
        super(type, origProps);
    }
}
