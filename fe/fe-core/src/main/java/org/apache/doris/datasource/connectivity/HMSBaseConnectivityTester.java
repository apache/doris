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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.datasource.property.metastore.HMSBaseProperties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;

public class HMSBaseConnectivityTester implements MetaConnectivityTester {
    private static final HiveMetaHookLoader DUMMY_HOOK_LOADER = t -> null;
    private final HMSBaseProperties properties;

    public HMSBaseConnectivityTester(HMSBaseProperties properties) {
        this.properties = properties;
    }

    @Override
    public String getTestType() {
        return "HMS";
    }

    @Override
    public void testConnection() throws Exception {
        HiveConf hiveConf = properties.getHiveConf();
        IMetaStoreClient client = null;
        try {
            client = properties.getHmsAuthenticator()
                    .doAs(() -> RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                            org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class.getName()));

            final IMetaStoreClient finalClient = client;
            properties.getHmsAuthenticator()
                    .doAs(finalClient::getAllDatabases);
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception ignored) {
                    // ignore
                }
            }
        }
    }
}
