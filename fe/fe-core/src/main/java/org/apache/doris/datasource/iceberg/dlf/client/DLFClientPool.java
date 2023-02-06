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

package org.apache.doris.datasource.iceberg.dlf.client;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.RuntimeMetaException;

public class DLFClientPool extends HiveClientPool {

    private final HiveConf hiveConf;

    public DLFClientPool(int poolSize, Configuration conf) {
        super(poolSize, conf);
        this.hiveConf = new HiveConf(conf, DLFClientPool.class);
        this.hiveConf.addResource(conf);
    }

    @Override
    protected IMetaStoreClient newClient() {
        try {
            return RetryingMetaStoreClient.getProxy(hiveConf, tbl -> null, ProxyMetaStoreClient.class.getName());
        } catch (Exception e) {
            throw new RuntimeMetaException(e, "Failed to connect to Hive Metastore");
        }
    }
}
