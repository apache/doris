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

package org.apache.doris.connector.iceberg.dlf.client;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.RuntimeMetaException;

/** Hive client pool whose connections use the Aliyun DLF proxy client. */
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
            // The DLF Hive 3 artifact retains the HiveConf constructor, while Hive 3's default
            // retrying-client factory looks up a Configuration constructor by exact signature.
            return RetryingMetaStoreClient.getProxy(hiveConf,
                    new Class<?>[] {HiveConf.class, HiveMetaHookLoader.class, Boolean.class},
                    new Object[] {hiveConf, (HiveMetaHookLoader) tbl -> null, true},
                    ProxyMetaStoreClient.class.getName());
        } catch (Exception e) {
            throw new RuntimeMetaException(e, "Failed to connect to DLF Metastore");
        }
    }
}
