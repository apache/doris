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

package org.apache.doris.datasource.iceberg.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.ClientPoolImpl;
import org.apache.iceberg.hive.RuntimeMetaException;
import shade.doris.hive.org.apache.thrift.TException;
import shade.doris.hive.org.apache.thrift.transport.TTransportException;

public class HClientPool extends ClientPoolImpl<IMetaStoreClient, TException> {
    private final HiveConf hiveConf;

    public HClientPool(int poolSize, Configuration conf) {
        super(poolSize, TTransportException.class, false);
        this.hiveConf = new HiveConf(conf, org.apache.iceberg.hive.HiveClientPool.class);
        this.hiveConf.addResource(conf);
    }

    protected IMetaStoreClient newClient() {
        try {
            try {
                return RetryingMetaStoreClient.getProxy(hiveConf, t -> null, HiveMetaStoreClient.class.getName());
            } catch (RuntimeException e) {
                if (e.getCause() instanceof MetaException) {
                    throw (MetaException) e.getCause();
                } else {
                    throw e;
                }
            }
        } catch (MetaException e) {
            throw new RuntimeMetaException(e, "Failed to connect to Hive Metastore");
        } catch (Throwable t) {
            if (t.getMessage().contains("Another instance of Derby may have already booted")) {
                throw new RuntimeMetaException(t,
                        "Embedded Derby supports only one client at a time."
                                + "To fix this, use a metastore that supports multiple clients.");
            } else {
                throw new RuntimeMetaException(t, "Failed to connect to Hive Metastore");
            }
        }
    }

    protected IMetaStoreClient reconnect(IMetaStoreClient client) {
        try {
            client.close();
            client.reconnect();
            return client;
        } catch (MetaException var3) {
            MetaException e = var3;
            throw new RuntimeMetaException(e, "Failed to reconnect to Hive Metastore", new Object[0]);
        }
    }

    protected boolean isConnectionException(Exception e) {
        return super.isConnectionException(e)
            || e instanceof MetaException
            && e.getMessage().contains("Got exception: org.apache.thrift.transport.TTransportException");
    }

    protected void close(IMetaStoreClient client) {
        client.close();
    }
}
