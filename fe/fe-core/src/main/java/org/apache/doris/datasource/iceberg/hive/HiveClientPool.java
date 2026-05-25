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

import org.apache.doris.qe.BDPAuthContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.ClientPoolImpl;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.hive.RuntimeMetaException;
import shade.doris.hive.org.apache.thrift.TException;
import shade.doris.hive.org.apache.thrift.transport.TTransportException;

import java.security.PrivilegedAction;

public class HiveClientPool extends ClientPoolImpl<IMetaStoreClient, TException> {
    private static final HiveMetaHookLoader DUMMY_HOOK_LOADER = t -> null;

    private static final String JD_CONF_KEYS = "BEE_COMPUTE,BEE_BUSINESSID,BEE_SN,BEE_SCRIPT_ID,BEE_SCRIPT_V,"
            + "BUFFALO_ENV_ACTION_DEF_ID,BUFFALO_ENV_ACTION_INSTANCE_ID,BUFFALO_ENV_TASK_DEF_ID";

    private static final DynMethods.StaticMethod GET_CLIENT = DynMethods.builder("getProxy")
            .impl(RetryingMetaStoreClient.class, HiveConf.class, HiveMetaHookLoader.class, String.class) // Hive1 & 2
            .impl(RetryingMetaStoreClient.class, Configuration.class, HiveMetaHookLoader.class, String.class) // Hive3
            .buildStatic();

    private final HiveConf hiveConf;

    public HiveClientPool(int poolSize, Configuration conf) {
        super(poolSize, TTransportException.class, false);
        this.hiveConf = new HiveConf(conf, HiveClientPool.class);
        this.hiveConf.addResource(conf);
    }

    @Override
    protected IMetaStoreClient newClient() {
        try {
            try {
                synchronized (HiveClientPool.class) {
                    Preconditions.checkNotNull(BDPAuthContext.get(), "bdp auth info cannot be null");
                    BDPAuthContext bdpAuthContext = BDPAuthContext.get();
                    HiveConf conf = new HiveConf(hiveConf);
                    Preconditions.checkNotNull(bdpAuthContext.getErp(), "erp cannot be null");
                    Preconditions.checkNotNull(bdpAuthContext.getSource(), "source cannot be null");
                    conf.set("BEE_SOURCE", bdpAuthContext.getSource());
                    conf.set("BEE_USER", bdpAuthContext.getErp());
                    conf.set("BEE_SN", bdpAuthContext.getQueryIdStr());
                    conf.set("hive.jd.conf.keys", JD_CONF_KEYS);
                    conf.set("BEE_COMPUTE", "Doris");
                    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(bdpAuthContext.getHadoopUserName(),
                            null, bdpAuthContext.getUserToken());
                    IMetaStoreClient client = ugi.doAs((PrivilegedAction<IMetaStoreClient>) () -> {
                        try {
                            return RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                                HiveMetaStoreClient.class.getName());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    });
                    if (bdpAuthContext.getUserType() != null && bdpAuthContext.getUserType().equalsIgnoreCase(
                            "dev_personal")) {
                        Preconditions.checkState(bdpAuthContext.getBusinessLine() != null,
                                "bdp business info cannot be null");
                        client.setMetaConf("USER_TYPE", bdpAuthContext.getUserType());
                        client.setMetaConf("BUSINESS_LINE", bdpAuthContext.getBusinessLine());
                    }
                    return client;
                }
            } catch (RuntimeException e) {
                // any MetaException would be wrapped into RuntimeException during reflection,
                // so let's double-check type here
                if (e.getCause() instanceof MetaException) {
                    throw (MetaException) e.getCause();
                }
                throw e;
            }
        } catch (MetaException e) {
            throw new RuntimeMetaException(e, "Failed to connect to Hive Metastore");
        } catch (Throwable t) {
            if (t.getMessage().contains("Another instance of Derby may have already booted")) {
                throw new RuntimeMetaException(t, "Failed to start an embedded metastore because embedded "
                    + "Derby supports only one client at a time. To fix this, use a metastore that supports "
                    + "multiple clients.");
            }

            throw new RuntimeMetaException(t, "Failed to connect to Hive Metastore");
        }
    }

    @Override
    protected IMetaStoreClient reconnect(IMetaStoreClient client) {
        try {
            client.close();
            client.reconnect();
        } catch (MetaException e) {
            throw new RuntimeMetaException(e, "Failed to reconnect to Hive Metastore");
        }
        return client;
    }

    @Override
    protected boolean isConnectionException(Exception e) {
        return super.isConnectionException(e) || (e instanceof MetaException && e.getMessage()
            .contains("Got exception: org.apache.thrift.transport.TTransportException"));
    }

    @Override
    protected void close(IMetaStoreClient client) {
        client.close();
    }

    @VisibleForTesting
    HiveConf hiveConf() {
        return hiveConf;
    }
}
