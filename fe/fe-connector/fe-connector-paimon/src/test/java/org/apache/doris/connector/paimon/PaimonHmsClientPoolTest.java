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

package org.apache.doris.connector.paimon;

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;

public class PaimonHmsClientPoolTest {

    @Test
    public void hmsActionRunsAsMetastoreUser() throws Exception {
        Configuration conf = new Configuration();
        conf.set("hadoop.username", "paimon-hms-user");
        HadoopAuthenticator auth = HadoopAuthenticator.getHadoopAuthenticator(conf);
        ClientPool<IMetaStoreClient, TException> delegate = new ClientPool<IMetaStoreClient, TException>() {
            @Override
            public <R> R run(Action<R, IMetaStoreClient, TException> action)
                    throws TException, InterruptedException {
                return action.run(null);
            }

            @Override
            public void execute(ExecuteAction<IMetaStoreClient, TException> action)
                    throws TException, InterruptedException {
                action.run(null);
            }
        };

        String user = PaimonHmsClientPool.wrap(delegate, auth).run(client -> currentUser());
        Assertions.assertEquals("paimon-hms-user", user);
    }

    @Test
    public void installInitializesLazyHiveClientPoolBeforeWrapping() throws Exception {
        HiveConf hiveConf = new HiveConf();
        // CachedClientPool otherwise eagerly connects during construction. SASL mode keeps the
        // test focused on HiveCatalog's lazy pool initialization without requiring a live HMS.
        hiveConf.setBoolean("hive.metastore.sasl.enabled", true);
        HiveCatalog catalog = new HiveCatalog(
                LocalFileIO.create(), hiveConf,
                "org.apache.hadoop.hive.metastore.HiveMetaStoreClient", "file:///tmp/warehouse");
        Configuration conf = new Configuration();
        conf.set("hadoop.username", "paimon-hms-user");

        PaimonHmsClientPool.install(catalog, HadoopAuthenticator.getHadoopAuthenticator(conf));

        Field clients = HiveCatalog.class.getDeclaredField("clients");
        clients.setAccessible(true);
        Assertions.assertInstanceOf(PaimonHmsClientPool.class, clients.get(catalog));
    }

    private static String currentUser() {
        try {
            return UserGroupInformation.getCurrentUser().getUserName();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
