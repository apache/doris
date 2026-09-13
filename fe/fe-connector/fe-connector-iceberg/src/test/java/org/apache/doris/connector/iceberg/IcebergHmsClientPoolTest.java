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

package org.apache.doris.connector.iceberg;

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.ClientPool;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;

public class IcebergHmsClientPoolTest {

    @Test
    public void hmsActionRunsAsMetastoreUser() throws Exception {
        Configuration conf = new Configuration();
        conf.set("hadoop.username", "iceberg-hms-user");
        HadoopAuthenticator auth = HadoopAuthenticator.getHadoopAuthenticator(conf);
        ClientPool<IMetaStoreClient, TException> delegate = new ClientPool<IMetaStoreClient, TException>() {
            @Override
            public <R> R run(Action<R, IMetaStoreClient, TException> action)
                    throws TException, InterruptedException {
                return action.run(null);
            }

            @Override
            public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
                    throws TException, InterruptedException {
                return action.run(null);
            }
        };

        String user = IcebergHmsClientPool.wrap(delegate, auth).run(client -> currentUser());
        Assertions.assertEquals("iceberg-hms-user", user);
    }

    private static String currentUser() {
        try {
            return UserGroupInformation.getCurrentUser().getUserName();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
