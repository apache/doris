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

package org.apache.doris.transaction;

import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TransactionStateTest {

    private static String fileName = "./TransactionStateTest";

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testSerDe() throws IOException {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        UUID uuid = UUID.randomUUID();
        TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label123", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"), 50000L,
                60 * 1000L);

        Set<Long> errorReplicas = Sets.newHashSet();
        for (long i = 0; i < 100; i++) {
            errorReplicas.add(i);
        }
        transactionState.setErrorReplicas(errorReplicas);

        Map<Long, Set<Long>> commitErrorReplicas = Maps.newHashMap();
        for (long i = 1000; i < 2000; i++) {
            Set<Long> replicas = Sets.newHashSet();
            for (long j = 0; j < 10; j++) {
                replicas.add(i + j);
            }
            commitErrorReplicas.put(i, replicas);
        }
        transactionState.setCommitErrorReplicas(commitErrorReplicas);

        Map<Long, Set<Long>> publishErrorReplicas = Maps.newHashMap();
        for (long i = 2000; i < 3000; i++) {
            Set<Long> replicas = Sets.newHashSet();
            for (long j = 0; j < 10; j++) {
                replicas.add(i + j);
            }
            publishErrorReplicas.put(i, replicas);
        }
        transactionState.setPublishErrorReplicas(publishErrorReplicas);

        transactionState.setCommitErrorReplicas(commitErrorReplicas);
        transactionState.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TransactionState readTransactionState = new TransactionState();
        readTransactionState.readFields(in);
        in.close();

        Assert.assertEquals(transactionState.getCoordinator().ip, readTransactionState.getCoordinator().ip);
        Assert.assertEquals(errorReplicas, readTransactionState.getErrorReplicas());
        Assert.assertEquals(commitErrorReplicas, readTransactionState.getCommitErrorReplicas());
        Assert.assertEquals(publishErrorReplicas, readTransactionState.getPublishErrorReplicas());
    }
}
