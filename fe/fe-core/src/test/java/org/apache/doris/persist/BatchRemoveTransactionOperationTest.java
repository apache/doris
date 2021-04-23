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

package org.apache.doris.persist;

import org.apache.doris.common.FeConstants;
import org.apache.doris.meta.MetaContext;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

public class BatchRemoveTransactionOperationTest {
    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeConstants.meta_version);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File("./BatchRemoveTransactionOperationTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        Map<Long, List<Long>> dbTxnIds = Maps.newHashMap();
        dbTxnIds.put(1000L, Lists.newArrayList());
        dbTxnIds.get(1000L).add(1L);
        dbTxnIds.get(1000L).add(2L);
        dbTxnIds.get(1000L).add(3L);
        BatchRemoveTransactionsOperation op = new BatchRemoveTransactionsOperation(dbTxnIds);
        op.write(dos);

        dos.flush();
        dos.close();
        
        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        BatchRemoveTransactionsOperation op2 = BatchRemoveTransactionsOperation.read(dis);
        Assert.assertEquals(1, op2.getDbTxnIds().size());
        Assert.assertEquals(3, op2.getDbTxnIds().get(1000L).size());
        Assert.assertTrue(op2.getDbTxnIds().get(1000L).contains(1L));

        // 3. delete files
        dis.close();
        file.delete();
    }
}
