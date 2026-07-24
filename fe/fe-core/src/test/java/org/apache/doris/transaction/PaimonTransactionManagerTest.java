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

import org.apache.doris.datasource.paimon.PaimonMetadataOps;
import org.apache.doris.datasource.paimon.PaimonTransaction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class PaimonTransactionManagerTest {

    @Test
    public void testCreateTransactionReturnsPaimonTransaction() throws Exception {
        PaimonMetadataOps ops = Mockito.mock(PaimonMetadataOps.class);
        PaimonTransactionManager manager = new PaimonTransactionManager(ops);

        Method createTxn = PaimonTransactionManager.class.getDeclaredMethod("createTransaction");
        createTxn.setAccessible(true);
        Object txn = createTxn.invoke(manager);
        Assertions.assertNotNull(txn);
        Assertions.assertTrue(txn instanceof PaimonTransaction);
    }

    @Test
    public void testHoldsProvidedOps() throws Exception {
        PaimonMetadataOps ops = Mockito.mock(PaimonMetadataOps.class);
        PaimonTransactionManager manager = new PaimonTransactionManager(ops);

        Field field = PaimonTransactionManager.class.getDeclaredField("paimonOps");
        field.setAccessible(true);
        Assertions.assertSame(ops, field.get(manager));
    }

    @Test
    public void testMultipleTransactionsAreDistinct() throws Exception {
        PaimonMetadataOps ops = Mockito.mock(PaimonMetadataOps.class);
        PaimonTransactionManager manager = new PaimonTransactionManager(ops);

        Method createTxn = PaimonTransactionManager.class.getDeclaredMethod("createTransaction");
        createTxn.setAccessible(true);
        Object txn1 = createTxn.invoke(manager);
        Object txn2 = createTxn.invoke(manager);
        Assertions.assertNotNull(txn1);
        Assertions.assertNotNull(txn2);
        Assertions.assertNotSame(txn1, txn2);
    }
}
