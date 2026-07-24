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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonTransaction;
import org.apache.doris.transaction.TransactionManager;
import org.apache.doris.transaction.TransactionType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.objenesis.ObjenesisStd;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Unit tests for {@link PaimonInsertExecutor}.
 *
 * Because the executor hierarchy's constructor expects a fully-initialised
 * planner/coordinator/env, we create an uninitialised instance via Objenesis
 * and inject the minimal set of fields required for the paimon-specific
 * branches to run.
 */
public class PaimonInsertExecutorTest {

    private static PaimonInsertExecutor newBareExecutor() {
        return new ObjenesisStd().newInstance(PaimonInsertExecutor.class);
    }

    private static void setField(Object target, Class<?> declaringClass, String name, Object value) throws Exception {
        Field field = declaringClass.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object invoke(Object target, String name, Class<?>[] argTypes, Object... args) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Method m = clazz.getDeclaredMethod(name, argTypes);
                m.setAccessible(true);
                return m.invoke(target, args);
            } catch (NoSuchMethodException ignored) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchMethodException(name);
    }

    @Test
    public void testTransactionTypeIsPaimon() throws Exception {
        PaimonInsertExecutor executor = newBareExecutor();
        Object type = invoke(executor, "transactionType", new Class<?>[0]);
        Assertions.assertEquals(TransactionType.PAIMON, type);
    }

    @Test
    public void testBeginTransactionPopulatesBaseExternalContext() throws Exception {
        PaimonInsertExecutor executor = newBareExecutor();
        TransactionManager txnMgr = Mockito.mock(TransactionManager.class);
        Mockito.when(txnMgr.begin()).thenReturn(42L);

        // BaseExternalTableInsertExecutor.transactionManager is final; inject via reflection.
        setField(executor, BaseExternalTableInsertExecutor.class, "transactionManager", txnMgr);

        BaseExternalTableInsertCommandContext ctx = new BaseExternalTableInsertCommandContext();
        Optional<InsertCommandContext> insertCtx = Optional.of(ctx);
        setField(executor, AbstractInsertExecutor.class, "insertCtx", insertCtx);

        executor.beginTransaction();

        Assertions.assertEquals(42L, ctx.getTxnId());
        Assertions.assertEquals("doris_txn_42", ctx.getCommitUser());
        // txnId field propagated from super.beginTransaction()
        Field txnIdField = AbstractInsertExecutor.class.getDeclaredField("txnId");
        txnIdField.setAccessible(true);
        Assertions.assertEquals(42L, txnIdField.getLong(executor));
    }

    @Test
    public void testBeginTransactionSkipsWhenContextAbsent() throws Exception {
        PaimonInsertExecutor executor = newBareExecutor();
        TransactionManager txnMgr = Mockito.mock(TransactionManager.class);
        Mockito.when(txnMgr.begin()).thenReturn(7L);
        setField(executor, BaseExternalTableInsertExecutor.class, "transactionManager", txnMgr);
        setField(executor, AbstractInsertExecutor.class, "insertCtx", Optional.empty());

        // Should not throw even though insertCtx is not a BaseExternalTableInsertCommandContext.
        executor.beginTransaction();

        Field txnIdField = AbstractInsertExecutor.class.getDeclaredField("txnId");
        txnIdField.setAccessible(true);
        Assertions.assertEquals(7L, txnIdField.getLong(executor));
    }

    @Test
    public void testBeforeExecDelegatesToPaimonTransaction() throws Exception {
        PaimonInsertExecutor executor = newBareExecutor();

        PaimonTransaction txn = Mockito.mock(PaimonTransaction.class);
        TransactionManager txnMgr = Mockito.mock(TransactionManager.class);
        Mockito.when(txnMgr.getTransaction(100L)).thenReturn(txn);
        setField(executor, BaseExternalTableInsertExecutor.class, "transactionManager", txnMgr);

        Field txnIdField = AbstractInsertExecutor.class.getDeclaredField("txnId");
        txnIdField.setAccessible(true);
        txnIdField.setLong(executor, 100L);

        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        setField(executor, AbstractInsertExecutor.class, "table", table);
        setField(executor, AbstractInsertExecutor.class, "insertCtx", Optional.empty());

        invoke(executor, "beforeExec", new Class<?>[0]);

        Mockito.verify(txn).setTransactionId(100L);
        Mockito.verify(txn).beginInsert(Mockito.eq(table), Mockito.any());
    }

    @Test
    public void testDoBeforeCommitPropagatesUpdatedRows() throws Exception {
        PaimonInsertExecutor executor = newBareExecutor();

        PaimonTransaction txn = Mockito.mock(PaimonTransaction.class);
        Mockito.when(txn.getUpdateCnt()).thenReturn(5L);
        TransactionManager txnMgr = Mockito.mock(TransactionManager.class);
        Mockito.when(txnMgr.getTransaction(9L)).thenReturn(txn);
        setField(executor, BaseExternalTableInsertExecutor.class, "transactionManager", txnMgr);

        Field txnIdField = AbstractInsertExecutor.class.getDeclaredField("txnId");
        txnIdField.setAccessible(true);
        txnIdField.setLong(executor, 9L);

        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        setField(executor, AbstractInsertExecutor.class, "table", table);
        setField(executor, AbstractInsertExecutor.class, "insertCtx", Optional.empty());

        invoke(executor, "doBeforeCommit", new Class<?>[0]);

        Field loadedRowsField = AbstractInsertExecutor.class.getDeclaredField("loadedRows");
        loadedRowsField.setAccessible(true);
        Assertions.assertEquals(5L, loadedRowsField.getLong(executor));
        Mockito.verify(txn).finishInsert(Mockito.eq(table), Mockito.any());
    }

    @Test
    public void testDoBeforeCommitKeepsZeroLoadedRowsWhenNoUpdate() throws Exception {
        PaimonInsertExecutor executor = newBareExecutor();

        PaimonTransaction txn = Mockito.mock(PaimonTransaction.class);
        Mockito.when(txn.getUpdateCnt()).thenReturn(0L);
        TransactionManager txnMgr = Mockito.mock(TransactionManager.class);
        Mockito.when(txnMgr.getTransaction(Mockito.anyLong())).thenReturn(txn);
        setField(executor, BaseExternalTableInsertExecutor.class, "transactionManager", txnMgr);

        Field txnIdField = AbstractInsertExecutor.class.getDeclaredField("txnId");
        txnIdField.setAccessible(true);
        txnIdField.setLong(executor, 3L);

        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        setField(executor, AbstractInsertExecutor.class, "table", table);
        setField(executor, AbstractInsertExecutor.class, "insertCtx", Optional.empty());

        invoke(executor, "doBeforeCommit", new Class<?>[0]);

        Field loadedRowsField = AbstractInsertExecutor.class.getDeclaredField("loadedRows");
        loadedRowsField.setAccessible(true);
        // Untouched when getUpdateCnt() returns 0.
        Assertions.assertEquals(0L, loadedRowsField.getLong(executor));
    }
}
