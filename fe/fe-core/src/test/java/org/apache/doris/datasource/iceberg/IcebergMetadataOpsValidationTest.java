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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalCatalog;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

public class IcebergMetadataOpsValidationTest {

    private IcebergMetadataOps ops;
    private Method validateForModifyColumnMethod;
    private Method validateForModifyComplexColumnMethod;

    @Before
    public void setUp() throws Exception {
        ExternalCatalog dorisCatalog = Mockito.mock(ExternalCatalog.class);
        Catalog icebergCatalog = Mockito.mock(Catalog.class,
                Mockito.withSettings().extraInterfaces(SupportsNamespaces.class));
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        ops = new IcebergMetadataOps(dorisCatalog, icebergCatalog);

        validateForModifyColumnMethod = IcebergMetadataOps.class.getDeclaredMethod(
                "validateForModifyColumn", Column.class, NestedField.class);
        validateForModifyColumnMethod.setAccessible(true);
        validateForModifyComplexColumnMethod = IcebergMetadataOps.class.getDeclaredMethod(
                "validateForModifyComplexColumn", Column.class, NestedField.class);
        validateForModifyComplexColumnMethod.setAccessible(true);
    }

    @Test
    public void testValidateForModifyColumnRejectsComplexType() {
        Column column = new Column("arr_i", ArrayType.create(Type.INT, true), true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i", Types.IntegerType.get());
        assertUserException(() -> invokeValidateForModifyColumn(column, currentCol),
                "Modify column type to non-primitive type is not supported");
    }

    @Test
    public void testValidateForModifyColumnRejectsNullableToNotNull() {
        Column column = new Column("int_col", Type.INT, false);
        NestedField currentCol = Types.NestedField.optional(1, "int_col", Types.IntegerType.get());
        assertUserException(() -> invokeValidateForModifyColumn(column, currentCol),
                "Can not change nullable column int_col to not null");
    }

    @Test
    public void testValidateForModifyColumnSuccess() throws Throwable {
        Column column = new Column("int_col", Type.INT, true);
        NestedField currentCol = Types.NestedField.required(1, "int_col", Types.IntegerType.get());
        invokeValidateForModifyColumn(column, currentCol);
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsPrimitiveType() {
        Column column = new Column("arr_i", Type.INT, true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Modify column type to non-complex type is not supported");
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsIncompatibleNestedType() {
        Column column = new Column("arr_i", ArrayType.create(Type.SMALLINT, true), true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Cannot change int to smallint in nested types");
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsPrimitiveToComplex() {
        Column column = new Column("arr_i", ArrayType.create(Type.INT, true), true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i", Types.IntegerType.get());
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Modify column type from non-complex to complex is not supported");
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsDifferentComplexCategory() {
        Column column = new Column("arr_i", new MapType(Type.INT, Type.INT), true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Cannot change complex column type category");
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsNullableToNotNull() {
        Column column = new Column("arr_i", ArrayType.create(Type.INT, true), false);
        NestedField currentCol = Types.NestedField.optional(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Cannot change nullable column arr_i to not null");
    }

    @Test
    public void testValidateForModifyComplexColumnRejectsDefaultValue() {
        Column column = new Column("arr_i", ArrayType.create(Type.INT, true),
                false, null, true, "1", "");
        NestedField currentCol = Types.NestedField.required(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        assertUserException(() -> invokeValidateForModifyComplexColumn(column, currentCol),
                "Complex type default value only supports NULL");
    }

    @Test
    public void testValidateForModifyComplexColumnSuccess() throws Throwable {
        Column column = new Column("arr_i", ArrayType.create(Type.BIGINT, true), true);
        NestedField currentCol = Types.NestedField.required(1, "arr_i",
                Types.ListType.ofOptional(2, Types.IntegerType.get()));
        invokeValidateForModifyComplexColumn(column, currentCol);
    }

    private void invokeValidateForModifyColumn(Column column, NestedField currentCol) throws Throwable {
        invokeValidationMethod(validateForModifyColumnMethod, column, currentCol);
    }

    private void invokeValidateForModifyComplexColumn(Column column, NestedField currentCol) throws Throwable {
        invokeValidationMethod(validateForModifyComplexColumnMethod, column, currentCol);
    }

    private void invokeValidationMethod(Method method, Column column, NestedField currentCol) throws Throwable {
        try {
            method.invoke(ops, column, currentCol);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private void assertUserException(ThrowingRunnable runnable, String expectedMessage) {
        try {
            runnable.run();
            Assert.fail("expected UserException");
        } catch (Throwable t) {
            Assert.assertTrue(t instanceof UserException);
            Assert.assertTrue(t.getMessage().contains(expectedMessage));
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Throwable;
    }
}
