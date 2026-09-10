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

package org.apache.doris.iceberg;

import org.apache.doris.common.classloader.JniScannerClassLoader;
import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.utils.OffHeap;

import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class IcebergScannerSerializationTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "type_name", Types.StringType.get()));

    @Test
    void readsTaskContainingPrimitiveClassThroughScanner() throws Exception {
        assertScannedType(new ClassBearingTask(int.class), "int");
    }

    @ParameterizedTest
    @MethodSource("otherDescriptorTypes")
    void readsOtherPrimitiveArrayAndOrdinaryClassDescriptors(Class<?> type) throws Exception {
        assertScannedType(new ClassBearingTask(type), type.getName());
    }

    private static Stream<Class<?>> otherDescriptorTypes() {
        return Stream.of(boolean.class, byte.class, char.class, short.class, long.class, float.class,
                double.class, void.class, int[].class, String.class, String[].class);
    }

    @Test
    void readsCustomTaskVisibleOnlyToTheScannerLoader() throws Exception {
        assertScannedTypeWithIsolatedLoader(new ClassBearingTask(String.class), "java.lang.String");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void readsPublicAndPrivateProxyInterfacesInTheScannerLoader(boolean privateInterface) throws Exception {
        Class<?> projectionType = privateInterface ? PrivateTypeProjection.class : TypeProjection.class;
        TypeProjection projection = (TypeProjection) Proxy.newProxyInstance(
                projectionType.getClassLoader(), new Class<?>[] {projectionType}, new ProjectionHandler());
        assertScannedTypeWithIsolatedLoader(new ClassBearingTask(String.class, projection),
                "proxy:java.lang.String");
    }

    private static void assertScannedType(ClassBearingTask task, String expected) throws Exception {
        OffHeap.setTesting();
        assertScannerRows(new IcebergSysTableJniScanner(4, scannerParams(task)), expected);
    }

    private static void assertScannedTypeWithIsolatedLoader(ClassBearingTask task, String expected)
            throws Exception {
        ClassLoader caller = Thread.currentThread().getContextClassLoader();
        // Reproduce a child-only task/provider namespace while sharing the Iceberg and JNI APIs
        // with the parent. A flat Maven classpath would hide a wrong proxy classloader choice.
        ClassLoader parent = new ClassLoader(IcebergScannerSerializationTest.class.getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("org.apache.doris.iceberg.")) {
                    throw new ClassNotFoundException(name);
                }
                return super.loadClass(name, resolve);
            }
        };
        URL scannerClasses = IcebergSysTableJniScanner.class.getProtectionDomain().getCodeSource().getLocation();
        URL testClasses = IcebergScannerSerializationTest.class.getProtectionDomain().getCodeSource().getLocation();
        try (URLClassLoader loader = new JniScannerClassLoader(
                "iceberg-metadata-scanner", List.of(scannerClasses, testClasses), parent)) {
            Assertions.assertThrows(ClassNotFoundException.class,
                    () -> parent.loadClass(ClassBearingTask.class.getName()));
            Class<?> scannerClass = loader.loadClass(IcebergSysTableJniScanner.class.getName());
            Assertions.assertSame(loader, scannerClass.getClassLoader());
            Assertions.assertSame(loader, loader.loadClass(TypeProjection.class.getName()).getClassLoader());
            Assertions.assertSame(parent.loadClass(DataTask.class.getName()),
                    loader.loadClass(DataTask.class.getName()));
            Assertions.assertNotSame(ClassBearingTask.class, loader.loadClass(ClassBearingTask.class.getName()));
            OffHeap.setTesting();
            JniScanner scanner = (JniScanner) scannerClass.getConstructor(int.class, Map.class)
                    .newInstance(4, scannerParams(task));
            assertScannerRows(scanner, expected);
        } finally {
            Assertions.assertSame(caller, Thread.currentThread().getContextClassLoader(),
                    "Deserialization and scanning must restore the caller's TCCL");
        }
    }

    private static Map<String, String> scannerParams(ClassBearingTask task) {
        return Map.of("serialized_split", SerializationUtil.serializeToBase64(task),
                "required_fields", "type_name", "required_types", "string");
    }

    private static void assertScannerRows(JniScanner scanner, String expected) throws Exception {
        try {
            scanner.open();
            Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
            Assertions.assertArrayEquals(new Object[] {expected}, scanner.getTable().getMaterializedData()[0]);
            scanner.resetTable();
            Assertions.assertEquals(0, scanner.getNextBatchMeta());
        } finally {
            try {
                scanner.releaseTable();
            } finally {
                scanner.close();
            }
        }
    }

    public interface TypeProjection extends Serializable {
        String name(Class<?> type);
    }

    private interface PrivateTypeProjection extends TypeProjection {
    }

    private static final class ProjectionHandler implements InvocationHandler, Serializable {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            Assertions.assertEquals("name", method.getName());
            return "proxy:" + ((Class<?>) args[0]).getName();
        }
    }

    // A custom task/FileIO may serialize Class descriptors as well as ordinary values. Exercise
    // the scanner's real deserialization boundary instead of calling its private helper.
    private static final class ClassBearingTask extends BaseFileScanTask implements DataTask {
        private final Class<?> type;
        private final TypeProjection projection;

        private ClassBearingTask(Class<?> type) {
            this(type, null);
        }

        private ClassBearingTask(Class<?> type, TypeProjection projection) {
            super(DataFiles.builder(PartitionSpec.unpartitioned()).withPath("memory:///metadata.parquet")
                            .withFileSizeInBytes(1).withRecordCount(1).build(),
                    new DeleteFile[0], SchemaParser.toJson(SCHEMA),
                    PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
                    ResidualEvaluator.unpartitioned(Expressions.alwaysTrue()));
            this.type = type;
            this.projection = projection;
        }

        @Override
        public CloseableIterable<StructLike> rows() {
            return CloseableIterable.withNoopClose(List.of(new StructLike() {
                @Override
                public int size() {
                    return 1;
                }

                @Override
                public <T> T get(int pos, Class<T> javaClass) {
                    Assertions.assertEquals(0, pos);
                    return javaClass.cast(projection == null ? type.getName() : projection.name(type));
                }

                @Override
                public <T> void set(int pos, T value) {
                    throw new UnsupportedOperationException("Read-only test row");
                }
            }));
        }
    }
}
