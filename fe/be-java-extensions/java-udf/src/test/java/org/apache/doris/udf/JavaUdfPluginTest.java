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

package org.apache.doris.udf;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.UdfExecutorFactory;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TFunctionBinaryType;
import org.apache.doris.thrift.TFunctionName;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScalarFunction;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * What this plugin promises BE and what it promises a user function.
 *
 * <p>Everything here loads user classes out of a jar built on the spot rather than off the test
 * classpath, because the questions worth asking are all questions about classloaders and a class
 * the test classpath already holds answers them wrongly.
 *
 * <p>What these tests do NOT establish is that the deployed plugin directory holds everything the
 * plugin needs: surefire puts `provided` dependencies on the test classpath, so a dependency
 * wrongly scoped stays invisible here. Only loading out of a real deployment directory shows that.
 */
public class JavaUdfPluginTest {

    @Test
    public void publishesItsExecutorsUnderTheNamesBeAddresses() {
        DorisPlugin plugin = new JavaUdfPlugin();

        Set<String> names = new HashSet<>();
        for (UdfExecutorFactory factory : plugin.getUdfExecutorFactories()) {
            names.add(factory.getName());
        }

        // These two strings are BE's addresses for this plugin - Jni::plugin::JAVA_UDF_SCALAR and
        // JAVA_UDF_AGGREGATE in jni_plugin_registry.h. Renaming one here without renaming it there
        // deploys a plugin BE cannot reach.
        Assertions.assertEquals(new HashSet<>(Arrays.asList("scalar", "aggregate")), names);
        Assertions.assertTrue(plugin.getScannerFactories() == null
                        || !plugin.getScannerFactories().iterator().hasNext(),
                "the udf plugin provides no scanners");
    }

    @Test
    public void runsAUserFunctionOutOfItsOwnJar(@TempDir Path tmp) throws Exception {
        Path jar = jarOf(tmp.resolve("fn.jar"), AddOne.class);

        Object executor = new ScalarUdfExecutorFactory()
                .create(scalarParams(AddOne.class.getName(), jar, "add_one(INT)"));

        Assertions.assertTrue(executor instanceof UdfExecutor);
        UdfClassCache cache = ((UdfExecutor) executor).objCache;
        // The signature matched, which means fe-type's Type model, the thrift decoding and
        // reflectasm all resolved inside the plugin.
        Assertions.assertEquals(JavaUdfDataType.INT.getPrimitiveType(),
                cache.retType.getPrimitiveType());
        Assertions.assertEquals(1, cache.argTypes.length);
        // Loaded from the jar we just built, not from the test classpath: if the parent were
        // searched first this would be the test classpath's copy and the test would prove nothing.
        Assertions.assertNotSame(AddOne.class, cache.udfClass);
    }

    @Test
    public void userFunctionSeesTheJdkAndItsOwnJarAndNothingOfDoris(@TempDir Path tmp)
            throws Exception {
        Path jar = jarOf(tmp.resolve("fn.jar"), AddOne.class);
        Object executor = new ScalarUdfExecutorFactory()
                .create(scalarParams(AddOne.class.getName(), jar, "add_one(INT)"));
        ClassLoader loader = ((UdfExecutor) executor).objCache.udfClass.getClassLoader();

        // The JDK, including the platform modules rather than only the bootstrap ones.
        Assertions.assertNotNull(loader.loadClass("java.util.ArrayList"));
        Assertions.assertNotNull(loader.loadClass("java.sql.Date"));
        // The two things a user function may borrow, because they have to be the same class
        // object on both sides of the call.
        Assertions.assertSame(org.joda.time.LocalDate.class,
                loader.loadClass("org.joda.time.LocalDate"));
        Assertions.assertNotNull(loader.loadClass("org.apache.hadoop.hive.ql.exec.UDF"));
        // Everything else this plugin happens to ship stays out of reach - that a user function
        // could reach guava at all was an accident of BE's old shared classpath, and code written
        // against it broke whenever an unrelated dependency moved.
        assertNotVisible(loader, "com.google.common.base.Strings");
        assertNotVisible(loader, "org.apache.thrift.TDeserializer");
        assertNotVisible(loader, "org.apache.doris.catalog.Type");
        assertNotVisible(loader, "org.apache.doris.udf.UdfExecutor");
    }

    @Test
    public void dropFunctionReleasesWhatAStaticallyLoadedFunctionCompiledTo(@TempDir Path tmp)
            throws Exception {
        Path jar = jarOf(tmp.resolve("fn.jar"), AddOne.class);
        // Deliberately a VARIADIC signature, spelled the way FE spells it on the requests that
        // execute a function - with the trailing "..." that DROP FUNCTION does not send. Keyed by
        // the signature, this function could never be invalidated at all.
        byte[] params = staticLoadParams(AddOne.class.getName(), 4242L, "static_add_one(INT...)", jar);

        UdfExecutor first = (UdfExecutor) new ScalarUdfExecutorFactory().create(params);
        UdfExecutor second = (UdfExecutor) new ScalarUdfExecutorFactory().create(params);
        // Static load means the compiled class is shared, not rebuilt per executor.
        Assertions.assertSame(first.objCache.udfClass, second.objCache.udfClass);

        // Either factory drops it: BE broadcasts DROP FUNCTION to every factory of every plugin
        // without knowing which one ran the function. The id is what identifies it, so the
        // signature arriving in its other spelling changes nothing.
        new AggregateUdfExecutorFactory().invalidate(4242L, "static_add_one(INT)");

        UdfExecutor afterDrop = (UdfExecutor) new ScalarUdfExecutorFactory().create(params);
        Assertions.assertNotSame(first.objCache.udfClass, afterDrop.objCache.udfClass);
    }

    /** ... and dropping some other function leaves this one alone, whatever it is called. */
    @Test
    public void droppingAnotherFunctionKeepsThisOneCompiled(@TempDir Path tmp) throws Exception {
        Path jar = jarOf(tmp.resolve("fn.jar"), AddOne.class);
        byte[] params = staticLoadParams(AddOne.class.getName(), 11L, "static_add_one(INT)", jar);

        UdfExecutor first = (UdfExecutor) new ScalarUdfExecutorFactory().create(params);
        // Same name and same argument types, another database: another function, another id. Keyed
        // by the signature, this drop took this function's classes with it.
        new ScalarUdfExecutorFactory().invalidate(12L, "static_add_one(INT)");

        UdfExecutor afterOtherDrop = (UdfExecutor) new ScalarUdfExecutorFactory().create(params);
        Assertions.assertSame(first.objCache.udfClass, afterOtherDrop.objCache.udfClass);
    }

    @Test
    public void functionWithNoJarIsLookedForInThePreInstalledDirectories() {
        // Named here so the two halves cannot drift: start_be.sh no longer puts these on any
        // class path, so this plugin reading them is the only thing that makes a function
        // created without a jar resolve at all.
        Assertions.assertArrayEquals(new String[] {"plugins/java_extensions", "custom_lib"},
                UserFunctionLoaders.preinstalledDirs());
    }

    private static void assertNotVisible(ClassLoader loader, String className) {
        ClassNotFoundException thrown = Assertions.assertThrows(ClassNotFoundException.class,
                () -> loader.loadClass(className), className + " must not be visible to a user function");
        Assertions.assertTrue(thrown.getMessage().contains("Bundle this class into that jar"),
                "the failure should say what to do about it, was: " + thrown.getMessage());
    }

    /** A user function, deliberately depending on nothing. */
    public static class AddOne {
        public Integer evaluate(Integer value) {
            return value == null ? null : value + 1;
        }
    }

    private static byte[] scalarParams(String symbol, Path jar, String signature) throws Exception {
        return serialize(function(symbol, signature, false), jar);
    }

    private static byte[] staticLoadParams(String symbol, long functionId, String signature, Path jar)
            throws Exception {
        TFunction fn = function(symbol, signature, true);
        fn.setId(functionId);
        return serialize(fn, jar);
    }

    private static TFunction function(String symbol, String signature, boolean staticLoad) {
        TFunction fn = new TFunction();
        fn.setName(new TFunctionName("test_fn"));
        fn.setBinaryType(TFunctionBinaryType.JAVA_UDF);
        fn.setArgTypes(Collections.singletonList(scalarType(TPrimitiveType.INT)));
        fn.setRetType(scalarType(TPrimitiveType.INT));
        fn.setHasVarArgs(false);
        fn.setSignature(signature);
        fn.setIsStaticLoad(staticLoad);
        fn.setScalarFn(new TScalarFunction(symbol));
        return fn;
    }

    private static byte[] serialize(TFunction fn, Path jar) throws Exception {
        TJavaUdfExecutorCtorParams params = new TJavaUdfExecutorCtorParams();
        params.setFn(fn);
        params.setLocation(jar.toString());
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(params);
    }

    private static TTypeDesc scalarType(TPrimitiveType type) {
        TTypeNode node = new TTypeNode();
        node.setType(TTypeNodeType.SCALAR);
        node.setScalarType(new TScalarType(type));
        List<TTypeNode> nodes = new ArrayList<>();
        nodes.add(node);
        return new TTypeDesc(nodes);
    }

    private static Path jarOf(Path path, Class<?>... classes) throws IOException {
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(path))) {
            for (Class<?> clazz : classes) {
                String resource = clazz.getName().replace('.', '/') + ".class";
                jar.putNextEntry(new JarEntry(resource));
                copy(resource, jar);
                jar.closeEntry();
            }
        }
        return path;
    }

    private static void copy(String resource, OutputStream out) throws IOException {
        try (InputStream in = JavaUdfPluginTest.class.getClassLoader().getResourceAsStream(resource)) {
            Assertions.assertNotNull(in, resource);
            byte[] buffer = new byte[8192];
            for (int read = in.read(buffer); read > 0; read = in.read(buffer)) {
                out.write(buffer, 0, read);
            }
        }
    }
}
