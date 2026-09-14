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

package org.apache.doris.datasource.lance.job;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.meta.MetaPersistMethod;
import org.apache.doris.persist.meta.PersistMetaModules;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LanceIndexJobWiringTest {
    private static final short LANCE_INDEX_JOB_OPCODE = 500;
    private static final String LANCE_INDEX_JOB_MODULE = "lanceIndexJobManager";

    @Test
    public void lanceIndexJobOpcodeIsUniquelyAssigned() throws IllegalAccessException {
        List<String> fieldsUsingOpcode = new ArrayList<>();
        for (Field field : OperationType.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())
                    && field.getType() == short.class
                    && field.getShort(null) == LANCE_INDEX_JOB_OPCODE) {
                fieldsUsingOpcode.add(field.getName());
            }
        }

        Assertions.assertEquals(
                Collections.singletonList("OP_LANCE_INDEX_JOB_UPSERT"),
                fieldsUsingOpcode,
                "operation code 500 must remain uniquely assigned to Lance index job upserts");
        Assertions.assertEquals(LANCE_INDEX_JOB_OPCODE, OperationType.OP_LANCE_INDEX_JOB_UPSERT);
    }

    @Test
    public void lanceIndexJobManagerIsTheLastBaseImageModuleWithEnvBindings() throws Exception {
        Assertions.assertEquals(
                LANCE_INDEX_JOB_MODULE,
                PersistMetaModules.MODULE_NAMES.get(PersistMetaModules.MODULE_NAMES.size() - 1),
                "new image modules must be appended without reordering existing base modules");

        MetaPersistMethod persistMethod = PersistMetaModules.MODULES_MAP.get(LANCE_INDEX_JOB_MODULE);
        Assertions.assertNotNull(persistMethod);

        Method expectedReadMethod = Env.class.getDeclaredMethod(
                "loadLanceIndexJobManager", DataInputStream.class, long.class);
        Method expectedWriteMethod = Env.class.getDeclaredMethod(
                "saveLanceIndexJobManager", CountingDataOutputStream.class, long.class);
        Assertions.assertEquals(expectedReadMethod, persistMethod.readMethod);
        Assertions.assertEquals(expectedWriteMethod, persistMethod.writeMethod);
        Assertions.assertEquals(long.class, persistMethod.readMethod.getReturnType());
        Assertions.assertEquals(long.class, persistMethod.writeMethod.getReturnType());
    }

    /**
     * The dispatcher is wired as a master-only daemon: an instance field on {@link Env}
     * (assigned in the Env constructor from the job manager, so both share one durable
     * image), of a {@link MasterDaemon} subclass constructible from exactly the manager.
     */
    @Test
    public void lanceIndexJobDispatcherIsAWiredMasterDaemon() throws Exception {
        Field field = Env.class.getDeclaredField("lanceIndexJobDispatcher");
        Assertions.assertFalse(Modifier.isStatic(field.getModifiers()), "one dispatcher per Env instance");
        Assertions.assertEquals(LanceIndexJobDispatcher.class, field.getType());
        Assertions.assertTrue(MasterDaemon.class.isAssignableFrom(field.getType()),
                "the dispatcher must start through the master-only MasterDaemon machinery");
        Assertions.assertNotNull(
                LanceIndexJobDispatcher.class.getDeclaredConstructor(LanceIndexJobManager.class),
                "the dispatcher is constructed from the Env-owned job manager");
    }

    /**
     * Source-order wiring of the dispatch lifecycle in Env.java: the constructor creates
     * the dispatcher on the manager, only {@code startMasterOnlyDaemonThreads} starts it
     * (never the non-master path), and the master-transfer sweep of the job manager runs
     * before that start, so no dispatcher round can ever observe a durable RUNNING left by
     * the old master. Reflection cannot see call sites, so this reads the source; it is
     * skipped when sources are not next to the test run (jar-only environment).
     */
    @Test
    public void dispatcherStartsInStartMasterOnlyDaemonThreadsAfterTheTransferSweep() throws Exception {
        String source = readEnvSource();

        Assertions.assertTrue(source.contains(
                "this.lanceIndexJobDispatcher = new LanceIndexJobDispatcher(lanceIndexJobManager);"),
                "the Env constructor must create the dispatcher on the Env-owned job manager");

        String masterOnlyBody = methodBody(source, "protected void startMasterOnlyDaemonThreads() {");
        Assertions.assertTrue(masterOnlyBody.contains("lanceIndexJobDispatcher.start();"),
                "the dispatcher must be started in startMasterOnlyDaemonThreads");
        Assertions.assertFalse(
                methodBody(source, "protected void startNonMasterDaemonThreads() {").contains(
                        "lanceIndexJobDispatcher"),
                "the dispatcher must never start on a non-master FE");

        int sweep = source.indexOf("lanceIndexJobManager.onTransferToMaster();");
        int daemonStart = source.indexOf("startMasterOnlyDaemonThreads();");
        Assertions.assertTrue(sweep >= 0, "the master-transfer sweep call was not found");
        Assertions.assertTrue(daemonStart > sweep,
                "the RUNNING-to-UNKNOWN sweep must run before any master-only daemon starts");
    }

    private static String readEnvSource() throws Exception {
        // Surefire runs with the module directory as the working directory; also try the
        // checkout root so an IDE run from fe/ resolves the same file.
        for (Path candidate : new Path[]{
                Paths.get("src/main/java/org/apache/doris/catalog/Env.java"),
                Paths.get("fe-core/src/main/java/org/apache/doris/catalog/Env.java")}) {
            if (Files.exists(candidate)) {
                return new String(Files.readAllBytes(candidate), StandardCharsets.UTF_8);
            }
        }
        Assumptions.assumeTrue(false, "Env.java source is not available next to the test run; skipping");
        throw new IllegalStateException("unreachable");
    }

    /** Extracts one method's source from its definition down to its closing brace. */
    private static String methodBody(String source, String definition) {
        int signature = source.indexOf(definition);
        Assertions.assertTrue(signature >= 0, "method definition not found in Env.java: " + definition);
        int end = source.indexOf("\n    }", signature);
        Assertions.assertTrue(end > signature, "no closing brace found for method " + definition);
        return source.substring(signature, end);
    }
}
