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
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.meta.MetaPersistMethod;
import org.apache.doris.persist.meta.PersistMetaModules;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
}
