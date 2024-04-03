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

package org.apache.doris.datasource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MetaIdMappingsLogTest {

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        MetaIdMappingsLog log1 = new MetaIdMappingsLog();
        Path path = Files.createFile(Paths.get("./metaIdMappingsLogTest.txt"));
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path))) {
            log1.setFromHmsEvent(true);
            log1.setLastSyncedEventId(-1L);
            log1.setCatalogId(1L);
            log1.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                        MetaIdMappingsLog.OPERATION_TYPE_ADD,
                        MetaIdMappingsLog.META_OBJECT_TYPE_DATABASE,
                        "db1", ExternalMetaIdMgr.nextMetaId()));
            log1.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                        MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                        MetaIdMappingsLog.META_OBJECT_TYPE_TABLE,
                        "db1", "tbl1", ExternalMetaIdMgr.nextMetaId()));
            log1.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                        MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                        MetaIdMappingsLog.META_OBJECT_TYPE_TABLE,
                        "db1", "tbl2", ExternalMetaIdMgr.nextMetaId()));
            log1.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                        MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                        MetaIdMappingsLog.META_OBJECT_TYPE_DATABASE,
                        "db2"));
            log1.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                        MetaIdMappingsLog.OPERATION_TYPE_ADD,
                        MetaIdMappingsLog.META_OBJECT_TYPE_PARTITION,
                        "db1", "tbl1", "p1", ExternalMetaIdMgr.nextMetaId()));
            log1.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                        MetaIdMappingsLog.OPERATION_TYPE_ADD,
                        MetaIdMappingsLog.META_OBJECT_TYPE_PARTITION,
                        "db1", "tbl1", "p2", ExternalMetaIdMgr.nextMetaId()));
            log1.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                        MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                        MetaIdMappingsLog.META_OBJECT_TYPE_DATABASE,
                        "db2"));
            log1.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                        MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                        MetaIdMappingsLog.META_OBJECT_TYPE_TABLE,
                        "db1", "tbl1", ExternalMetaIdMgr.nextMetaId()));
            log1.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                        MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                        MetaIdMappingsLog.META_OBJECT_TYPE_PARTITION,
                        "db1", "tbl1", "p2", ExternalMetaIdMgr.nextMetaId()));
            log1.write(dos);
            dos.flush();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            Files.deleteIfExists(path);
            Assertions.fail();
        }

        // 2. Read objects from file
        MetaIdMappingsLog log2;
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(path))) {
            log2 = MetaIdMappingsLog.read(dis);
            Assertions.assertEquals(log1, log2);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            Assertions.fail();
        } finally {
            Files.deleteIfExists(path);
        }
    }

}
