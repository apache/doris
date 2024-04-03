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

public class ExternalMetaIdMgrTest {

    @Test
    public void testReplayMetaIdMappingsLog() {
        ExternalMetaIdMgr mgr = new ExternalMetaIdMgr();
        MetaIdMappingsLog log1 = new MetaIdMappingsLog();
        log1.setCatalogId(1L);
        log1.setFromHmsEvent(false);
        log1.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_ADD,
                    MetaIdMappingsLog.META_OBJECT_TYPE_DATABASE,
                    "db1", ExternalMetaIdMgr.nextMetaId()));
        mgr.replayMetaIdMappingsLog(log1);
        Assertions.assertNotEquals(-1L, mgr.getDbId(1L, "db1"));

        MetaIdMappingsLog log2 = new MetaIdMappingsLog();
        log2.setCatalogId(1L);
        log2.setFromHmsEvent(false);
        log2.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                    MetaIdMappingsLog.META_OBJECT_TYPE_DATABASE,
                    "db1"));
        mgr.replayMetaIdMappingsLog(log2);
        Assertions.assertEquals(-1L, mgr.getDbId(1L, "db1"));

        MetaIdMappingsLog log3 = new MetaIdMappingsLog();
        log3.setCatalogId(1L);
        log3.setFromHmsEvent(false);
        log3.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_ADD,
                    MetaIdMappingsLog.META_OBJECT_TYPE_TABLE,
                    "db1", "tbl1", ExternalMetaIdMgr.nextMetaId()));
        mgr.replayMetaIdMappingsLog(log3);
        Assertions.assertEquals(-1L, mgr.getDbId(1L, "db1"));
        Assertions.assertNotEquals(-1L, mgr.getTblId(1L, "db1", "tbl1"));

        MetaIdMappingsLog log4 = new MetaIdMappingsLog();
        log4.setCatalogId(1L);
        log4.setFromHmsEvent(false);
        log4.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                    MetaIdMappingsLog.META_OBJECT_TYPE_TABLE,
                    "db1", "tbl1"));
        log4.addMetaIdMapping(new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_ADD,
                    MetaIdMappingsLog.META_OBJECT_TYPE_PARTITION,
                    "db1", "tbl1", "p1", ExternalMetaIdMgr.nextMetaId()));
        mgr.replayMetaIdMappingsLog(log4);
        Assertions.assertEquals(-1L, mgr.getDbId(1L, "db1"));
        Assertions.assertEquals(-1L, mgr.getTblId(1L, "db1", "tbl1"));
        Assertions.assertNotEquals(-1L, mgr.getPartitionId(1L, "db1", "tbl1", "p1"));
    }

}
