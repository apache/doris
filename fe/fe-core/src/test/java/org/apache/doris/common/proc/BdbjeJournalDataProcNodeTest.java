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

package org.apache.doris.common.proc;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.journal.bdbje.BDBDebugger;
import org.apache.doris.persist.OperationType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * SHOW PROC '/bdbje/dbName/journalId', the BDBJE debug-mode view of a single journal record.
 */
public class BdbjeJournalDataProcNodeTest {
    private static final String DB_NAME = "1";

    private boolean debugMode;
    private BDBDebugger.BDBDebugEnv debugEnv;

    @BeforeEach
    public void setUp() {
        debugMode = Config.enable_bdbje_debug_mode;
        debugEnv = BDBDebugger.get().getEnv();
    }

    @AfterEach
    public void tearDown() {
        Config.enable_bdbje_debug_mode = debugMode;
        Deencapsulation.setField(BDBDebugger.get(), "debugEnv", debugEnv);
    }

    @Test
    public void recordsOfRetiredOperationTypesArePrintable() throws Exception {
        // The readers of retired operation types consume the payload without keeping it. Op 500 is the Lance
        // index job of unreleased branch-4.x builds, written as Text-framed JSON; the PL/SQL operation types were
        // retired the same way.
        BDBDebugger.JournalEntityWrapper lanceIndexJob = record(7L, OperationType.OP_LANCE_INDEX_JOB_UPSERT,
                "{\"jid\":7,\"rev\":2,\"cid\":10101,\"dbn\":\"db1\",\"tbn\":\"tbl1\",\"iid\":\"idx_v\","
                        + "\"it\":\"IVF_PQ\",\"cn\":[\"v\"],\"rs\":\"RUNNING\"}");
        BDBDebugger.JournalEntityWrapper plsqlPackage = record(8L, OperationType.OP_DROP_PLSQL_PACKAGE,
                "{\"name\":\"pkg1\",\"catalogId\":0,\"dbName\":\"db1\"}");
        Assertions.assertNull(lanceIndexJob.entity.getData());
        Assertions.assertNull(plsqlPackage.entity.getData());
        installDebugEnv(lanceIndexJob, plsqlPackage);

        // MUTATION: with getData().toString() both rows fail with a NullPointerException.
        Assertions.assertEquals(Lists.newArrayList("7", "OP_LANCE_INDEX_JOB_UPSERT",
                lanceIndexJob.size.toString(), "null"), fetchRow(7L));
        Assertions.assertEquals(Lists.newArrayList("8", "OP_DROP_PLSQL_PACKAGE",
                plsqlPackage.size.toString(), "null"), fetchRow(8L));
    }

    @Test
    public void liveRecordStillShowsItsPayload() throws Exception {
        BDBDebugger.JournalEntityWrapper nextId = record(9L, OperationType.OP_SAVE_NEXTID, "12345");
        installDebugEnv(nextId);

        Assertions.assertEquals(Lists.newArrayList("9", "OP_SAVE_NEXTID", nextId.size.toString(), "12345"),
                fetchRow(9L));
    }

    /** A journal record as BDBDebugEnv#getJournalEntity reads it back from BDBJE. */
    private static BDBDebugger.JournalEntityWrapper record(long journalId, short opCode, String payload)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeShort(opCode);
            // The int-length framing that both Text#write and Text.writeString produce.
            Text.writeString(out, payload);
        }
        BDBDebugger.JournalEntityWrapper wrapper = new BDBDebugger.JournalEntityWrapper(journalId);
        wrapper.size = bytes.size();
        JournalEntity entity = new JournalEntity();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            entity.readFields(in);
            Assertions.assertEquals(-1, in.read(), "the record must be consumed exactly");
        }
        wrapper.entity = entity;
        return wrapper;
    }

    private static void installDebugEnv(BDBDebugger.JournalEntityWrapper... records) {
        BDBDebugger.BDBDebugEnv env = Mockito.mock(BDBDebugger.BDBDebugEnv.class);
        for (BDBDebugger.JournalEntityWrapper record : records) {
            Mockito.when(env.getJournalEntity(DB_NAME, record.journalId)).thenReturn(record);
        }
        Deencapsulation.setField(BDBDebugger.get(), "debugEnv", env);
        Config.enable_bdbje_debug_mode = true;
    }

    private static List<String> fetchRow(long journalId) throws AnalysisException {
        ProcResult result = new BdbjeJournalDataProcNode(DB_NAME, journalId).fetchResult();
        Assertions.assertEquals(BdbjeJournalDataProcNode.TITLE_NAMES, result.getColumnNames());
        Assertions.assertEquals(1, result.getRows().size());
        return result.getRows().get(0);
    }
}
