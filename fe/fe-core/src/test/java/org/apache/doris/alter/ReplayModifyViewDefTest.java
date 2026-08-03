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

package org.apache.doris.alter;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.View;
import org.apache.doris.persist.AlterViewInfo;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Unit tests for {@link Alter#replayModifyViewDef}.
 *
 * Covers three replay scenarios:
 * 1. Replay a full view definition change (new SQL + new schema).
 * 2. Replay a comment-only change (inlineViewDef is empty / null → def must not change).
 * 3. Replay with both a new definition and a new comment simultaneously.
 */
public class ReplayModifyViewDefTest extends TestWithFeService {

    // ──────────────────────────────────────────────────────────────────────────
    // Setup
    // ──────────────────────────────────────────────────────────────────────────

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        createDatabase("test_replay_view");

        createTable("create table test_replay_view.tbl1(k1 int, k2 int, v1 int)"
                + " duplicate key(k1) distributed by hash(k1) buckets 1"
                + " properties('replication_num' = '1');");

        createView("create view test_replay_view.v1 as select k1, k2 from test_replay_view.tbl1;");
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────────────

    private static View getView(String viewName) throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test_replay_view");
        return (View) db.getTableOrDdlException(viewName);
    }

    private static long getDbId() throws Exception {
        return Env.getCurrentInternalCatalog().getDbOrDdlException("test_replay_view").getId();
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Tests
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Replay a full definition change.
     * After replay the view's inlineViewDef and full-schema must reflect the new values.
     */
    @Test
    public void testReplayModifyViewDefWithNewDef() throws Exception {
        View view = getView("v1");
        long dbId = getDbId();
        long tableId = view.getId();

        String newDef = "select k1, v1 from test_replay_view.tbl1 where k1 > 0";
        List<Column> newSchema = Lists.newArrayList(
                new Column("k1", PrimitiveType.INT),
                new Column("v1", PrimitiveType.INT));

        AlterViewInfo info = new AlterViewInfo(dbId, tableId, newDef, newSchema,
                new HashMap<>(), null);

        Env.getCurrentEnv().getAlterInstance().replayModifyViewDef(info);

        View updated = getView("v1");
        Assertions.assertEquals(newDef, updated.getInlineViewDef());
        Assertions.assertEquals(2, updated.getFullSchema().size());
        Assertions.assertNotNull(updated.getColumn("k1"));
        Assertions.assertNotNull(updated.getColumn("v1"));
        Assertions.assertNull(updated.getColumn("k2"));
    }

    /**
     * Replay a comment-only change (inlineViewDef is null).
     * The view definition must remain unchanged; only the comment is updated.
     */
    @Test
    public void testReplayModifyViewDefCommentOnly() throws Exception {
        // First bring v1 back to a known state with two columns.
        View view = getView("v1");
        long dbId = getDbId();
        long tableId = view.getId();
        String originalDef = view.getInlineViewDef();

        AlterViewInfo info = new AlterViewInfo(dbId, tableId, null,
                Collections.emptyList(), new HashMap<>(), "my comment");

        Env.getCurrentEnv().getAlterInstance().replayModifyViewDef(info);

        View updated = getView("v1");
        // Definition must not change.
        Assertions.assertEquals(originalDef, updated.getInlineViewDef());
        // Comment must be set.
        Assertions.assertEquals("my comment", updated.getComment());
    }

    /**
     * Replay with both a new definition and a comment.
     * Both the def/schema and the comment must be updated.
     */
    @Test
    public void testReplayModifyViewDefWithDefAndComment() throws Exception {
        View view = getView("v1");
        long dbId = getDbId();
        long tableId = view.getId();

        String newDef = "select k2 from test_replay_view.tbl1";
        List<Column> newSchema = Lists.newArrayList(new Column("k2", PrimitiveType.INT));

        AlterViewInfo info = new AlterViewInfo(dbId, tableId, newDef, newSchema,
                new HashMap<>(), "updated comment");

        Env.getCurrentEnv().getAlterInstance().replayModifyViewDef(info);

        View updated = getView("v1");
        Assertions.assertEquals(newDef, updated.getInlineViewDef());
        Assertions.assertEquals(1, updated.getFullSchema().size());
        Assertions.assertNotNull(updated.getColumn("k2"));
        Assertions.assertEquals("updated comment", updated.getComment());
    }
}
