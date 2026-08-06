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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.commands.AddConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.DropConstraintCommand;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

class ConstraintTest extends TestWithFeService implements PlanPatternMatchSupported {

    private ConstraintManager getConstraintMgr() {
        return Env.getCurrentEnv().getConstraintManager();
    }

    private TableNameInfo tableNameInfoOf(TableIf table) {
        String tblName = table.getName();
        if (GlobalVariable.isStoredTableNamesLowerCase()) {
            tblName = tblName.toLowerCase();
        }
        return new TableNameInfo(
                table.getDatabase().getCatalog().getName(), table.getDatabase().getFullName(), tblName);
    }

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("create table t1 (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t2 (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t3 (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
    }

    @Test
    void primaryKeyConstraintTest() throws Exception {
        AddConstraintCommand addCommand = (AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint pk primary key (k1)");
        addCommand.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(logicalOlapScan().when(o -> {
            TableNameInfo tni = tableNameInfoOf(o.getTable());
            Constraint c = getConstraintMgr().getConstraint(tni, "pk");
            if (c instanceof PrimaryKeyConstraint) {
                Set<String> columns = ((PrimaryKeyConstraint) c).getPrimaryKeyNames();
                return columns.size() == 1 && columns.iterator().next().equals("k1");
            }
            return false;
        }));

        DropConstraintCommand dropCommand = (DropConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 drop constraint pk");
        dropCommand.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
    }

    @Test
    void uniqueConstraintTest() throws Exception {
        AddConstraintCommand command = (AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint un unique (k1)");
        command.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(logicalOlapScan().when(o -> {
            TableNameInfo tni = tableNameInfoOf(o.getTable());
            Constraint c = getConstraintMgr().getConstraint(tni, "un");
            if (c instanceof UniqueConstraint) {
                Set<String> columns = ((UniqueConstraint) c).getUniqueColumnNames();
                return columns.size() == 1 && columns.iterator().next().equals("k1");
            }
            return false;
        }));

        DropConstraintCommand dropCommand = (DropConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 drop constraint un");
        dropCommand.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
    }

    @Test
    void foreignKeyConstraintTest() throws Exception {
        AddConstraintCommand command = (AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint fk foreign key (k1) references t2 (k1)");
        try {
            command.run(connectContext, null);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains(
                    "Foreign key constraint requires a primary key constraint [k1] in"));
        }
        ((AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t2 add constraint pk primary key (k1, k2)")).run(connectContext, null);
        ((AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint fk foreign key (k1, k2) references t2(k1, k2)")).run(connectContext,
                null);

        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(logicalOlapScan().when(o -> {
            TableNameInfo tni = tableNameInfoOf(o.getTable());
            Constraint c = getConstraintMgr().getConstraint(tni, "fk");
            if (c instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint f = (ForeignKeyConstraint) c;
                Column ref1 = f.getReferencedColumn(((SlotReference) o.getOutput().get(0))
                        .getOriginalColumn().get().getName());
                Column ref2 = f.getReferencedColumn(((SlotReference) o.getOutput().get(1))
                        .getOriginalColumn().get().getName());
                return ref1.getName().equals("k1") && ref2.getName().equals("k2");
            }
            return false;
        }));

        PlanChecker.from(connectContext).parse("select * from t2").analyze().matches(logicalOlapScan().when(o -> {
            TableNameInfo tni = tableNameInfoOf(o.getTable());
            Constraint c = getConstraintMgr().getConstraint(tni, "pk");
            if (c instanceof PrimaryKeyConstraint) {
                Set<String> columnNames = ((PrimaryKeyConstraint) c).getPrimaryKeyNames();
                java.util.List<TableNameInfo> foreignTableInfos
                        = ((PrimaryKeyConstraint) c).getForeignTableInfos();
                return columnNames.size() == 2
                        && columnNames.equals(Sets.newHashSet("k1", "k2"))
                        && foreignTableInfos.size() == 1;
            }
            return false;
        }));

        // drop fk
        DropConstraintCommand dropCommand = (DropConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 drop constraint fk");
        dropCommand.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
        // drop pk and fk referenced it also should be dropped
        ((AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint fk foreign key (k1, k2) references t2(k1, k2)")).run(connectContext,
                null);
        ((DropConstraintCommand) new NereidsParser().parseSingle("alter table t2 drop constraint pk"))
                .run(connectContext, null);

        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
        PlanChecker.from(connectContext).parse("select * from t2").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
    }

    @Test
    void cascadeDropTest() throws Exception {
        addConstraint("alter table t1 add constraint pk primary key (k1)");
        addConstraint("alter table t2 add constraint fk foreign key (k1) references t1(k1)");
        dropConstraint("alter table t1 drop constraint pk");

        PlanChecker.from(connectContext).parse("select * from t2").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));

        addConstraint("alter table t1 add constraint pk primary key (k1)");
        addConstraint("alter table t1 add constraint fk foreign key (k1) references t1(k1)");
        addConstraint("alter table t2 add constraint fk foreign key (k1) references t1(k1)");
        addConstraint("alter table t3 add constraint fk foreign key (k1) references t1(k1)");
        dropConstraint("alter table t1 drop constraint pk");
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
        PlanChecker.from(connectContext).parse("select * from t2").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
        PlanChecker.from(connectContext).parse("select * from t3").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
    }

    @Test
    void dropTableBlockedByForeignKeyTest() throws Exception {
        // Setup: PK on t1, FK on t2 referencing t1
        addConstraint("alter table t1 add constraint pk_dt primary key (k1)");
        addConstraint("alter table t2 add constraint fk_dt foreign key (k1) references t1(k1)");

        // Drop t1 should fail because t2's FK references t1's PK
        Assertions.assertThrows(Exception.class, () -> {
            executeSql("drop table t1");
        });

        // Verify t1 still exists and constraints are intact
        TableNameInfo t1Info = new TableNameInfo("internal", "test", "t1");
        Assertions.assertNotNull(getConstraintMgr().getConstraint(t1Info, "pk_dt"));

        // Cleanup: drop FK first, then PK
        dropConstraint("alter table t2 drop constraint fk_dt");
        dropConstraint("alter table t1 drop constraint pk_dt");
    }

    @Test
    void forceDropTableCascadesForeignKeyTest() throws Exception {
        // Create new tables for this test to avoid affecting other tests
        createTable("create table t_pk (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t_fk (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");

        addConstraint("alter table t_pk add constraint pk_force primary key (k1)");
        addConstraint("alter table t_fk add constraint fk_force foreign key (k1) references t_pk(k1)");

        // Force drop t_pk should succeed and cascade-drop FK on t_fk
        executeSql("drop table t_pk force");

        // Verify FK on t_fk was cascade-dropped
        TableNameInfo tFkInfo = new TableNameInfo("internal", "test", "t_fk");
        Assertions.assertTrue(getConstraintMgr().getConstraints(tFkInfo).isEmpty());

        // Cleanup
        executeSql("drop table t_fk force");
    }

    @Test
    void dropColumnBlockedByConstraintTest() throws Exception {
        // Create a table with non-key columns
        createTable("create table t_schema (\n"
                + "    k1 int,\n"
                + "    v1 int,\n"
                + "    v2 int\n"
                + ")\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");

        // Add UNIQUE constraint on v1
        addConstraint("alter table t_schema add constraint un_v1 unique (v1)");

        // Try to drop column v1 -> should fail because of constraint
        Assertions.assertThrows(Exception.class, () -> {
            executeSql("alter table t_schema drop column v1");
        });

        // Drop constraint first, then drop column should not throw during validation
        dropConstraint("alter table t_schema drop constraint un_v1");

        // Cleanup
        executeSql("drop table t_schema force");
    }

    @Test
    void replaceTableWithConstraintsTest() throws Exception {
        // Create tables for replace test
        createTable("create table t_orig (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t_repl (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t_ref (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");

        // Add PK on t_orig, FK on t_ref referencing t_orig
        addConstraint("alter table t_orig add constraint pk_orig primary key (k1)");
        addConstraint("alter table t_ref add constraint fk_ref foreign key (k1) references t_orig(k1)");

        // Replace t_orig with t_repl (no swap, t_orig gets dropped) -> should fail
        // because t_orig's PK is referenced by t_ref's FK
        Assertions.assertThrows(Exception.class, () -> {
            executeSql("alter table t_orig replace with table t_repl "
                    + "properties(\"swap\"=\"false\")");
        });

        // Drop FK first
        dropConstraint("alter table t_ref drop constraint fk_ref");

        // Now replace should succeed
        executeSql("alter table t_orig replace with table t_repl "
                + "properties(\"swap\"=\"false\")");

        // After replace: t_repl is renamed to t_orig, old t_orig is dropped
        TableNameInfo tOrigInfo = new TableNameInfo("internal", "test", "t_orig");
        Assertions.assertTrue(getConstraintMgr().getConstraints(tOrigInfo).isEmpty());

        // Cleanup
        executeSql("drop table if exists t_orig force");
        executeSql("drop table if exists t_repl force");
        executeSql("drop table if exists t_ref force");
    }

    @Test
    void dropConstraintOnNonExistentTableTest() throws Exception {
        // Simulate an external table scenario: a constraint exists in the manager
        // but the table has been deleted by another system.
        ConstraintManager mgr = getConstraintMgr();
        TableNameInfo ghostTable = new TableNameInfo("internal", "test", "ghost_table");
        PrimaryKeyConstraint pk = new PrimaryKeyConstraint("ghost_pk", Sets.newHashSet("col1"));
        // Add via replay path to bypass table validation
        mgr.addConstraint(ghostTable, "ghost_pk", pk, true);
        Assertions.assertNotNull(mgr.getConstraint(ghostTable, "ghost_pk"));

        // Drop constraint via SQL — the table does not exist, but the command should still succeed
        DropConstraintCommand dropCmd = (DropConstraintCommand) new NereidsParser().parseSingle(
                "alter table test.ghost_table drop constraint ghost_pk");
        dropCmd.run(connectContext, null);

        // Constraint should be removed
        Assertions.assertNull(mgr.getConstraint(ghostTable, "ghost_pk"));
    }

    @Test
    void renameTableUpdatesConstraintsTest() throws Exception {
        // Create dedicated tables for rename test
        createTable("create table t_rename_src (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");

        addConstraint("alter table t_rename_src add constraint pk_rename primary key (k1)");
        TableNameInfo oldInfo = new TableNameInfo("internal", "test", "t_rename_src");
        Assertions.assertNotNull(getConstraintMgr().getConstraint(oldInfo, "pk_rename"));

        // Rename the table
        executeSql("alter table t_rename_src rename t_rename_dst");

        // Constraint should be accessible under the new name
        TableNameInfo newInfo = new TableNameInfo("internal", "test", "t_rename_dst");
        Assertions.assertNotNull(getConstraintMgr().getConstraint(newInfo, "pk_rename"));

        // Old name should no longer have constraints
        Assertions.assertTrue(getConstraintMgr().getConstraints(oldInfo).isEmpty());

        // Cleanup
        dropConstraint("alter table t_rename_dst drop constraint pk_rename");
        executeSql("drop table t_rename_dst force");
    }
}
