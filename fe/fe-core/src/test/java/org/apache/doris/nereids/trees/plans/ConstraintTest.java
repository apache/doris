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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.constraint.DistributionMappingConstraint;
import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.commands.AddConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.DropConstraintCommand;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.persist.TableRenameColumnInfo;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
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
    void distributionMappingConstraintTest() throws Exception {
        createTable("create table mapping_basic (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\"replication_num\"=\"1\")");
        try {
            Exception duplicateDeterminant = Assertions.assertThrows(Exception.class, () -> addConstraint(
                    "alter table mapping_basic add constraint duplicate_determinant "
                            + "colocate mapping mapping_id (k2, K2) "
                            + "determines distribution key (k1) not enforced"));
            Assertions.assertTrue(duplicateDeterminant.getMessage().contains(
                    "Determinant columns in distribution mapping constraint must be unique"));

            Exception invalidDistribution = Assertions.assertThrows(Exception.class, () -> addConstraint(
                    "alter table mapping_basic add constraint invalid_distribution "
                            + "colocate mapping mapping_id (k2) "
                            + "determines distribution key (k2) not enforced"));
            Assertions.assertTrue(invalidDistribution.getMessage().contains(
                    "must be an ordered subset of table distribution columns"));

            addConstraint("alter table mapping_basic add constraint mapping_constraint "
                    + "colocate mapping mapping_id (k2) determines distribution key (k1) not enforced");
            OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_basic");
            TableNameInfo tableNameInfo = tableNameInfoOf(table);

            Assertions.assertNull(getConstraintMgr().getConstraint(tableNameInfo, "mapping_constraint"));
            Constraint constraint = getConstraintMgr().getConstraint(
                    tableNameInfo, table, "mapping_constraint");
            Assertions.assertInstanceOf(DistributionMappingConstraint.class, constraint);
            DistributionMappingConstraint mapping = (DistributionMappingConstraint) constraint;
            Assertions.assertEquals("mapping_id", mapping.getMappingId());
            Assertions.assertEquals(java.util.List.of("k2"), mapping.getDeterminantColumnNames());
            Assertions.assertEquals(java.util.List.of("k1"), mapping.getDistributionColumnNames());
            Assertions.assertEquals(table.getBaseSchemaVersion(), mapping.getBaseSchemaVersion());
            Assertions.assertEquals(java.util.List.of(table.getColumn("k2").getUniqueId()),
                    mapping.getDeterminantColumnUniqueIds());
            Assertions.assertEquals(java.util.List.of(table.getColumn("k1").getUniqueId()),
                    mapping.getDistributionColumnUniqueIds());

            Exception dropColumn = Assertions.assertThrows(Exception.class,
                    () -> executeSql("alter table mapping_basic drop column k2"));
            Assertions.assertTrue(dropColumn.getMessage().contains("mapping_constraint"));
            Exception renameColumn = Assertions.assertThrows(Exception.class,
                    () -> executeSql("alter table mapping_basic rename column k2 k3"));
            Assertions.assertTrue(renameColumn.getMessage().contains("mapping_constraint"));
            Exception modifyColumn = Assertions.assertThrows(Exception.class,
                    () -> executeSql("alter table mapping_basic modify column k2 bigint"));
            Assertions.assertTrue(modifyColumn.getMessage().contains("mapping_constraint"));

            dropConstraint("alter table mapping_basic drop constraint mapping_constraint");
            Assertions.assertTrue(getConstraintMgr().getDistributionMappingConstraints(table).isEmpty());
        } finally {
            executeSql("drop table if exists mapping_basic force");
        }
    }

    @Test
    void distributionMappingFailsClosedAfterOldFrontendSchemaReplay() throws Exception {
        createTable("create table mapping_schema_binding (k1 int, k2 int) "
                + "duplicate key(k1) distributed by hash(k1) buckets 4 "
                + "properties(\"replication_num\"=\"1\", \"light_schema_change\"=\"true\")");
        try {
            addConstraint("alter table mapping_schema_binding add constraint mapping "
                    + "colocate mapping mapping_id (k2) determines distribution key (k1) not enforced");
            OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_schema_binding");

            executeSql("alter table mapping_schema_binding add column k3 int");
            Assertions.assertEquals(1,
                    getConstraintMgr().getDistributionMappingConstraintsForPlanning(table).size());

            Map<Long, Integer> schemaVersions = new HashMap<>();
            table.getIndexIdToMeta().forEach((indexId, indexMeta) ->
                    schemaVersions.put(indexId, indexMeta.getSchemaVersion() + 1));
            Env.getCurrentEnv().replayRenameColumn(new TableRenameColumnInfo(
                    table.getDatabase().getId(), table.getId(), "k2", "renamed_k2", schemaVersions));

            Assertions.assertNotNull(table.getColumn("renamed_k2"));
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> getConstraintMgr().getDistributionMappingConstraintsForPlanning(table));
            Assertions.assertTrue(exception.getMessage().contains("Drop and recreate the constraint"));
        } finally {
            executeSql("drop table if exists mapping_schema_binding force");
        }
    }

    @Test
    void distributionMappingRejectsTemporaryTable() throws Exception {
        createTable("create temporary table mapping_temporary (k1 int, k2 int) "
                + "duplicate key(k1) distributed by hash(k1) buckets 4 "
                + "properties(\"replication_num\"=\"1\")");
        try {
            Exception exception = Assertions.assertThrows(Exception.class, () -> addConstraint(
                    "alter table mapping_temporary add constraint mapping_constraint "
                            + "colocate mapping mapping_id (k2) "
                            + "determines distribution key (k1) not enforced"));
            Assertions.assertTrue(exception.getMessage().contains(
                    "Distribution mapping constraint does not support temporary tables"));
        } finally {
            executeSql("drop table if exists mapping_temporary force");
        }
    }

    @Test
    void distributionMappingFollowsTableObjectLifecycle() throws Exception {
        createTable("create table mapping_lifecycle_a (k1 int, k2 int) "
                + "duplicate key(k1) distributed by hash(k1) buckets 4 "
                + "properties(\"replication_num\"=\"1\")");
        createTable("create table mapping_lifecycle_b (k1 int, k2 int) "
                + "duplicate key(k1) distributed by hash(k1) buckets 4 "
                + "properties(\"replication_num\"=\"1\")");
        createTable("create table mapping_replace_a (k1 int, k2 int) "
                + "duplicate key(k1) distributed by hash(k1) buckets 4 "
                + "properties(\"replication_num\"=\"1\")");
        createTable("create table mapping_replace_b (k1 int, k2 int) "
                + "duplicate key(k1) distributed by hash(k1) buckets 4 "
                + "properties(\"replication_num\"=\"1\")");
        try {
            addConstraint("alter table mapping_lifecycle_a add constraint mapping_a "
                    + "colocate mapping mapping_a (k2) determines distribution key (k1) not enforced");
            addConstraint("alter table mapping_lifecycle_b add constraint mapping_b "
                    + "colocate mapping mapping_b (k2) determines distribution key (k1) not enforced");
            addConstraint("alter table mapping_replace_a add constraint replace_mapping_a "
                    + "colocate mapping replace_mapping_a (k2) determines distribution key (k1) not enforced");
            addConstraint("alter table mapping_replace_b add constraint replace_mapping_b "
                    + "colocate mapping replace_mapping_b (k2) determines distribution key (k1) not enforced");

            OlapTable originalA = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_lifecycle_a");
            OlapTable originalB = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_lifecycle_b");
            executeSql("alter table mapping_lifecycle_a rename mapping_lifecycle_a_renamed");
            OlapTable renamedA = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_lifecycle_a_renamed");
            Assertions.assertSame(originalA, renamedA);
            Assertions.assertEquals("mapping_a",
                    getConstraintMgr().getDistributionMappingConstraints(renamedA).get(0).getName());

            executeSql("alter table mapping_lifecycle_a_renamed replace with table mapping_lifecycle_b "
                    + "properties(\"swap\"=\"true\")");
            OlapTable currentA = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_lifecycle_a_renamed");
            OlapTable currentB = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_lifecycle_b");
            Assertions.assertSame(originalB, currentA);
            Assertions.assertSame(originalA, currentB);
            Assertions.assertEquals("mapping_b",
                    getConstraintMgr().getDistributionMappingConstraints(currentA).get(0).getName());
            Assertions.assertEquals("mapping_a",
                    getConstraintMgr().getDistributionMappingConstraints(currentB).get(0).getName());

            executeSql("truncate table mapping_lifecycle_b");
            OlapTable truncatedB = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_lifecycle_b");
            Assertions.assertSame(originalA, truncatedB);
            Assertions.assertEquals("mapping_a",
                    getConstraintMgr().getDistributionMappingConstraints(truncatedB).get(0).getName());

            executeSql("drop table mapping_lifecycle_b");
            createTable("create table mapping_lifecycle_b (k1 int, k2 int) "
                    + "duplicate key(k1) distributed by hash(k1) buckets 4 "
                    + "properties(\"replication_num\"=\"1\")");
            OlapTable sameNameReplacement = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_lifecycle_b");
            Assertions.assertNotSame(originalA, sameNameReplacement);
            Assertions.assertTrue(
                    getConstraintMgr().getDistributionMappingConstraints(sameNameReplacement).isEmpty());
            executeSql("drop table mapping_lifecycle_b force");
            executeSql("recover table mapping_lifecycle_b");
            OlapTable recoveredB = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_lifecycle_b");
            Assertions.assertEquals(originalA.getId(), recoveredB.getId());
            Assertions.assertEquals("mapping_a",
                    getConstraintMgr().getDistributionMappingConstraints(recoveredB).get(0).getName());

            OlapTable replacementB = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_replace_b");
            executeSql("alter table mapping_replace_a replace with table mapping_replace_b "
                    + "properties(\"swap\"=\"false\")");
            OlapTable currentReplacement = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_replace_a");
            Assertions.assertSame(replacementB, currentReplacement);
            Assertions.assertEquals("replace_mapping_b",
                    getConstraintMgr().getDistributionMappingConstraints(currentReplacement).get(0).getName());
        } finally {
            executeSql("drop table if exists mapping_lifecycle_a_renamed force");
            executeSql("drop table if exists mapping_lifecycle_b force");
            executeSql("drop table if exists mapping_replace_a force");
            executeSql("drop table if exists mapping_replace_b force");
        }
    }

    @Test
    void distributionMappingFollowsDatabaseRename() throws Exception {
        executeSql("drop database if exists mapping_lifecycle_db force");
        executeSql("drop database if exists mapping_lifecycle_db_renamed force");
        createDatabase("mapping_lifecycle_db");
        createTable("create table mapping_lifecycle_db.mapping_table (k1 int, k2 int) "
                + "duplicate key(k1) distributed by hash(k1) buckets 4 "
                + "properties(\"replication_num\"=\"1\")");
        try {
            addConstraint("alter table mapping_lifecycle_db.mapping_table add constraint mapping "
                    + "colocate mapping mapping_id (k2) determines distribution key (k1) not enforced");
            OlapTable originalTable = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("mapping_lifecycle_db").getTableOrDdlException("mapping_table");

            executeSql("alter database mapping_lifecycle_db rename mapping_lifecycle_db_renamed");

            OlapTable renamedTable = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("mapping_lifecycle_db_renamed")
                    .getTableOrDdlException("mapping_table");
            Assertions.assertSame(originalTable, renamedTable);
            Assertions.assertEquals("mapping",
                    getConstraintMgr().getDistributionMappingConstraints(renamedTable).get(0).getName());

            executeSql("drop database mapping_lifecycle_db_renamed");
            executeSql("recover database mapping_lifecycle_db_renamed");

            OlapTable recoveredTable = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("mapping_lifecycle_db_renamed")
                    .getTableOrDdlException("mapping_table");
            Assertions.assertSame(originalTable, recoveredTable);
            Assertions.assertEquals("mapping",
                    getConstraintMgr().getDistributionMappingConstraints(recoveredTable).get(0).getName());
        } finally {
            executeSql("drop database if exists mapping_lifecycle_db force");
            executeSql("drop database if exists mapping_lifecycle_db_renamed force");
        }
    }

    @Test
    void distributionMappingIsNotCopiedByCreateTableLike() throws Exception {
        createTable("create table mapping_like_source (k1 int, k2 int) "
                + "duplicate key(k1) distributed by hash(k1) buckets 4 "
                + "properties(\"replication_num\"=\"1\")");
        try {
            addConstraint("alter table mapping_like_source add constraint mapping "
                    + "colocate mapping mapping_id (k2) determines distribution key (k1) not enforced");

            executeSql("create table mapping_like_target like mapping_like_source");

            TableIf targetTable = Env.getCurrentInternalCatalog()
                    .getDbOrDdlException("test").getTableOrDdlException("mapping_like_target");
            Assertions.assertTrue(getConstraintMgr().getDistributionMappingConstraints(targetTable).isEmpty());
        } finally {
            executeSql("drop table if exists mapping_like_source force");
            executeSql("drop table if exists mapping_like_target force");
        }
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
