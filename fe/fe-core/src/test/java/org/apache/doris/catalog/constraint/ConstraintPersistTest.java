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

package org.apache.doris.catalog.constraint;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.persist.AlterConstraintLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class ConstraintPersistTest extends TestWithFeService implements PlanPatternMatchSupported {

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
    }

    @Test
    void addConstraintLogPersistTest() throws Exception {
        Config.edit_log_type = "local";
        addConstraint("alter table t1 add constraint pk primary key (k1)");
        addConstraint("alter table t2 add constraint pk primary key (k1)");
        addConstraint("alter table t1 add constraint uk unique (k1)");
        addConstraint("alter table t1 add constraint fk foreign key (k1) references t2(k1)");
        TableIf tableIf = RelationUtil.getTable(
                RelationUtil.getQualifierName(connectContext, Lists.newArrayList("test", "t1")),
                connectContext.getEnv(), Optional.empty());
        Map<String, Constraint> constraintMap = tableIf.getConstraintsMap();
        tableIf.getConstraintsMapUnsafe().clear();
        Assertions.assertTrue(tableIf.getConstraintsMap().isEmpty());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        for (Constraint value : constraintMap.values()) {
            JournalEntity journalEntity = new JournalEntity();
            journalEntity.setData(new AlterConstraintLog(value, tableIf));
            journalEntity.setOpCode(OperationType.OP_ADD_CONSTRAINT);
            journalEntity.write(output);
        }
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        for (int i = 0; i < constraintMap.values().size(); i++) {
            JournalEntity journalEntity = new JournalEntity();
            journalEntity.readFields(input);
            EditLog.loadJournal(Env.getCurrentEnv(), 0L, journalEntity);
        }
        Assertions.assertEquals(tableIf.getConstraintsMap(), constraintMap);
        dropConstraint("alter table t1 drop constraint fk");
        dropConstraint("alter table t1 drop constraint pk");
        dropConstraint("alter table t2 drop constraint pk");
        dropConstraint("alter table t1 drop constraint uk");
    }

    @Test
    void dropConstraintLogPersistTest() throws Exception {
        Config.edit_log_type = "local";
        addConstraint("alter table t1 add constraint pk primary key (k1)");
        addConstraint("alter table t2 add constraint pk primary key (k1)");
        addConstraint("alter table t1 add constraint uk unique (k1)");
        addConstraint("alter table t1 add constraint fk foreign key (k1) references t2(k1)");
        TableIf tableIf = RelationUtil.getTable(
                RelationUtil.getQualifierName(connectContext, Lists.newArrayList("test", "t1")),
                connectContext.getEnv(), Optional.empty());
        Map<String, Constraint> constraintMap = tableIf.getConstraintsMap();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        for (Constraint value : constraintMap.values()) {
            JournalEntity journalEntity = new JournalEntity();
            journalEntity.setData(new AlterConstraintLog(value, tableIf));
            journalEntity.setOpCode(OperationType.OP_DROP_CONSTRAINT);
            journalEntity.write(output);
        }
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        for (int i = 0; i < constraintMap.values().size(); i++) {
            JournalEntity journalEntity = new JournalEntity();
            journalEntity.readFields(input);
            EditLog.loadJournal(Env.getCurrentEnv(), 0L, journalEntity);
        }
        Assertions.assertTrue(tableIf.getConstraintsMap().isEmpty());
    }

    @Test
    void constraintWithTablePersistTest() throws Exception {
        addConstraint("alter table t1 add constraint pk primary key (k1)");
        addConstraint("alter table t2 add constraint pk primary key (k1)");
        addConstraint("alter table t1 add constraint uk unique (k1)");
        addConstraint("alter table t1 add constraint fk foreign key (k1) references t2(k1)");
        TableIf tableIf = RelationUtil.getTable(
                RelationUtil.getQualifierName(connectContext, Lists.newArrayList("test", "t1")),
                connectContext.getEnv(), Optional.empty());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        tableIf.write(output);
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        TableIf loadTable = Table.read(input);
        Assertions.assertEquals(loadTable.getConstraintsMap(), tableIf.getConstraintsMap());
        dropConstraint("alter table t1 drop constraint fk");
        dropConstraint("alter table t1 drop constraint pk");
        dropConstraint("alter table t2 drop constraint pk");
        dropConstraint("alter table t1 drop constraint uk");
    }

    @Test
    void externalTableTest() throws Exception {
        ExternalTable externalTable =  new ExternalTable();
        try {
            externalTable.addPrimaryKeyConstraint("pk", ImmutableList.of("col"), false);
        } catch (Exception ignore) {
            // ignore
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        externalTable.write(output);
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        TableIf loadTable = ExternalTable.read(input);
        Assertions.assertEquals(1, loadTable.getConstraintsMap().size());
    }

    @Test
    void addConstraintLogPersistForExternalTableTest() throws Exception {
        Config.edit_log_type = "local";
        FeConstants.runningUnitTest = true;
        createCatalog("create catalog extCtl1 properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\""
                + ");");

        TableIf tableIf = RelationUtil.getTable(
                RelationUtil.getQualifierName(connectContext, Lists.newArrayList("extCtl1", "db1", "tbl11")),
                connectContext.getEnv(), Optional.empty());

        // add constraints
        addConstraint("alter table extCtl1.db1.tbl11 add constraint pk primary key (a11)");
        addConstraint("alter table extCtl1.db1.tbl11 add constraint uk unique (a11)");
        Assertions.assertEquals(2, tableIf.getConstraintsMap().size());
        // clear the constraints
        Map<String, Constraint> constraintMap = tableIf.getConstraintsMap();
        // save constraints map in edit log
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        for (Constraint value : new ArrayList<>(constraintMap.values())) {
            JournalEntity journalEntity = new JournalEntity();
            journalEntity.setData(new AlterConstraintLog(value, tableIf));
            journalEntity.setOpCode(OperationType.OP_ADD_CONSTRAINT);
            journalEntity.write(output);
        }
        // clear constraints map manually
        tableIf.getConstraintsMapUnsafe().clear();
        Assertions.assertTrue(tableIf.getConstraintsMap().isEmpty());
        // add constraints back from edit log
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        for (int i = 0; i < constraintMap.values().size(); i++) {
            JournalEntity journalEntity = new JournalEntity();
            journalEntity.readFields(input);
            EditLog.loadJournal(Env.getCurrentEnv(), 0L, journalEntity);
        }
        Assertions.assertEquals(2, tableIf.getConstraintsMap().size());
    }

    @Test
    void dropConstraintLogPersistForExternalTest() throws Exception {
        Config.edit_log_type = "local";
        FeConstants.runningUnitTest = true;
        createCatalog("create catalog extCtl2 properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider\""
                + ");");

        TableIf tableIf = RelationUtil.getTable(
                RelationUtil.getQualifierName(connectContext, Lists.newArrayList("extCtl2", "db1", "tbl11")),
                connectContext.getEnv(), Optional.empty());

        // add constraints
        addConstraint("alter table extCtl2.db1.tbl11 add constraint pk primary key (a11)");
        addConstraint("alter table extCtl2.db1.tbl11 add constraint uk unique (a11)");
        Assertions.assertEquals(2, tableIf.getConstraintsMap().size());
        // drop it
        // dropConstraint("alter table extCtl2.db1.tbl11 drop constraint pk");
        // dropConstraint("alter table extCtl2.db1.tbl11 drop constraint uk");
        Map<String, Constraint> constraintMap = tableIf.getConstraintsMap();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        for (Constraint value : constraintMap.values()) {
            JournalEntity journalEntity = new JournalEntity();
            journalEntity.setData(new AlterConstraintLog(value, tableIf));
            journalEntity.setOpCode(OperationType.OP_DROP_CONSTRAINT);
            journalEntity.write(output);
        }
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        for (int i = 0; i < constraintMap.values().size(); i++) {
            JournalEntity journalEntity = new JournalEntity();
            journalEntity.readFields(input);
            EditLog.loadJournal(Env.getCurrentEnv(), 0L, journalEntity);
        }
        Assertions.assertTrue(tableIf.getConstraintsMap().isEmpty());

        Env.getCurrentEnv().changeCatalog(connectContext, "internal");
    }

    public static class RefreshCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        public static final Map<String, Map<String, List<Column>>> MOCKED_META;

        static {
            MOCKED_META = Maps.newHashMap();
            Map<String, List<Column>> tblSchemaMap1 = Maps.newHashMap();
            // db1
            tblSchemaMap1.put("tbl11", Lists.newArrayList(
                    new Column("a11", PrimitiveType.BIGINT),
                    new Column("a12", PrimitiveType.STRING),
                    new Column("a13", PrimitiveType.FLOAT)));
            tblSchemaMap1.put("tbl12", Lists.newArrayList(
                    new Column("b21", PrimitiveType.BIGINT),
                    new Column("b22", PrimitiveType.STRING),
                    new Column("b23", PrimitiveType.FLOAT)));
            MOCKED_META.put("db1", tblSchemaMap1);
            // db2
            Map<String, List<Column>> tblSchemaMap2 = Maps.newHashMap();
            tblSchemaMap2.put("tbl21", Lists.newArrayList(
                    new Column("c11", PrimitiveType.BIGINT),
                    new Column("c12", PrimitiveType.STRING),
                    new Column("c13", PrimitiveType.FLOAT)));
            MOCKED_META.put("db2", tblSchemaMap2);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }
}
