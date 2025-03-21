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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.es.EsExternalCatalog;
import org.apache.doris.datasource.es.EsExternalDatabase;
import org.apache.doris.datasource.es.EsExternalTable;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.persist.AlterConstraintLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.util.Lists;
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
import java.util.concurrent.CopyOnWriteArrayList;

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
                connectContext.getEnv());
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
                connectContext.getEnv());
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
    void replayDropConstraintLogTest() throws Exception {
        Config.edit_log_type = "local";
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        List<Pair<Short, AlterConstraintLog>> logs = new CopyOnWriteArrayList<>();
        EditLog editLog = new EditLog("");
        new MockUp<EditLog>() {
            @Mock
            public void logAddConstraint(AlterConstraintLog log) {
                logs.add(Pair.of(OperationType.OP_ADD_CONSTRAINT, log));
            }

            @Mock
            public void logDropConstraint(AlterConstraintLog log) {
                logs.add(Pair.of(OperationType.OP_DROP_CONSTRAINT, log));
            }
        };
        new MockUp<Env>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };
        addConstraint("alter table t1 add constraint pk primary key (k1)");
        addConstraint("alter table t2 add constraint pk primary key (k1)");
        addConstraint("alter table t1 add constraint uk unique (k1)");
        addConstraint("alter table t1 add constraint fk foreign key (k1) references t2(k1)");
        TableIf tableIf = RelationUtil.getTable(
                RelationUtil.getQualifierName(connectContext, Lists.newArrayList("test", "t1")),
                connectContext.getEnv());
        Assertions.assertEquals(3, tableIf.getConstraintsMap().size());
        dropConstraint("alter table t1 drop constraint uk");
        dropConstraint("alter table t1 drop constraint pk");
        dropConstraint("alter table t2 drop constraint pk");
        Assertions.assertEquals(0, tableIf.getConstraintsMap().size());
        for (Pair<Short, AlterConstraintLog> log : logs) {
            JournalEntity journalEntity = new JournalEntity();
            journalEntity.setData(log.second);
            journalEntity.setOpCode(log.first);
            journalEntity.write(output);
        }
        Assertions.assertEquals(0, tableIf.getConstraintsMap().size());
        Assertions.assertEquals(0, tableIf.getConstraintsMap().size());
    }

    @Test
    void constraintWithTablePersistTest() throws Exception {
        addConstraint("alter table t1 add constraint pk primary key (k1)");
        addConstraint("alter table t2 add constraint pk primary key (k1)");
        addConstraint("alter table t1 add constraint uk unique (k1)");
        addConstraint("alter table t1 add constraint fk foreign key (k1) references t2(k1)");
        TableIf tableIf = RelationUtil.getTable(
                RelationUtil.getQualifierName(connectContext, Lists.newArrayList("test", "t1")),
                connectContext.getEnv());
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
        createCatalog("create catalog es properties('type' = 'es', 'elasticsearch.hosts' = 'http://192.168.0.1',"
                + " 'elasticsearch.username' = 'user1');");

        Env.getCurrentEnv().changeCatalog(connectContext, "es");
        EsExternalCatalog esCatalog = (EsExternalCatalog) getCatalog("es");
        EsExternalDatabase db = new EsExternalDatabase(esCatalog, 10002, "es_db1", "es_db1");
        EsExternalTable tbl = new EsExternalTable(10003, "es_tbl1", "es_db1", esCatalog, db);
        ImmutableList<Column> schema = ImmutableList.of(new Column("k1", PrimitiveType.INT));
        tbl.setNewFullSchema(schema);
        db.addTableForTest(tbl);
        esCatalog.addDatabaseForTest(db);
        Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(esCatalog).addSchemaForTest(db.getFullName(), tbl.getName(), schema);
        new MockUp<RelationUtil>() {
            @Mock
            public TableIf getTable(List<String> qualifierName, Env env) {
                return tbl;
            }
        };

        new MockUp<ExternalTable>() {
            @Mock
            public DatabaseIf getDatabase() {
                return db;
            }
        };

        new MockUp<TableIdentifier>() {
            @Mock
            public TableIf toTableIf() {
                return tbl;
            }
        };

        addConstraint("alter table es.es_db1.es_tbl1 add constraint pk primary key (k1)");
        addConstraint("alter table es.es_db1.es_tbl1 add constraint uk unique (k1)");
        TableIf tableIf = RelationUtil.getTable(
                RelationUtil.getQualifierName(connectContext, Lists.newArrayList("test", "t1")),
                connectContext.getEnv());
        Map<String, Constraint> constraintMap = tableIf.getConstraintsMap();
        tableIf.getConstraintsMapUnsafe().clear();
        Assertions.assertTrue(tableIf.getConstraintsMap().isEmpty());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        for (Constraint value : new ArrayList<>(constraintMap.values())) {
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
        Env.getCurrentEnv().changeCatalog(connectContext, "internal");
    }

    @Test
    void dropConstraintLogPersistForExternalTest() throws Exception {
        Config.edit_log_type = "local";
        createCatalog("create catalog es2 properties('type' = 'es', 'elasticsearch.hosts' = 'http://192.168.0.1',"
                + " 'elasticsearch.username' = 'user1');");

        Env.getCurrentEnv().changeCatalog(connectContext, "es2");
        EsExternalCatalog esCatalog = (EsExternalCatalog) getCatalog("es2");
        EsExternalDatabase db = new EsExternalDatabase(esCatalog, 10002, "es_db1", "es_db1");
        EsExternalTable tbl = new EsExternalTable(10003, "es_tbl1", "es_db1", esCatalog, db);
        ImmutableList<Column> schema = ImmutableList.of(new Column("k1", PrimitiveType.INT));
        tbl.setNewFullSchema(schema);
        db.addTableForTest(tbl);
        esCatalog.addDatabaseForTest(db);
        Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(esCatalog).addSchemaForTest(db.getFullName(), tbl.getName(), schema);
        new MockUp<RelationUtil>() {
            @Mock
            public TableIf getTable(List<String> qualifierName, Env env) {
                return tbl;
            }
        };

        new MockUp<ExternalTable>() {
            @Mock
            public DatabaseIf getDatabase() {
                return db;
            }
        };

        new MockUp<TableIdentifier>() {
            @Mock
            public TableIf toTableIf() {
                return tbl;
            }
        };
        addConstraint("alter table es.es_db1.es_tbl1 add constraint pk primary key (k1)");
        addConstraint("alter table es.es_db1.es_tbl1 add constraint uk unique (k1)");
        TableIf tableIf = RelationUtil.getTable(
                RelationUtil.getQualifierName(connectContext, Lists.newArrayList("test", "t1")),
                connectContext.getEnv());
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
}
