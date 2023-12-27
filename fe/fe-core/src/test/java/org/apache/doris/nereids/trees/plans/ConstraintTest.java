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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.commands.AddConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.DropConstraintCommand;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

class ConstraintTest extends TestWithFeService implements PlanPatternMatchSupported {
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
    void primaryKeyConstraintTest() throws Exception {
        AddConstraintCommand addCommand = (AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint pk primary key (k1)");
        addCommand.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(logicalOlapScan().when(o -> {
            Constraint c = o.getTable().getConstraint("pk");
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
                logicalOlapScan().when(o -> o.getTable().getConstraintsMap().isEmpty()));
    }

    @Test
    void uniqueConstraintTest() throws Exception {
        AddConstraintCommand command = (AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint un unique (k1)");
        command.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(logicalOlapScan().when(o -> {
            Constraint c = o.getTable().getConstraint("un");
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
                logicalOlapScan().when(o -> o.getTable().getConstraintsMap().isEmpty()));
    }

    @Test
    void foreignKeyConstraintTest() throws Exception {
        AddConstraintCommand command = (AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint fk foreign key (k1) references t2 (k1)");
        try {
            command.run(connectContext, null);
        } catch (Exception e) {
            Assertions.assertEquals("Foreign key constraint requires a primary key constraint [k1] in t2",
                    e.getMessage());
        }
        ((AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t2 add constraint pk primary key (k1, k2)")).run(connectContext, null);
        ((AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint fk foreign key (k1, k2) references t2(k1, k2)")).run(connectContext,
                null);

        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(logicalOlapScan().when(o -> {
            Constraint c = o.getTable().getConstraint("fk");
            if (c instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint f = (ForeignKeyConstraint) c;
                Column ref1 = f.getReferencedColumn(((SlotReference) o.getOutput().get(0)).getColumn().get().getName());
                Column ref2 = f.getReferencedColumn(((SlotReference) o.getOutput().get(1)).getColumn().get().getName());
                return ref1.getName().equals("k1") && ref2.getName().equals("k2");
            }
            return false;
        }));

        PlanChecker.from(connectContext).parse("select * from t2").analyze().matches(logicalOlapScan().when(o -> {
            Constraint c = o.getTable().getConstraint("pk");
            if (c instanceof PrimaryKeyConstraint) {
                Set<String> columnNames = ((PrimaryKeyConstraint) c).getPrimaryKeyNames();
                List<TableIf> foreignTables = ((PrimaryKeyConstraint) c).getForeignTables();
                return columnNames.size() == 2
                        && columnNames.equals(Sets.newHashSet("k1", "k2"))
                        && foreignTables.size() == 1 && foreignTables.get(0).getName().equals("t1");
            }
            return false;
        }));

        // drop fk
        DropConstraintCommand dropCommand = (DropConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 drop constraint fk");
        dropCommand.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> o.getTable().getConstraintsMap().isEmpty()));
        // drop pk and fk referenced it also should be dropped
        ((AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint fk foreign key (k1, k2) references t2(k1, k2)")).run(connectContext,
                null);
        ((DropConstraintCommand) new NereidsParser().parseSingle("alter table t2 drop constraint pk"))
                .run(connectContext, null);

        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> o.getTable().getConstraintsMap().isEmpty()));
        PlanChecker.from(connectContext).parse("select * from t2").analyze().matches(
                logicalOlapScan().when(o -> o.getTable().getConstraintsMap().isEmpty()));
    }
}
