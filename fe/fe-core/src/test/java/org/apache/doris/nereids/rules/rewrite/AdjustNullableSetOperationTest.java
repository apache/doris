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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Nullable attribute of set operation output.
 *
 * <p>A row is in the output of an INTERSECT only when it appears in every child, so the output column can
 * be null only when every child column can be null. For EXCEPT the output is a subset of the left child, so
 * only the left child can make the output column nullable. Applying the UNION rule (nullable if any child
 * column is nullable) to all set operations marks such columns as nullable, which contradicts the
 * not-nullable attribute the upper plan derived earlier and trips the debug check in {@link AdjustNullable}.
 */
public class AdjustNullableSetOperationTest extends TestWithFeService {
    private static final String NULLABLE_TABLE = "adjust_nullable_set_op_nullable";
    private static final String NOT_NULL_TABLE = "adjust_nullable_set_op_not_null";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test." + NULLABLE_TABLE + "(k int null, v int null)"
                + " distributed by hash(k) properties('replication_num' = '1');");
        createTable("create table test." + NOT_NULL_TABLE + "(k int not null, v int null)"
                + " distributed by hash(k) properties('replication_num' = '1');");

        // tables of RQG case 11000258898, the query below used to trip
        // "AdjustNullable convert slot 2004-04-28#xx from not-nullable to nullable"
        createTable("create table test.table_100_undef_partitions2_keys3_properties4_distributed_by52 ("
                + " col_datetime_3__undef_signed_not_null datetime(3) not null,"
                + " col_datetime_3__undef_signed datetime(3) null,"
                + " col_datetime_undef_signed datetime null,"
                + " col_datetime_undef_signed_not_null datetime not null,"
                + " col_date_undef_signed_not_null date not null,"
                + " col_date_undef_signed date null,"
                + " col_decimal_12_2__undef_signed decimal(12,2) null,"
                + " col_decimal_32_6__undef_signed decimal(32,6) null)"
                + " distributed by hash(col_datetime_3__undef_signed) properties('replication_num' = '1');");
        createTable("create table test.table_100_undef_partitions2_keys3_properties4_distributed_by53 ("
                + " col_date_undef_signed_not_null date not null,"
                + " col_date_undef_signed date null,"
                + " col_datetime_3__undef_signed datetime(3) null,"
                + " col_datetime_3__undef_signed_not_null datetime(3) not null,"
                + " col_datetime_undef_signed datetime null,"
                + " col_datetime_undef_signed_not_null datetime not null,"
                + " col_varchar_25__undef_signed varchar(25) null,"
                + " col_char_25__undef_signed_not_null char(25) not null,"
                + " col_char_25__undef_signed char(25) null,"
                + " col_char_40__undef_signed char(40) null,"
                + " col_varchar_100__undef_signed varchar(100) null,"
                + " col_tinyint_undef_signed_not_null tinyint not null,"
                + " col_bigint_undef_signed_not_null bigint not null)"
                + " distributed by hash(col_date_undef_signed_not_null) properties('replication_num' = '1');");
        createTable("create table test.table_100_undef_partitions2_keys3_properties4_distributed_by54 ("
                + " col_datetime_3__undef_signed datetime(3) null,"
                + " col_datetime_3__undef_signed_not_null datetime(3) not null,"
                + " col_datetime_undef_signed datetime null,"
                + " col_datetime_undef_signed_not_null datetime not null,"
                + " col_date_undef_signed_not_null date not null,"
                + " col_date_undef_signed date null,"
                + " col_char_25__undef_signed_not_null char(25) not null,"
                + " col_char_40__undef_signed char(40) null)"
                + " distributed by hash(col_datetime_3__undef_signed) properties('replication_num' = '1');");
        createTable("create table test.table_100_undef_partitions2_keys3_properties4_distributed_by5 ("
                + " col_date_undef_signed_not_null date not null,"
                + " col_datetime_undef_signed datetime null,"
                + " col_datetime_undef_signed_not_null datetime not null,"
                + " col_datetime_3__undef_signed datetime(3) null,"
                + " col_datetime_3__undef_signed_not_null datetime(3) not null,"
                + " col_char_25__undef_signed char(25) null,"
                + " col_char_25__undef_signed_not_null char(25) not null,"
                + " col_varchar_100__undef_signed_not_null varchar(100) not null,"
                + " col_varchar_100__undef_signed varchar(100) null)"
                + " distributed by hash(col_date_undef_signed_not_null) properties('replication_num' = '1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    private void assertOutputNullable(String sql, boolean expectedNullable) {
        assertOutputNullable(sql, expectedNullable, 0);
    }

    private void assertOutputNullable(String sql, boolean expectedNullable, int columnIndex) {
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite();
        Assertions.assertEquals(expectedNullable, checker.getPlan().getOutput().get(columnIndex).nullable(), sql);
    }

    @Test
    void testIntersectOutputNotNull() {
        // every output row is also a row of the not-null side, so the column can not be null
        assertOutputNullable("select k from " + NULLABLE_TABLE
                + " intersect select k from " + NOT_NULL_TABLE, false);
    }

    @Test
    void testIntersectOutputNullable() {
        assertOutputNullable("select v from " + NULLABLE_TABLE
                + " intersect select v from " + NOT_NULL_TABLE, true);
    }

    @Test
    void testUnionOutputNullable() {
        assertOutputNullable("select k from " + NULLABLE_TABLE
                + " union select k from " + NOT_NULL_TABLE, true);
    }

    @Test
    void testUnionOutputNotNull() {
        assertOutputNullable("select k from " + NOT_NULL_TABLE
                + " union select k from " + NOT_NULL_TABLE, false);
    }

    @Test
    void testExceptOutputNotNull() {
        // the output is a subset of the left child
        assertOutputNullable("select k from " + NOT_NULL_TABLE
                + " except select k from " + NULLABLE_TABLE, false);
    }

    @Test
    void testExceptOutputNullable() {
        assertOutputNullable("select k from " + NULLABLE_TABLE
                + " except select k from " + NOT_NULL_TABLE, true);
    }

    @Test
    void testExceptWithConstantBranchOutputNotNull() {
        // RQG case 11000259585 starts with `select 'vZRgkfajsD', 'cKToNmsxok' except (...)`, its second column
        // can only be the constant of the left child, so it is not nullable
        assertOutputNullable("select 'vZRgkfajsD', 'cKToNmsxok' except"
                + " select col_char_25__undef_signed_not_null, col_char_40__undef_signed"
                + " from table_100_undef_partitions2_keys3_properties4_distributed_by54", false, 1);
    }

    @Test
    void testIntersectWithConstantBranchOutputNotNull() {
        // both columns of this intersect can only be the constant, so they are not nullable
        assertOutputNullable("select '2004-04-28', '2014-07-07'"
                + " intersect distinct"
                + " select col_datetime_3__undef_signed_not_null, col_date_undef_signed_not_null"
                + " from table_100_undef_partitions2_keys3_properties4_distributed_by52"
                + " where col_decimal_12_2__undef_signed = 94.0212"
                + " union distinct"
                + " select col_datetime_3__undef_signed, col_datetime_3__undef_signed_not_null"
                + " from table_100_undef_partitions2_keys3_properties4_distributed_by54"
                + " where col_datetime_3__undef_signed = '2011-11-15 13:33:05'"
                + " intersect"
                + " select col_date_undef_signed_not_null, col_date_undef_signed_not_null"
                + " from table_100_undef_partitions2_keys3_properties4_distributed_by53"
                + " intersect"
                + " select col_datetime_3__undef_signed_not_null, col_date_undef_signed_not_null"
                + " from table_100_undef_partitions2_keys3_properties4_distributed_by54 ;", false);
    }
}
