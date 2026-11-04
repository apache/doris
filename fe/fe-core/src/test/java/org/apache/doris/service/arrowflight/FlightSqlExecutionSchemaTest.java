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

package org.apache.doris.service.arrowflight;

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.service.arrowflight.sessions.FlightSqlConnectContext;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class FlightSqlExecutionSchemaTest extends TestWithFeService {
    private FlightSqlConnectContext flightContext;
    private boolean previousUnitTest;

    @Override
    protected void runBeforeAll() throws Exception {
        previousUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        createDatabase("execution_schema_test");
        connectContext.setDatabase("execution_schema_test");
        createTable("CREATE TABLE schema_input (id INT NOT NULL, encoded STRING NULL) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        flightContext = new FlightSqlConnectContext("schema-peer");
        flightContext.setEnv(connectContext.getEnv());
        flightContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        flightContext.setSessionVariable(connectContext.getSessionVariable());
        flightContext.setDatabase("execution_schema_test");
    }

    @Override
    protected void runAfterAll() throws Exception {
        FeConstants.runningUnitTest = previousUnitTest;
        flightContext.getFlightSqlChannel().close();
    }

    @Test
    void unaliasedLiteralRetainsExecutionLabel() throws Exception {
        assertExecutionSchema("SELECT 1", false, false);
        assertExecutionSchema("SELECT 1 AS id", false, false);
    }

    @Test
    void unaliasedFunctionRetainsExecutionLabel() throws Exception {
        assertExecutionSchema("SELECT unhex(encoded) FROM schema_input WHERE id <= 4 ORDER BY id", false, false);
    }

    @Test
    void constantFoldingNarrowsNullableCast() throws Exception {
        assertExecutionSchema("SELECT CAST(CAST('2026-01-01' AS DATETIME(6)) AS STRING)", false, true);
    }

    @Test
    void nullableColumnRemainsCompatibleWithFilter() throws Exception {
        assertExecutionSchema("SELECT encoded FROM schema_input WHERE encoded IS NOT NULL", false, false);
    }

    @Test
    void tableValuedFunctionUsesQualifiedWireLabel() throws Exception {
        Schema actual = assertExecutionSchema("SELECT * FROM numbers('number' = '1')", true, false);
        Assertions.assertTrue(actual.getFields().get(0).getName().startsWith("_tvf_numbers."));
    }

    private Schema assertExecutionSchema(String sql, boolean differentLabel, boolean narrowedNullability)
            throws Exception {
        flightContext.setThreadLocalInfo();
        try (FlightSqlConnectProcessor processor = new FlightSqlConnectProcessor(flightContext)) {
            Schema prepared = FlightSqlQuerySchema.analyze(flightContext, sql);
            // Execution marks the query state before binding output labels. A bare planner call
            // misses that transition and can reproduce Prepare's incorrect command-style aliases.
            processor.handleQuery(sql);
            Assertions.assertNotEquals(MysqlStateType.ERR, flightContext.getState().getStateType(),
                    flightContext.getState().getErrorMessage());
            Assertions.assertFalse(flightContext.isReturnResultFromLocal());
            Assertions.assertNotNull(flightContext.getExecutor());
            LogicalPlanAdapter statement = (LogicalPlanAdapter) flightContext.getExecutor().getParsedStmt();
            List<Field> wireFields = new ArrayList<>();
            for (Expr expression : statement.getResultExprs()) {
                TExprNode node = expression.treeToThrift().getNodes().get(0);
                Assertions.assertEquals(TExprNodeType.SLOT_REF, node.getNodeType());
                ScalarType type = (ScalarType) expression.getType();
                ArrowType arrowType = FlightSqlSchemaHelper.getArrowType(type.getPrimitiveType(),
                        type.getScalarPrecision(), type.getScalarScale());
                // BE derives these fields from the serialized output expressions, not analyzed Slot names.
                wireFields.add(new Field(node.getLabel(),
                        new FieldType(node.isIsNullable(), arrowType, null), Collections.emptyList()));
            }
            Schema actual = new Schema(wireFields);
            Assertions.assertEquals(differentLabel,
                    !prepared.getFields().get(0).getName().equals(actual.getFields().get(0).getName()));
            Assertions.assertEquals(narrowedNullability,
                    prepared.getFields().get(0).isNullable() && !actual.getFields().get(0).isNullable());
            Assertions.assertTrue(FlightSqlQuerySchema.matchesExecutionSchema(
                    prepared, actual, statement.getColLabels()));
            return actual;
        } finally {
            flightContext.closeFlightSqlDeferredExecutors();
            flightContext.clearFlightSqlEndpointsLocations();
            flightContext.getFlightSqlChannel().reset();
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    void sameArityRenameIsRejectedDespiteIdenticalWireTypes() {
        Schema schema = schema(Field.nullable("old_name", new ArrowType.Utf8()));
        Assertions.assertFalse(FlightSqlQuerySchema.matchesExecutionSchema(schema, schema,
                Collections.singletonList("new_name")));
    }

    @Test
    void differentTypesAreRejected() {
        Schema prepared = schema(Field.nullable("value", new ArrowType.Int(32, true)));
        Schema actual = schema(Field.nullable("value", new ArrowType.Int(64, true)));
        Assertions.assertFalse(FlightSqlQuerySchema.matchesExecutionSchema(prepared, actual,
                Collections.singletonList("value")));
    }

    @Test
    void nestedFieldNamesAreNotTransportLabels() {
        Schema prepared = schema(new Field("value", FieldType.nullable(new ArrowType.Struct()),
                Collections.singletonList(Field.nullable("old_name", new ArrowType.Utf8()))));
        Schema actual = schema(new Field("value", FieldType.nullable(new ArrowType.Struct()),
                Collections.singletonList(Field.nullable("new_name", new ArrowType.Utf8()))));
        Assertions.assertFalse(FlightSqlQuerySchema.matchesExecutionSchema(prepared, actual,
                Collections.singletonList("value")));
    }

    @Test
    void typeMetadataChangesAreRejected() {
        Schema prepared = schema(new Field("value", new FieldType(true, new ArrowType.Utf8(), null,
                Collections.singletonMap("doris_type", "LARGEINT")), Collections.emptyList()));
        Schema actual = schema(Field.nullable("value", new ArrowType.Utf8()));
        Assertions.assertFalse(FlightSqlQuerySchema.matchesExecutionSchema(prepared, actual,
                Collections.singletonList("value")));
    }

    @Test
    void nullableWideningIsRejected() {
        Schema prepared = schema(Field.notNullable("value", new ArrowType.Utf8()));
        Schema actual = schema(Field.nullable("value", new ArrowType.Utf8()));
        Assertions.assertFalse(FlightSqlQuerySchema.matchesExecutionSchema(prepared, actual,
                Collections.singletonList("value")));
    }

    private static Schema schema(Field field) {
        return new Schema(Collections.singletonList(field));
    }
}
