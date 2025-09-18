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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator.SlotSizeComparator;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CountStarSmallestSlotTest extends TestWithFeService {

    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testCountStarSmallestSlotTest() throws Exception {
        createDatabase("count_star_test_db;");
        createTables(
                "create table count_star_test_db.count_test1(k1 varchar(1), k2 int) "
                        + "properties('replication_num' = '1')",
                "create table count_star_test_db.count_test2(k3 int, k2 varchar(1)) "
                        + "properties('replication_num' = '1')",
                "create table count_star_test_db.count_test3(k1 varchar(35), k2 varchar(1), k3 int) "
                        + "properties('replication_num' = '1')",
                "create table count_star_test_db.count_test4(k1 varchar(35), k2 varchar(1)) "
                        + "properties('replication_num' = '1')",
                "create table count_star_test_db.count_test5(k1 varchar(35), k2 int) partition by range(k2) "
                        + "(partition p1 values less than ('100')) "
                        + "properties('replication_num' = '1')");
        checkCountStarSlot("select count(*) from count_star_test_db.count_test1", "k2");
        checkCountStarSlot("select count(*) from count_star_test_db.count_test2", "k3");
        checkCountStarSlot("select count(*) from count_star_test_db.count_test3", "k3");
        checkCountStarSlot("select count(*) from count_star_test_db.count_test4", "k2");
        checkCountStarSlot("select count(*) from count_star_test_db.count_test5", "k2");
    }

    private void checkCountStarSlot(String sql, String countCol) {
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        PlanFragment fragment = new PhysicalPlanTranslator(
                new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        List<OlapScanNode> scanNodes = Lists.newArrayList();
        fragment.getPlanRoot().collect(OlapScanNode.class, scanNodes);
        List<SlotDescriptor> slots = ((OlapScanNode) scanNodes.get(0)).getTupleDesc().getSlots();
        Assertions.assertEquals(1, slots.size());
        Assertions.assertEquals(countCol, slots.get(0).getColumn().getName());
    }

    @Test
    public void testGetSmallestSlotWithNullInput() {
        // Test null list
        Assertions.assertNull(PhysicalPlanTranslator.getSmallestSlot(null));

        // Test empty list
        Assertions.assertNull(PhysicalPlanTranslator.getSmallestSlot(Lists.newArrayList()));
    }

    @Test
    public void testGetSmallestSlotWithSingleElement() {
        SlotDescriptor slot = createSlotDescriptor(Type.INT, "test_col");
        List<SlotDescriptor> slots = Lists.newArrayList(slot);

        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);
        Assertions.assertEquals(slot, result);
    }

    @Test
    public void testGetSmallestSlotNumericTypePriority() {
        // Test numeric types have priority 1 - should always win
        SlotDescriptor intSlot = createSlotDescriptor(Type.INT, "int_col");
        SlotDescriptor stringSlot = createSlotDescriptor(Type.STRING, "string_col");
        SlotDescriptor arraySlot = createSlotDescriptor(Type.ARRAY, "array_col");

        List<SlotDescriptor> slots = Lists.newArrayList(stringSlot, arraySlot, intSlot);
        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);
        Assertions.assertEquals(intSlot, result);
    }

    @Test
    public void testGetSmallestSlotStringTypePriority() {
        // Test string types have priority 2 - should win over complex types
        SlotDescriptor stringSlot = createSlotDescriptor(Type.STRING, "string_col");
        SlotDescriptor arraySlot = createSlotDescriptor(Type.ARRAY, "array_col");
        SlotDescriptor structSlot = createSlotDescriptor(Type.STRUCT, "struct_col");

        List<SlotDescriptor> slots = Lists.newArrayList(arraySlot, structSlot, stringSlot);
        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);
        Assertions.assertEquals(stringSlot, result);
    }

    @Test
    public void testGetSmallestSlotComplexTypePriority() {
        // Test complex types have priority 3
        SlotDescriptor arraySlot = createSlotDescriptor(Type.ARRAY, "array_col");
        SlotDescriptor mapSlot = createSlotDescriptor(Type.MAP, "map_col");
        SlotDescriptor structSlot = createSlotDescriptor(Type.STRUCT, "struct_col");

        List<SlotDescriptor> slots = Lists.newArrayList(mapSlot, structSlot, arraySlot);
        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);
        // Should return the first one since they have same priority, compared by slot size
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result == mapSlot);
    }

    @Test
    public void testGetSmallestSlotAllNumericTypes() {
        // Test all numeric types covered in Type.java
        List<SlotDescriptor> slots = Lists.newArrayList();

        // Boolean type
        slots.add(createSlotDescriptor(Type.BOOLEAN, "boolean_col"));

        // Integer types
        slots.add(createSlotDescriptor(Type.TINYINT, "tinyint_col"));
        slots.add(createSlotDescriptor(Type.SMALLINT, "smallint_col"));
        slots.add(createSlotDescriptor(Type.INT, "int_col"));
        slots.add(createSlotDescriptor(Type.BIGINT, "bigint_col"));
        slots.add(createSlotDescriptor(Type.LARGEINT, "largeint_col"));

        // Floating point types
        slots.add(createSlotDescriptor(Type.FLOAT, "float_col"));
        slots.add(createSlotDescriptor(Type.DOUBLE, "double_col"));

        // Decimal types
        slots.add(createSlotDescriptor(Type.DECIMALV2, "decimalv2_col"));
        slots.add(createSlotDescriptor(Type.DECIMAL32, "decimal32_col"));
        slots.add(createSlotDescriptor(Type.DECIMAL64, "decimal64_col"));
        slots.add(createSlotDescriptor(Type.DECIMAL128, "decimal128_col"));
        slots.add(createSlotDescriptor(Type.DECIMAL256, "decimal256_col"));

        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getType().isBoolean());
    }

    @Test
    public void testGetSmallestSlotAllStringTypes() {
        // Test all string types covered in Type.java
        List<SlotDescriptor> slots = Lists.newArrayList();

        slots.add(createSlotDescriptor(Type.CHAR, "char_col"));
        slots.add(createSlotDescriptor(Type.VARCHAR, "varchar_col"));
        slots.add(createSlotDescriptor(Type.STRING, "string_col"));

        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result == slots.get(0));
    }

    @Test
    public void testGetSmallestSlotSpecialTypes() {
        // Test special/object types
        List<SlotDescriptor> slots = Lists.newArrayList();

        slots.add(createSlotDescriptor(Type.HLL, "hll_col"));
        slots.add(createSlotDescriptor(Type.BITMAP, "bitmap_col"));
        slots.add(createSlotDescriptor(Type.QUANTILE_STATE, "quantile_state_col"));
        slots.add(createSlotDescriptor(Type.JSONB, "jsonb_col"));
        slots.add(createSlotDescriptor(Type.VARIANT, "variant_col"));

        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGetSmallestSlotSlotSizeComparison() {
        // Test slot size comparison when types have same priority
        // Create two TINYINT slots to test slot size comparison
        SlotDescriptor slot1 = createSlotDescriptor(Type.TINYINT, "tinyint1");
        SlotDescriptor slot2 = createSlotDescriptor(Type.SMALLINT, "smallint1");

        List<SlotDescriptor> slots = Lists.newArrayList(slot2, slot1);
        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);

        // TINYINT should be selected as it has smaller slot size
        Assertions.assertEquals(slot1, result);
        Assertions.assertEquals(PrimitiveType.TINYINT, result.getType().getPrimitiveType());
    }

    @Test
    public void testGetSmallestSlotStringLengthComparison() {
        // Test length comparison for string types when slot sizes are equal
        SlotDescriptor charSlot1 = createSlotDescriptorWithLength(ScalarType.createCharType(5), "char5");
        SlotDescriptor charSlot2 = createSlotDescriptorWithLength(ScalarType.createCharType(10), "char10");

        List<SlotDescriptor> slots = Lists.newArrayList(charSlot2, charSlot1);
        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);

        // Char with length 5 should be selected
        Assertions.assertEquals(charSlot1, result);
    }

    @Test
    public void testGetSmallestSlotDecimalComparison() {
        // Test decimal types with different precisions
        SlotDescriptor decimal32 = createSlotDescriptor(Type.DECIMAL32, "decimal32");
        SlotDescriptor decimal64 = createSlotDescriptor(Type.DECIMAL64, "decimal64");
        SlotDescriptor decimal128 = createSlotDescriptor(Type.DECIMAL128, "decimal128");
        SlotDescriptor decimal256 = createSlotDescriptor(Type.DECIMAL256, "decimal256");

        List<SlotDescriptor> slots = Lists.newArrayList(decimal256, decimal128, decimal64, decimal32);
        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);

        // DECIMAL32 should be selected as smallest
        Assertions.assertEquals(decimal32, result);
        Assertions.assertEquals(PrimitiveType.DECIMAL32, result.getType().getPrimitiveType());
    }

    @Test
    public void testGetSmallestSlotMixedTypesWithDifferentPriorities() {
        // Test comprehensive scenario with all priority levels
        SlotDescriptor numericSlot = createSlotDescriptor(Type.BIGINT, "bigint_col");
        SlotDescriptor stringSlot = createSlotDescriptor(Type.VARCHAR, "varchar_col");
        SlotDescriptor complexSlot = createSlotDescriptor(Type.ARRAY, "array_col");
        SlotDescriptor objectSlot = createSlotDescriptor(Type.HLL, "hll_col");

        List<SlotDescriptor> slots = Lists.newArrayList(objectSlot, complexSlot, stringSlot, numericSlot);
        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);

        // Numeric type should win due to priority
        Assertions.assertEquals(numericSlot, result);
    }

    @Test
    public void testGetSmallestSlotVarbinaryType() {
        // Test VARBINARY type
        SlotDescriptor varbinarySlot = createSlotDescriptor(Type.VARBINARY, "varbinary_col");
        SlotDescriptor intSlot = createSlotDescriptor(Type.INT, "int_col");

        List<SlotDescriptor> slots = Lists.newArrayList(varbinarySlot, intSlot);
        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);

        // INT should win due to numeric priority
        Assertions.assertEquals(intSlot, result);
    }

    @Test
    public void testGetSmallestSlotLambdaFunctionType() {
        // Test LAMBDA_FUNCTION type
        SlotDescriptor lambdaSlot = createSlotDescriptor(Type.LAMBDA_FUNCTION, "lambda_col");
        SlotDescriptor stringSlot = createSlotDescriptor(Type.STRING, "string_col");

        List<SlotDescriptor> slots = Lists.newArrayList(lambdaSlot, stringSlot);
        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);

        // STRING should win due to string priority vs other types
        Assertions.assertEquals(stringSlot, result);
    }

    @Test
    public void testGetSmallestSlotInvalidAndNullTypes() {
        // Test with INVALID and NULL types
        SlotDescriptor invalidSlot = createSlotDescriptor(Type.INVALID, "invalid_col");
        SlotDescriptor nullSlot = createSlotDescriptor(Type.NULL, "null_col");
        SlotDescriptor intSlot = createSlotDescriptor(Type.INT, "int_col");

        List<SlotDescriptor> slots = Lists.newArrayList(invalidSlot, nullSlot, intSlot);
        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);

        // INT should be selected as it's a valid numeric type
        Assertions.assertEquals(intSlot, result);
    }

    @Test
    public void testGetSmallestSlotAllTypesComprehensive() {
        // Comprehensive test with representative types from each category
        List<SlotDescriptor> slots = Lists.newArrayList();

        // Add one representative from each major type category
        slots.add(createSlotDescriptor(Type.BOOLEAN, "boolean_col"));           // Numeric priority
        slots.add(createSlotDescriptor(Type.TINYINT, "tinyint_col"));          // Numeric priority
        slots.add(createSlotDescriptor(Type.INT, "int_col"));                  // Numeric priority
        slots.add(createSlotDescriptor(Type.BIGINT, "bigint_col"));            // Numeric priority
        slots.add(createSlotDescriptor(Type.FLOAT, "float_col"));              // Numeric priority
        slots.add(createSlotDescriptor(Type.DOUBLE, "double_col"));            // Numeric priority
        slots.add(createSlotDescriptor(Type.DECIMALV2, "decimalv2_col"));      // Numeric priority
        slots.add(createSlotDescriptor(Type.DECIMAL32, "decimal32_col"));      // Numeric priority

        slots.add(createSlotDescriptor(Type.CHAR, "char_col"));                // String priority
        slots.add(createSlotDescriptor(Type.VARCHAR, "varchar_col"));          // String priority
        slots.add(createSlotDescriptor(Type.STRING, "string_col"));            // String priority

        slots.add(createSlotDescriptor(Type.DATE, "date_col"));                // Other priority
        slots.add(createSlotDescriptor(Type.DATETIME, "datetime_col"));        // Other priority
        slots.add(createSlotDescriptor(Type.DATEV2, "datev2_col"));           // Other priority
        slots.add(createSlotDescriptor(Type.DATETIMEV2, "datetimev2_col"));   // Other priority
        slots.add(createSlotDescriptor(Type.TIMEV2, "timev2_col"));           // Other priority

        slots.add(createSlotDescriptor(Type.IPV4, "ipv4_col"));               // Other priority
        slots.add(createSlotDescriptor(Type.IPV6, "ipv6_col"));               // Other priority

        slots.add(createSlotDescriptor(Type.HLL, "hll_col"));                 // Other priority
        slots.add(createSlotDescriptor(Type.BITMAP, "bitmap_col"));           // Other priority
        slots.add(createSlotDescriptor(Type.QUANTILE_STATE, "quantile_col")); // Other priority
        slots.add(createSlotDescriptor(Type.JSONB, "jsonb_col"));             // Other priority
        slots.add(createSlotDescriptor(Type.VARIANT, "variant_col"));         // Other priority
        slots.add(createSlotDescriptor(Type.VARBINARY, "varbinary_col"));     // Other priority

        slots.add(createSlotDescriptor(Type.ARRAY, "array_col"));             // Complex priority
        slots.add(createSlotDescriptor(Type.MAP, "map_col"));                 // Complex priority
        slots.add(createSlotDescriptor(Type.STRUCT, "struct_col"));           // Complex priority

        SlotDescriptor result = PhysicalPlanTranslator.getSmallestSlot(slots);
        Assertions.assertNotNull(result);
        // Result should be a numeric type due to priority
        Assertions.assertTrue(result.getType().isBoolean());

        slots.sort(new SlotSizeComparator());
        for (int i = 0; i < slots.size(); i++) {
            SlotDescriptor slot = slots.get(i);
            switch (i) {
                case 0:
                    Assertions.assertTrue(slot.getType().isBoolean());
                    break;
                case 1:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.TINYINT);
                    break;
                case 2:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.INT);
                    break;
                case 3:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.FLOAT);
                    break;
                case 4:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.DECIMAL32);
                    break;
                case 5:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.DATEV2);
                    break;
                case 6:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.IPV4);
                    break;
                case 7:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.BIGINT);
                    break;
                case 8:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.DOUBLE);
                    break;
                case 9:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.DATETIMEV2);
                    break;
                case 10:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.TIMEV2);
                    break;
                case 11:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.DECIMALV2);
                    break;
                case 12:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.DATE);
                    break;
                case 13:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.DATETIME);
                    break;
                case 14:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.IPV6);
                    break;
                case 15:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.CHAR);
                    break;
                case 16:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.VARCHAR);
                    break;
                case 17:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.STRING);
                    break;
                case 18:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.MAP);
                    break;
                case 19:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.STRUCT);
                    break;
                case 20:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.ARRAY);
                    break;
                case 21:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.HLL);
                    break;
                case 22:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.BITMAP);
                    break;
                case 23:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.QUANTILE_STATE);
                    break;
                case 24:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.JSONB);
                    break;
                case 25:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.VARBINARY);
                    break;
                case 26:
                    Assertions.assertTrue(slot.getType().getPrimitiveType() == PrimitiveType.VARIANT);
                    break;
                default:
                    // Remaining types can be in any order due to similar slot sizes
                    Assertions.assertNotNull(false);
            }
        }
    }

    /**
     * Helper method to create a SlotDescriptor with the given type and column name
     */
    private SlotDescriptor createSlotDescriptor(Type type, String columnName) {
        SlotDescriptor slot = new SlotDescriptor(new SlotId(0), null);
        slot.setType(type);
        // Create a mock column for the slot
        Column column = new Column(columnName, type);
        slot.setColumn(column);
        return slot;
    }

    /**
     * Helper method to create a SlotDescriptor with specific type and length
     */
    private SlotDescriptor createSlotDescriptorWithLength(Type type, String columnName) {
        SlotDescriptor slot = new SlotDescriptor(new SlotId(0), null);
        slot.setType(type);
        Column column = new Column(columnName, type);
        slot.setColumn(column);
        return slot;
    }
}
