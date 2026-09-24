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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class HivePartitionFilterBuilderTest {
    @Test
    public void testBuildsEqualityAndInFilter() {
        Column city = new Column("city", Type.STRING, true);
        Column day = new Column("day", Type.INT, true);
        SlotReference citySlot = new SlotReference("city", StringType.INSTANCE);
        SlotReference daySlot = new SlotReference("day", IntegerType.INSTANCE);
        List<org.apache.doris.nereids.trees.expressions.Expression> predicates = Arrays.asList(
                new EqualTo(citySlot, new StringLiteral("shanghai")),
                new InPredicate(daySlot, Arrays.asList(new IntegerLiteral(1), new IntegerLiteral(2))));

        Assertions.assertEquals("(city = 'shanghai') AND (day = 1 OR day = 2)",
                HivePartitionFilterBuilder.build(new And(predicates), Arrays.asList(city, day),
                        ImmutableMap.of("city", "string", "day", "int")));
    }

    @Test
    public void testRejectsUnsafeStringAndUnsupportedPredicate() {
        Column city = new Column("city", Type.STRING, true);
        Column day = new Column("day", Type.INT, true);
        SlotReference citySlot = new SlotReference("city", StringType.INSTANCE);
        SlotReference daySlot = new SlotReference("day", IntegerType.INSTANCE);

        Assertions.assertNull(HivePartitionFilterBuilder.build(
                new EqualTo(citySlot, new StringLiteral("can't")), Arrays.asList(city),
                ImmutableMap.of("city", "string")));
        Assertions.assertNull(HivePartitionFilterBuilder.build(
                new EqualTo(daySlot, new StringLiteral("1")), Arrays.asList(day),
                ImmutableMap.of("day", "int")));
        Assertions.assertNull(HivePartitionFilterBuilder.build(
                new EqualTo(citySlot, daySlot),
                Arrays.asList(city), ImmutableMap.of("city", "string")));
    }

    @Test
    public void testRejectsFilterUnsafeColumnNames() {
        Column date = new Column("date", Type.STRING, true);
        Column digits = new Column("123", Type.STRING, true);
        SlotReference dateSlot = new SlotReference("date", StringType.INSTANCE);
        SlotReference digitsSlot = new SlotReference("123", StringType.INSTANCE);

        Assertions.assertNull(HivePartitionFilterBuilder.build(
                new EqualTo(dateSlot, new StringLiteral("20260101")), Arrays.asList(date),
                ImmutableMap.of("date", "string")));
        Assertions.assertNull(HivePartitionFilterBuilder.build(
                new EqualTo(digitsSlot, new StringLiteral("x")), Arrays.asList(digits),
                ImmutableMap.of("123", "string")));
    }

    @Test
    public void testRejectsNormalizedStringTypesThatHmsFilterDoesNotSupport() {
        Column value = new Column("value", Type.STRING, true);
        SlotReference valueSlot = new SlotReference("value", StringType.INSTANCE);
        EqualTo predicate = new EqualTo(valueSlot, new StringLiteral("x"));

        Assertions.assertNull(HivePartitionFilterBuilder.build(
                predicate, Arrays.asList(value), ImmutableMap.of("value", "char(10)")));
        Assertions.assertNull(HivePartitionFilterBuilder.build(
                predicate, Arrays.asList(value), ImmutableMap.of("value", "varchar(10)")));
        Assertions.assertNull(HivePartitionFilterBuilder.build(
                predicate, Arrays.asList(value), ImmutableMap.of("value", "binary")));
    }
}
