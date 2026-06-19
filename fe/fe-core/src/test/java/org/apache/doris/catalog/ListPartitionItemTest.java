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

package org.apache.doris.catalog;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.mtmv.MTMVPartitionUtil;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link ListPartitionItem#toPartitionKeyDesc} null-partition display handling (FIX-3, CI 973411).
 */
public class ListPartitionItemTest {

    /**
     * A genuine-NULL partition (connector renders it via the {@code __HIVE_DEFAULT_PARTITION__} sentinel and
     * marks the key isNull) and a literal string {@code 'NULL'} partition must produce DISTINCT MTMV partition
     * names. Before FIX-3 both rendered to the bare {@code NULL} keyword -> both named {@code p_NULL} ->
     * "Duplicated named partition: p_NULL" (CI 973411 test_paimon_mtmv on the paimon null_partition table,
     * which has rows for genuine NULL, 'null' and 'NULL'). The genuine-null key must keep isNull=true so
     * {@code region IS NULL} still prunes to it (the IS-NULL-prune fix is preserved); only the DISPLAY name
     * changes.
     */
    @Test
    public void testNullPartitionGetsDistinctNameButStaysNull() throws AnalysisException {
        List<Type> types = Collections.singletonList(Type.VARCHAR);

        // Genuine NULL partition as a paimon/hive connector builds it: a NULL literal whose origin-hive key
        // preserves the canonical sentinel string.
        PartitionKey nullKey = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue(TablePartitionValues.HIVE_DEFAULT_PARTITION, true)),
                types, true);
        // A literal string 'NULL' partition value (NOT a genuine null).
        PartitionKey stringNullKey = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue("NULL", false)), types, true);

        ListPartitionItem nullItem = new ListPartitionItem(Lists.newArrayList(nullKey));
        ListPartitionItem stringNullItem = new ListPartitionItem(Lists.newArrayList(stringNullKey));

        String nullName = MTMVPartitionUtil.generatePartitionName(nullItem.toPartitionKeyDesc(0));
        String stringNullName = MTMVPartitionUtil.generatePartitionName(stringNullItem.toPartitionKeyDesc(0));

        // MUTATION: reverting toPartitionKeyDesc to render the null value as the bare "NULL" keyword makes
        // both names "p_NULL" -> this assertion (and the CREATE MTMV) red.
        Assertions.assertNotEquals(nullName, stringNullName,
                "genuine-null and string-'NULL' partitions must produce distinct MTMV names");
        Assertions.assertEquals("p_NULL", stringNullName,
                "the literal string 'NULL' partition must stay p_NULL");
        Assertions.assertEquals("p_HIVEDEFAULTPARTITION", nullName,
                "the genuine-null partition must be named from the sentinel, not the bare NULL keyword");

        // The null partition's desc value must still resolve to a NULL literal so `col IS NULL` prunes to it.
        // MUTATION: dropping isNull on the substituted display value -> getValue is a non-null literal -> red.
        PartitionValue nullDescValue = nullItem.toPartitionKeyDesc(0).getInValues().get(0).get(0);
        Assertions.assertTrue(nullDescValue.isNullPartition(),
                "the null partition desc value must stay isNull");
        Assertions.assertTrue(nullDescValue.getValue(Type.VARCHAR).isNullLiteral(),
                "the null partition must still resolve to a NULL literal (IS NULL prune preserved)");
    }

    /**
     * Internal OLAP list partitions carry NO originHiveKeys, so the FIX-3 display substitution is a no-op:
     * a genuine NULL OLAP partition value keeps rendering as the bare {@code NULL} keyword (p_NULL).
     */
    @Test
    public void testOlapNullPartitionUnchanged() throws AnalysisException {
        List<Type> types = Collections.singletonList(Type.VARCHAR);
        // isHive=false -> originHiveKeys stays empty -> guard skips -> legacy behavior.
        PartitionKey olapNullKey = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue("NULL", true)), types, false);
        ListPartitionItem item = new ListPartitionItem(Lists.newArrayList(olapNullKey));
        // MUTATION: applying the sentinel substitution unconditionally (ignoring the originHiveKeys guard)
        // would change this to p_HIVEDEFAULTPARTITION -> red.
        Assertions.assertEquals("p_NULL",
                MTMVPartitionUtil.generatePartitionName(item.toPartitionKeyDesc(0)),
                "an OLAP null partition (no originHiveKeys) must be unaffected by the FIX-3 substitution");
    }
}
