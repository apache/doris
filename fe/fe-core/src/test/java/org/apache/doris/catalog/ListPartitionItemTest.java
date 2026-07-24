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
 * Tests for {@link ListPartitionItem#toPartitionKeyDesc} null-partition display handling.
 *
 * <p>Guards the naming of a genuine-NULL partition: its MTMV partition NAME must be {@code pn_NULL} (the
 * {@code pn_} prefix marks a null-bearing partition), NOT the bare {@code p_NULL}. A real NULL value and a
 * literal {@code 'NULL'} string both render to the text {@code NULL} once {@code PartitionKeyDesc} quotes the
 * value and the name pattern strips the quotes, so on a column holding both (e.g. paimon
 * {@code null_partition}: a real NULL row plus a {@code 'NULL'} string row) they would BOTH be named
 * {@code p_NULL} and fail the partition-uniqueness check (regression test_paimon_mtmv). The {@code pn_} prefix
 * keeps them distinct: a string value always yields a {@code p_}-prefixed name, so {@code pn_} can never
 * collide. The nullness is read from {@link PartitionValue#isNullPartition()}, which BOTH the connector-side
 * genuine-null item and the MV-OLAP item carry, so both render the SAME {@code pn_NULL} — keeping the
 * related-desc vs MV-OLAP-desc partition-mapping join symmetric (unlike the reverted FIX-3, which used the
 * one-sided {@code originHiveKeys} sentinel and broke the join).
 */
public class ListPartitionItemTest {

    /**
     * A genuine-NULL partition (e.g. a hive {@code __HIVE_DEFAULT_PARTITION__} default partition, built isNull
     * with the sentinel preserved as originHiveKeys) must render its MTMV partition name as {@code pn_NULL} so
     * that (a) it never collides with a literal {@code 'NULL'} string partition (which renders {@code p_NULL})
     * and (b) the MV-OLAP partition (which has no originHiveKeys) renders the SAME name, keeping the
     * sync-compare join symmetric. The value must still resolve to a NULL literal so {@code col IS NULL}
     * pruning is unaffected.
     */
    @Test
    public void testGenuineNullPartitionRendersAsPnNull() throws AnalysisException {
        List<Type> types = Collections.singletonList(Type.VARCHAR);

        // Genuine NULL partition as a hive/paimon connector builds it: a NULL literal whose origin-hive key
        // preserves the canonical sentinel string.
        PartitionKey nullKey = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue(TablePartitionValues.HIVE_DEFAULT_PARTITION, true)),
                types, true);
        ListPartitionItem nullItem = new ListPartitionItem(Lists.newArrayList(nullKey));

        Assertions.assertEquals("pn_NULL",
                MTMVPartitionUtil.generatePartitionName(nullItem.toPartitionKeyDesc(0)),
                "a genuine-null partition must render as pn_NULL (distinct from a literal 'NULL' string's p_NULL)");

        // The null partition's desc value must still resolve to a NULL literal so `col IS NULL` prunes to it.
        PartitionValue nullDescValue = nullItem.toPartitionKeyDesc(0).getInValues().get(0).get(0);
        Assertions.assertTrue(nullDescValue.isNullPartition(),
                "the null partition desc value must stay isNull");
        Assertions.assertTrue(nullDescValue.getValue(Type.VARCHAR).isNullLiteral(),
                "the null partition must still resolve to a NULL literal (IS NULL prune preserved)");
    }

    /**
     * An internal OLAP null partition (no originHiveKeys) renders as {@code pn_NULL} — the SAME name the
     * connector-side genuine-null item produces. Kept as a symmetry anchor for
     * {@link #testGenuineNullPartitionRendersAsPnNull}: both sides must produce the SAME pn_NULL name so the
     * partition-mapping join stays symmetric.
     */
    @Test
    public void testOlapNullPartitionRendersAsPnNull() throws AnalysisException {
        List<Type> types = Collections.singletonList(Type.VARCHAR);
        PartitionKey olapNullKey = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue("NULL", true)), types, false);
        ListPartitionItem item = new ListPartitionItem(Lists.newArrayList(olapNullKey));
        Assertions.assertEquals("pn_NULL",
                MTMVPartitionUtil.generatePartitionName(item.toPartitionKeyDesc(0)),
                "an OLAP null partition (no originHiveKeys) must render as pn_NULL");
    }

    /**
     * A literal {@code 'NULL'} string partition (NOT a genuine null) must keep the bare {@code p_NULL} name —
     * it is ordinary string data and must stay distinct from the real-NULL partition's {@code pn_NULL}. This
     * is the collision the {@code pn_} prefix resolves (regression test_paimon_mtmv, which has both).
     */
    @Test
    public void testLiteralNullStringPartitionRendersAsPNull() throws AnalysisException {
        List<Type> types = Collections.singletonList(Type.VARCHAR);
        PartitionKey strKey = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue("NULL")), types, false);
        ListPartitionItem item = new ListPartitionItem(Lists.newArrayList(strKey));
        Assertions.assertEquals("p_NULL",
                MTMVPartitionUtil.generatePartitionName(item.toPartitionKeyDesc(0)),
                "a literal 'NULL' string partition must render as p_NULL, distinct from the real-NULL pn_NULL");
    }
}
