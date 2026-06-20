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
 * <p>Guards the master-parity rendering of a genuine-NULL partition: its MTMV partition NAME must be the bare
 * {@code p_NULL}, the same as on master ({@link PartitionInfo#toPartitionValue} renders a {@code NullLiteral}
 * key as the keyword {@code NULL}). FIX-3 (CI 973411, reverted) substituted the {@code originHiveKeys} sentinel
 * for the display value, renaming the hive default-partition MV to {@code p_HIVEDEFAULTPARTITION} and breaking
 * regression test_hive_default_mtmv (asserts {@code p_NULL}) and test_upgrade_downgrade_mtmv (the related-desc
 * vs MV-OLAP-desc partition-mapping join, which only matches when both sides render symmetrically).
 */
public class ListPartitionItemTest {

    /**
     * A genuine-NULL partition (e.g. a hive {@code __HIVE_DEFAULT_PARTITION__} default partition, built isNull
     * with the sentinel preserved as originHiveKeys) must render its MTMV partition name as the bare
     * {@code p_NULL} — identical to master — so that (a) test_hive_default_mtmv:93 finds p_NULL and (b) the
     * MTMV-OLAP partition (which has no originHiveKeys) renders the SAME name, keeping the sync-compare join
     * symmetric. The value must still resolve to a NULL literal so {@code col IS NULL} pruning is unaffected.
     */
    @Test
    public void testGenuineNullPartitionRendersAsPNull() throws AnalysisException {
        List<Type> types = Collections.singletonList(Type.VARCHAR);

        // Genuine NULL partition as a hive/paimon connector builds it: a NULL literal whose origin-hive key
        // preserves the canonical sentinel string.
        PartitionKey nullKey = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue(TablePartitionValues.HIVE_DEFAULT_PARTITION, true)),
                types, true);
        ListPartitionItem nullItem = new ListPartitionItem(Lists.newArrayList(nullKey));

        Assertions.assertEquals("p_NULL",
                MTMVPartitionUtil.generatePartitionName(nullItem.toPartitionKeyDesc(0)),
                "a genuine-null partition must render as p_NULL (master parity), not p_HIVEDEFAULTPARTITION");

        // The null partition's desc value must still resolve to a NULL literal so `col IS NULL` prunes to it.
        PartitionValue nullDescValue = nullItem.toPartitionKeyDesc(0).getInValues().get(0).get(0);
        Assertions.assertTrue(nullDescValue.isNullPartition(),
                "the null partition desc value must stay isNull");
        Assertions.assertTrue(nullDescValue.getValue(Type.VARCHAR).isNullLiteral(),
                "the null partition must still resolve to a NULL literal (IS NULL prune preserved)");
    }

    /**
     * An internal OLAP null partition (no originHiveKeys) renders as {@code p_NULL} — unchanged by, and after,
     * the FIX-3 revert. Kept as a symmetry anchor for {@link #testGenuineNullPartitionRendersAsPNull}: both the
     * connector-side genuine-null item and the MV-OLAP item must produce the SAME p_NULL name.
     */
    @Test
    public void testOlapNullPartitionRendersAsPNull() throws AnalysisException {
        List<Type> types = Collections.singletonList(Type.VARCHAR);
        PartitionKey olapNullKey = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue("NULL", true)), types, false);
        ListPartitionItem item = new ListPartitionItem(Lists.newArrayList(olapNullKey));
        Assertions.assertEquals("p_NULL",
                MTMVPartitionUtil.generatePartitionName(item.toPartitionKeyDesc(0)),
                "an OLAP null partition (no originHiveKeys) must render as p_NULL");
    }
}
