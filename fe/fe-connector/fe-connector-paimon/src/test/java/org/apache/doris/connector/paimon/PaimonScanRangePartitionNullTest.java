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

package org.apache.doris.connector.paimon;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * P5-fix FIX-PARTITION-NULL-SENTINEL (review §5 sentinel data-edge) — pins that
 * {@link PaimonScanRange#populateRangeParams} derives a partition column's {@code isNull} from the
 * Java null ONLY (legacy {@code PaimonScanNode.setScanParams:323-326} parity), and does NOT apply
 * the Hive-directory sentinel coercion of {@code ConnectorPartitionValues.normalize}.
 *
 * <p>Paimon partition values are typed: the per-type serializer returns a Java null for a genuine
 * null, never a directory sentinel. So a literal {@code "\N"} or {@code "__HIVE_DEFAULT_PARTITION__"}
 * partition value is REAL DATA and must be kept, not coerced to SQL NULL (which is correct for
 * hudi's path-encoded partitions, but wrong here).
 */
public class PaimonScanRangePartitionNullTest {

    private static TFileRangeDesc populate(Map<String, String> partitionValues) {
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("jni")
                .paimonSplit("dummy-split")           // JNI path; the partition block runs regardless
                .partitionValues(partitionValues)
                .build();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(new TTableFormatFileDesc(), rangeDesc);
        return rangeDesc;
    }

    @Test
    public void onlyJavaNullIsTreatedAsNullPartition() {
        Map<String, String> pv = new LinkedHashMap<>();
        pv.put("ordinary", "cn");
        pv.put("genuine_null", null);
        pv.put("literal_slash_n", "\\N");                       // 2 chars: backslash, N
        pv.put("literal_hive_default", "__HIVE_DEFAULT_PARTITION__");

        TFileRangeDesc desc = populate(pv);
        List<String> keys = desc.getColumnsFromPathKeys();
        List<String> values = desc.getColumnsFromPath();
        List<Boolean> isNull = desc.getColumnsFromPathIsNull();

        Assertions.assertEquals(
                Arrays.asList("ordinary", "genuine_null", "literal_slash_n", "literal_hive_default"), keys);

        // WHY: paimon partition values are typed — a genuine null is a Java null, never a Hive
        // directory sentinel. isNull must derive from the Java null ONLY (legacy
        // PaimonScanNode:323-326). A literal "\N" / "__HIVE_DEFAULT_PARTITION__" is real data and
        // must be kept verbatim, not coerced to NULL. MUTATION: routing through
        // ConnectorPartitionValues.normalize (Hive-aware coercion) flips both literal rows to
        // isNull=true (and the genuine null renders "\N" not "") -> red.

        // ordinary value -> kept, not null
        Assertions.assertEquals("cn", values.get(0));
        Assertions.assertFalse(isNull.get(0));

        // genuine Java-null -> null, rendered "" (legacy-exact; BE ignores the string when isNull)
        Assertions.assertTrue(isNull.get(1));
        Assertions.assertEquals("", values.get(1));

        // literal "\N" -> NOT null, literal kept (the fix; paimon does not reserve "\N")
        Assertions.assertFalse(isNull.get(2),
                "literal \\N is real data, not a null marker, on the paimon scan path");
        Assertions.assertEquals("\\N", values.get(2));

        // literal "__HIVE_DEFAULT_PARTITION__" -> NOT null on the scan path (legacy keeps the literal)
        Assertions.assertFalse(isNull.get(3),
                "literal __HIVE_DEFAULT_PARTITION__ kept verbatim on the paimon scan path");
        Assertions.assertEquals("__HIVE_DEFAULT_PARTITION__", values.get(3));
    }
}
