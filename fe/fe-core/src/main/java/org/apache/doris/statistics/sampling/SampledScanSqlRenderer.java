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

package org.apache.doris.statistics.sampling;

import org.apache.doris.analysis.FixedBlockSampleSpec;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.statistics.analysis.AnalyzeSkipException;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FixedBlockScanClause;
import org.apache.doris.statistics.sampling.TabletSampleSpec.KeyHashScanClause;
import org.apache.doris.statistics.sampling.TabletSampleSpec.LegacyTabletScanClause;
import org.apache.doris.statistics.sampling.TabletSampleSpec.SampledScanClause;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public final class SampledScanSqlRenderer {
    private SampledScanSqlRenderer() {
    }

    public static String render(SampledScanClause clause, int maxBytes) {
        String suffix = "";
        if (clause instanceof FixedBlockScanClause fixed) {
            suffix = renderFixedBlock(fixed.blocks());
        } else if (clause instanceof KeyHashScanClause keyHash && keyHash.partitionBlocks().isPresent()) {
            suffix = renderFixedBlock(keyHash.partitionBlocks().get());
        } else if (clause instanceof LegacyTabletScanClause legacy) {
            suffix = "TABLET(" + ids(legacy.tablets()) + ")";
        }
        return checked(suffix, maxBytes);
    }

    public static String renderKeyHashWhere(SampledScanClause clause, List<String> keyColumnNames) {
        if (!(clause instanceof KeyHashScanClause keyHash)
                || keyHash.hashCutoff() >= KeyHashScanClause.HASH_MODULUS
                || keyColumnNames.isEmpty()) {
            return "";
        }
        String args = keyColumnNames.stream().map(SqlUtils::getIdentSql).map(ident -> {
            String str = "SUBSTRING(CAST(" + ident + " AS STRING), 1, 1024)";
            return "IF(" + ident + " IS NULL, '0', CONCAT('1:', LENGTH(" + str + "), ':', " + str + "))";
        }).collect(Collectors.joining(", "));
        long mask = KeyHashScanClause.HASH_MODULUS - 1;
        return " WHERE (xxhash_64('" + keyHash.seed() + "', " + args + ") & " + mask + ") < "
                + keyHash.hashCutoff();
    }

    private static String renderFixedBlock(FixedBlockSampleSpec spec) {
        return "TABLESAMPLE (" + spec.mode + " unit_rows = " + spec.unitRows + ", "
                + spec.groups.stream().map(g -> "GROUP (tablet_rate = " + g.tabletRate + ", unit_rate = "
                        + g.unitRate + ", max_units = " + g.maxUnits + ", TABLETS (" + ids(g.tablets) + "))")
                        .collect(Collectors.joining(", "))
                + ") REPEATABLE " + spec.seed;
    }

    public static String checked(String sql, int maxBytes) {
        if (sql.getBytes(StandardCharsets.UTF_8).length > maxBytes) {
            throw new AnalyzeSkipException("Statistics SQL exceeds " + maxBytes + " UTF-8 bytes");
        }
        return sql;
    }

    // 19 digits per positive long, two separator bytes. The fixed fields allow 24 bytes per double.
    static int tabletCapacity(int bytes, int groups, FixedBlockSampleSpec.Mode mode) {
        int overhead = 80 + mode.name().length() - FixedBlockSampleSpec.Mode.FIXED_BLOCK.name().length();
        int payload = Math.max(0, bytes - overhead);
        int groupBytes = 160;
        int tabletBytes = 21;
        // Each nonempty group needs at least one tablet. A one-tablet draw can only emit one group.
        int tabletsInSeparateGroups = payload / (groupBytes + tabletBytes);
        if (tabletsInSeparateGroups < groups) {
            return tabletsInSeparateGroups;
        }
        return (payload - groups * groupBytes) / tabletBytes;
    }

    private static String ids(List<Long> ids) {
        return ids.stream().map(Object::toString).collect(Collectors.joining(", "));
    }
}
