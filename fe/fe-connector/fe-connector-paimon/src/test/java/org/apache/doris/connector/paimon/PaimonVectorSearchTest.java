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

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.BucketVectorSearchSplit;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PaimonVectorSearchTest {
    private static final ConnectorColumnRef ID = new ConnectorColumnRef("id", ConnectorType.of("BIGINT"));
    private static final ConnectorLiteral VALUE = ConnectorLiteral.ofInt(7);

    @Test
    public void boundedSearchRequiresEveryConjunctToBeSupported() {
        ConnectorComparison supported = new ConnectorComparison(ConnectorComparison.Operator.GT, ID, VALUE);
        ConnectorComparison unsupported = new ConnectorComparison(ConnectorComparison.Operator.GT,
                new ConnectorColumnRef("price", ConnectorType.of("DOUBLE")), VALUE);
        Assertions.assertTrue(PaimonVectorSearch.supportsFilter(supported));
        Assertions.assertFalse(PaimonVectorSearch.supportsFilter(new ConnectorAnd(
                Arrays.asList(supported, unsupported))));
        Assertions.assertFalse(PaimonVectorSearch.supportsFilter(new ConnectorComparison(
                ConnectorComparison.Operator.EQ, ID, new ConnectorLiteral(ConnectorType.of("BIGINT"), null))));
        Assertions.assertTrue(PaimonVectorSearch.supportsFilter(new ConnectorNot(new ConnectorIsNull(ID, false))));
    }

    @Test
    public void inListMustFitNativeEncodingAndContainNoNulls() {
        Assertions.assertFalse(PaimonVectorSearch.supportsFilter(new ConnectorIn(ID,
                Collections.emptyList(), false)));
        Assertions.assertTrue(PaimonVectorSearch.supportsFilter(new ConnectorIn(ID,
                Collections.nCopies(0xfffe, VALUE), false)));
        Assertions.assertFalse(PaimonVectorSearch.supportsFilter(new ConnectorIn(ID,
                Collections.nCopies(0xffff, VALUE), false)));
        Assertions.assertFalse(PaimonVectorSearch.supportsFilter(new ConnectorIn(ID,
                Arrays.asList(VALUE, new ConnectorLiteral(ConnectorType.of("BIGINT"), null)), false)));
    }

    @Test
    public void sqlRequiresCurrentCoverageForEveryActiveSource() {
        DataFileMeta first = file("first.parquet").upgrade(1).assignFileSource(FileSource.COMPACT);
        DataFileMeta second = file("second.parquet").upgrade(2).assignFileSource(FileSource.COMPACT);
        IndexFileMeta firstIndex = index("first.index", 1, first);
        IndexFileMeta secondIndex = index("second.index", 2, second);
        Assertions.assertTrue(covered(bucket(Arrays.asList(first, second), firstIndex, secondIndex)));
        Assertions.assertFalse(covered(bucket(Arrays.asList(first, second), firstIndex)));
        Assertions.assertFalse(covered(bucket(Collections.singletonList(first))));
        Assertions.assertFalse(covered(bucket(Collections.singletonList(first),
                index("stale.index", 1, file("replaced.parquet")))));
        Assertions.assertFalse(covered(bucket(Collections.singletonList(first), firstIndex,
                index("duplicate.index", 1, first))));
        // A same-level segment must cover the complete current source set, not just one file.
        DataFileMeta sameLevel = file("same-level.parquet").upgrade(1).assignFileSource(FileSource.COMPACT);
        Assertions.assertFalse(covered(bucket(Arrays.asList(first, sameLevel), firstIndex)));
        Assertions.assertTrue(covered(bucket(Arrays.asList(first, sameLevel),
                index("combined.index", 1, first, sameLevel))));
        Assertions.assertFalse(covered(bucket(Collections.singletonList(first), firstIndex),
                bucket(Collections.singletonList(second))));
        Assertions.assertTrue(PaimonVectorSearch.hasSqlCoverage(2, "ivf_rq", Collections.emptyList()));
    }

    @Test
    public void sqlFallsBackForSourcesIgnoredEvenByFullVectorSearch() {
        DataFileMeta append = file("append.parquet");
        Assertions.assertFalse(covered(bucket(Collections.singletonList(append))));
        DataFileMeta upgradedAppend = append.upgrade(1);
        Assertions.assertFalse(covered(bucket(Collections.singletonList(upgradedAppend),
                index("upgraded.index", 1, upgradedAppend))));
        DataFileMeta levelZero = append.assignFileSource(FileSource.COMPACT);
        Assertions.assertFalse(covered(bucket(Collections.singletonList(levelZero))));
    }

    private static boolean covered(BucketVectorSearchSplit... buckets) {
        return PaimonVectorSearch.hasSqlCoverage(2, "ivf_rq", Arrays.asList(buckets));
    }

    private static BucketVectorSearchSplit bucket(List<DataFileMeta> files, IndexFileMeta... indexes) {
        DataSplit split = DataSplit.builder().withSnapshot(1).withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0).withBucketPath("bucket-0").withDataFiles(files).rawConvertible(false).build();
        return new BucketVectorSearchSplit(split, Arrays.asList(indexes));
    }

    private static DataFileMeta file(String name) {
        return DataFileMeta.forAppend(name, 1024, 1, SimpleStats.EMPTY_STATS,
                1, 1, 0, Collections.emptyList(), null, FileSource.APPEND,
                Collections.emptyList(), null, null, Collections.emptyList());
    }

    private static IndexFileMeta index(String name, int level, DataFileMeta... files) {
        PrimaryKeyIndexSourceMeta source = new PrimaryKeyIndexSourceMeta(level, Arrays.stream(files)
                .map(file -> new PrimaryKeyIndexSourceFile(file.fileName(), file.rowCount()))
                .collect(Collectors.toList()));
        return new IndexFileMeta("ivf_rq", name, 1024, files.length, null, null,
                new GlobalIndexMeta(0, files.length - 1, 2, null, null, source.serialize()));
    }

}
