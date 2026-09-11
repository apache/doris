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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * The delete-manifest partition prune inside the manifest-cache planning path must project the ROW filter into
 * partition space before {@link ManifestEvaluator#forPartitionFilter} (exactly like the data-manifest side).
 * Binding the raw row filter works by accident on identity-only specs (the partition field keeps the source
 * column name) but throws on any spec with a transform — e.g. {@code month(ts)} stores the partition field as
 * {@code ts_month: int} — whenever the filter references the transform's source column. Before the fix that
 * exception aborted the whole cached plan into the SDK fallback for every filtered query on every table with
 * delete manifests, logging a WARN + stack per query.
 *
 * <p>Uses a real {@link InMemoryCatalog} v2 table (identity + month spec, one data file, one position-delete
 * file) so the evaluator runs against a genuine delete manifest with real partition summaries.
 */
public class IcebergScanPlanProviderDeleteManifestPruneTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "flag", Types.BooleanType.get()),
            Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone()));

    // month(ts) ordinals are months since 1970-01: 2026-07 -> (2026-1970)*12 + 6 = 678.
    private static final String JULY_2026_PATH = "flag=true/ts_month=678";

    private static long micros(String instant) {
        return Instant.parse(instant).toEpochMilli() * 1000L;
    }

    /** v2 table partitioned by (identity(flag), month(ts)) with one data + one position-delete file in 2026-07. */
    private static Table tableWithJuly2026Deletes() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("flag").month("ts").build();
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), SCHEMA, spec,
                Collections.singletonMap("format-version", "2"));
        table.newAppend()
                .appendFile(DataFiles.builder(table.spec())
                        .withPath("/data/f1.parquet").withFileSizeInBytes(100).withRecordCount(10)
                        .withPartitionPath(JULY_2026_PATH).withFormat(FileFormat.PARQUET).build())
                .commit();
        table.newRowDelta()
                .addDeletes(FileMetadata.deleteFileBuilder(table.spec())
                        .ofPositionDeletes()
                        .withPath("/data/pd1.parquet").withFileSizeInBytes(50).withRecordCount(1)
                        .withPartitionPath(JULY_2026_PATH).withFormat(FileFormat.PARQUET).build())
                .commit();
        return table;
    }

    private static ManifestFile onlyDeleteManifest(Table table) {
        List<ManifestFile> deletes = table.currentSnapshot().deleteManifests(table.io());
        Assertions.assertEquals(1, deletes.size(), "the single row delta produced one delete manifest");
        return deletes.get(0);
    }

    @Test
    public void rawRowFilterFailsToBindOnTransformSpec() {
        // The bug this class guards against: binding the un-projected row filter against the partition struct
        // throws, because 'ts' only appears through the month transform ('ts_month'). If this ever STOPS
        // throwing, the projection in deleteManifestEvaluator becomes optional — revisit, don't delete it.
        Table table = tableWithJuly2026Deletes();
        Expression rowFilter = Expressions.greaterThan("ts", micros("2026-07-15T00:00:00Z"));
        Assertions.assertThrows(ValidationException.class,
                () -> ManifestEvaluator.forPartitionFilter(rowFilter, table.spec(), true)
                        .eval(onlyDeleteManifest(table)));
    }

    @Test
    public void projectedEvaluatorKeepsOverlappingMonthAndPrunesFarMonth() {
        Table table = tableWithJuly2026Deletes();
        ManifestFile deleteManifest = onlyDeleteManifest(table);

        // WHY: a filter inside 2026-07 must keep the manifest (its deletes apply to matching data).
        // MUTATION: dropping the projection -> ValidationException -> red.
        Assertions.assertTrue(IcebergScanPlanProvider.deleteManifestEvaluator(
                        table.spec(), Expressions.greaterThan("ts", micros("2026-07-15T00:00:00Z")), true)
                .eval(deleteManifest));

        // WHY: a filter entirely after 2026-07 (2027-05 -> ordinal 688) must PRUNE the manifest — the
        // projection maps ts > X to ts_month >= 688, and the summaries hold only 678. MUTATION: projecting
        // to alwaysTrue() (no prune) -> true -> red.
        Assertions.assertFalse(IcebergScanPlanProvider.deleteManifestEvaluator(
                        table.spec(), Expressions.greaterThan("ts", micros("2027-05-01T00:00:00Z")), true)
                .eval(deleteManifest));
    }

    @Test
    public void identityPredicateStillPrunes() {
        // Unchanged behavior on the identity leg of the same spec: flag=true keeps, flag=false prunes.
        Table table = tableWithJuly2026Deletes();
        ManifestFile deleteManifest = onlyDeleteManifest(table);

        Assertions.assertTrue(IcebergScanPlanProvider.deleteManifestEvaluator(
                table.spec(), Expressions.equal("flag", true), true).eval(deleteManifest));
        Assertions.assertFalse(IcebergScanPlanProvider.deleteManifestEvaluator(
                table.spec(), Expressions.equal("flag", false), true).eval(deleteManifest));
    }

    @Test
    public void predicateOnNonPartitionColumnProjectsToKeep() {
        // A residual-only filter (no partition column at all) must not prune delete manifests: the inclusive
        // projection yields alwaysTrue(). Guard against an over-eager projection that prunes everything.
        Schema wider = new Schema(
                Types.NestedField.required(1, "flag", Types.BooleanType.get()),
                Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(3, "note", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(wider).identity("flag").month("ts").build();
        Table table = tableWithJuly2026Deletes();
        Assertions.assertTrue(IcebergScanPlanProvider.deleteManifestEvaluator(
                        spec, Expressions.equal("note", "x"), true)
                .eval(onlyDeleteManifest(table)));
    }
}
