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

import org.apache.doris.thrift.TIcebergDeleteFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit-pins {@link IcebergRewritableDeleteStash}, the scan→write seam carrier for a row-level DML's
 * non-equality delete supply. WHY each invariant matters: the BE OR-merges the supplied old deletes into the
 * new deletion vector, so a DROPPED or WRONG supply silently resurrects previously-deleted rows; an over-stash
 * (a plain SELECT that never writes) must NOT grow unboundedly. The clock is injected so the TTL sweep is
 * deterministic without sleeping.
 */
public class IcebergRewritableDeleteStashTest {

    private static TIcebergDeleteFileDesc desc(String path, int content) {
        TIcebergDeleteFileDesc d = new TIcebergDeleteFileDesc();
        d.setPath(path);
        d.setContent(content);
        return d;
    }

    private static List<TIcebergDeleteFileDesc> descs(String path, int content) {
        return Collections.singletonList(desc(path, content));
    }

    /** A stash whose clock the test drives by hand (nanos), so TTL expiry is exact. */
    private static IcebergRewritableDeleteStash stashWithClock(long ttlSeconds, AtomicLong nanos) {
        return new IcebergRewritableDeleteStash(ttlSeconds, nanos::get);
    }

    @Test
    public void retrieveReturnsWhatWasAccumulatedKeyedByRawDataFilePath() {
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate("q1", "s3://b/db/t/f1.parquet", descs("s3://b/db/t/dv1.puffin", 3));

        Map<String, List<TIcebergDeleteFileDesc>> sets = stash.retrieveAndRemove("q1");
        Assertions.assertNotNull(sets);
        Assertions.assertEquals(1, sets.size());
        // The KEY is the raw data-file path (the string the BE matches against). A mutation keying on anything
        // else loses the lookup -> resurrection.
        Assertions.assertTrue(sets.containsKey("s3://b/db/t/f1.parquet"));
        Assertions.assertEquals("s3://b/db/t/dv1.puffin", sets.get("s3://b/db/t/f1.parquet").get(0).getPath());
    }

    @Test
    public void retrieveAndRemoveEvictsSoASecondRetrieveIsEmpty() {
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate("q1", "s3://b/db/t/f1.parquet", descs("dv", 3));

        Assertions.assertNotNull(stash.retrieveAndRemove("q1"));
        // The retrieve is the primary eviction. MUTATION: making retrieve NOT remove -> this second read is
        // non-null and the entry leaks across statements.
        Assertions.assertNull(stash.retrieveAndRemove("q1"));
        Assertions.assertEquals(0, stash.size());
    }

    @Test
    public void retrieveUnknownQueryIdReturnsNull() {
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        Assertions.assertNull(stash.retrieveAndRemove("never-stashed"));
    }

    @Test
    public void accumulateMergesDistinctDataFilesUnderOneQuery() {
        // A MERGE scans two tables under one queryId; their data-file paths are globally distinct, so both must
        // survive in one entry. MUTATION: overwriting (put under the queryId) instead of merging per path drops
        // the first table's supply -> resurrection on that table.
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate("q1", "s3://b/db/target/f.parquet", descs("dv-target", 3));
        stash.accumulate("q1", "s3://b/db/source/g.parquet", descs("dv-source", 3));

        Map<String, List<TIcebergDeleteFileDesc>> sets = stash.retrieveAndRemove("q1");
        Assertions.assertEquals(2, sets.size());
        Assertions.assertTrue(sets.containsKey("s3://b/db/target/f.parquet"));
        Assertions.assertTrue(sets.containsKey("s3://b/db/source/g.parquet"));
    }

    @Test
    public void accumulateSamePathIsIdempotentForSplitRanges() {
        // A large data file split into several ranges carries an identical delete list per split; re-putting the
        // same key must not duplicate. Last-wins (same value) keeps exactly one entry.
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        List<TIcebergDeleteFileDesc> d = descs("dv", 3);
        stash.accumulate("q1", "s3://b/db/t/f.parquet", d);
        stash.accumulate("q1", "s3://b/db/t/f.parquet", d);

        Map<String, List<TIcebergDeleteFileDesc>> sets = stash.retrieveAndRemove("q1");
        Assertions.assertEquals(1, sets.size());
        Assertions.assertEquals(1, sets.get("s3://b/db/t/f.parquet").size());
    }

    @Test
    public void twoQueryIdsAreIsolated() {
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate("q1", "s3://b/db/t/f1.parquet", descs("dv1", 3));
        stash.accumulate("q2", "s3://b/db/t/f2.parquet", descs("dv2", 3));

        Assertions.assertTrue(stash.retrieveAndRemove("q1").containsKey("s3://b/db/t/f1.parquet"));
        // q1's retrieve must not touch q2.
        Assertions.assertTrue(stash.retrieveAndRemove("q2").containsKey("s3://b/db/t/f2.parquet"));
    }

    @Test
    public void blankQueryIdIsNeverStashed() {
        // A null/absent ConnectContext coerces queryId to "" — two concurrent such statements would collide on
        // "" and read each other's (or stale) supply. MUTATION: dropping the blank guard lets the "" key store a
        // map that a later null-ctx statement reads -> stale supply / resurrection.
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate("", "s3://b/db/t/f.parquet", descs("dv", 3));
        stash.accumulate(null, "s3://b/db/t/f.parquet", descs("dv", 3));

        Assertions.assertEquals(0, stash.size());
        Assertions.assertNull(stash.retrieveAndRemove(""));
        Assertions.assertNull(stash.retrieveAndRemove(null));
    }

    @Test
    public void emptyOrNullSupplyIsNotStashed() {
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate("q1", "s3://b/db/t/f.parquet", Collections.emptyList());
        stash.accumulate("q1", "s3://b/db/t/f.parquet", null);

        Assertions.assertEquals(0, stash.size());
    }

    @Test
    public void ttlSweepEvictsLeakedEntryOnceExpiredButNotBefore() {
        AtomicLong nanos = new AtomicLong(0L);
        IcebergRewritableDeleteStash stash = stashWithClock(300L, nanos);

        // q1 scanned but never writes (a leaked plain-SELECT entry).
        stash.accumulate("q1", "s3://b/db/t/f1.parquet", descs("dv1", 3));
        Assertions.assertEquals(1, stash.size());

        // 299s later: a new query arrives; q1 is still within TTL so it is NOT swept (it could still be a live
        // supply whose write is pending). MUTATION: sweeping a live entry here would resurrect its rows.
        nanos.set(TimeUnit.SECONDS.toNanos(299L));
        stash.accumulate("q2", "s3://b/db/t/f2.parquet", descs("dv2", 3));
        Assertions.assertEquals(2, stash.size());

        // Past the TTL: the next new-query accumulate sweeps the now-expired q1 (but keeps the fresh q3).
        nanos.set(TimeUnit.SECONDS.toNanos(301L));
        stash.accumulate("q3", "s3://b/db/t/f3.parquet", descs("dv3", 3));
        Assertions.assertNull(stash.retrieveAndRemove("q1"));
        Assertions.assertNotNull(stash.retrieveAndRemove("q2"));
        Assertions.assertNotNull(stash.retrieveAndRemove("q3"));
    }

    @Test
    public void accumulateRefreshesTouchStampSoAnActiveQueryIsNotSwept() {
        AtomicLong nanos = new AtomicLong(0L);
        IcebergRewritableDeleteStash stash = stashWithClock(300L, nanos);

        stash.accumulate("q1", "s3://b/db/t/f1.parquet", descs("dv1", 3));
        // q1 keeps adding ranges over time (a long scan). At 200s a fresh range refreshes its stamp.
        nanos.set(TimeUnit.SECONDS.toNanos(200L));
        stash.accumulate("q1", "s3://b/db/t/f2.parquet", descs("dv2", 3));
        // 400s absolute = 200s since the last touch: still within TTL, must survive a sweep triggered by q9.
        nanos.set(TimeUnit.SECONDS.toNanos(400L));
        stash.accumulate("q9", "s3://b/db/t/g.parquet", descs("dvg", 3));

        Map<String, List<TIcebergDeleteFileDesc>> sets = stash.retrieveAndRemove("q1");
        Assertions.assertNotNull(sets);
        Assertions.assertEquals(2, sets.size());
    }

    @Test
    public void multipleDescsForOneDataFileAreAllCarried() {
        // A v2->v3 upgraded data file can have BOTH an old position-delete file and an old DV; both must reach
        // the BE for the union, or the rows covered by the missing one resurrect.
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate("q1", "s3://b/db/t/f.parquet",
                Arrays.asList(desc("pos.parquet", 1), desc("dv.puffin", 3)));

        Map<String, List<TIcebergDeleteFileDesc>> sets = stash.retrieveAndRemove("q1");
        Assertions.assertEquals(2, sets.get("s3://b/db/t/f.parquet").size());
    }
}
