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

package org.apache.doris.datasource.lance.metadata;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.lance.storage.LanceStorageProvider;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.Uris;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Opens every dataset named by the {@code uris} array and pins one snapshot per dataset.
 *
 * <p>Each dataset is opened on its own and keeps the version, schema and fragments that one open
 * read. The result preserves the caller's array order, so the first dataset is the reference the
 * others are compared against.
 *
 * <p>Any failure fails the whole query, and a failure names the path and the operation.
 */
public final class LanceMultiSnapshotLoader {

    private static final Logger LOG = LogManager.getLogger(LanceMultiSnapshotLoader.class);

    /** The maximum number of distinct datasets, after deduplication. */
    public static final int MAX_DATASETS = 500;

    /** The maximum number of fragments, summed over every dataset. */
    public static final long MAX_TOTAL_FRAGMENTS = 100_000L;

    /** Fields in one dataset's schema. */
    public static final int MAX_FIELDS_PER_SCHEMA = 1_000;

    // Datasets opened concurrently per query (and the batch size).
    private static final int MAX_CONCURRENT_OPENS = 4;
    // Pending opens allowed to wait in the executor queue.
    private static final int MAX_QUEUED_OPENS = 4096;
    // Floor for the load deadline of small queries; the real deadline scales with the dataset count.
    private static final long DEADLINE_SECONDS = 60L;
    // Bound for one dataset open, applied to both the Java future.get wait and the native
    // object_store client timeout.
    private static final long PER_OPEN_TIMEOUT_SECONDS = 60L;
    private static final String SCHEME_DELIMITER = "://";

    private static final ThreadPoolExecutor EXECUTOR = ThreadPoolManager.newDaemonFixedThreadPool(
            MAX_CONCURRENT_OPENS, MAX_QUEUED_OPENS, "lance-multi-dataset-open", true,
            new ThreadPoolExecutor.AbortPolicy());

    private LanceMultiSnapshotLoader() {
    }

    /** The planned result: one entry per dataset, plus the reference schema all datasets share. */
    public static final class Loaded {
        /** One entry per dataset, in the order written: URI, version and fragments. */
        public final List<LancePlannedDataset> datasets;
        /** The schema every dataset was compared against, and the one the plan's columns come from. */
        public final Schema referenceSchema;

        private Loaded(List<LancePlannedDataset> datasets, Schema referenceSchema) {
            this.datasets = datasets;
            this.referenceSchema = referenceSchema;
        }
    }

    /**
     * Opens every dataset {@code normalizedUris} names.
     *
     * @param normalizedUris the non-empty dataset URIs to open
     * @return the planned datasets and the schema the plan's columns come from
     * @throws AnalysisException if a URI is not well-formed for Lance, the datasets do not share one
     *         provider, a budget is exceeded, a dataset cannot be opened, or two schemas differ
     */
    public static Loaded load(List<String> normalizedUris,
            StorageProperties storageProperties) throws AnalysisException {
        List<String> uris = new ArrayList<>(new LinkedHashSet<>(normalizedUris));
        requireWellFormed(uris);
        requireOneProvider(uris);
        if (uris.size() > MAX_DATASETS) {
            throw new AnalysisException("'" + Uris.KEY + "' must not exceed " + MAX_DATASETS
                    + " distinct datasets, but got " + uris.size());
        }

        List<StorageProperties> properties = Collections.singletonList(storageProperties);
        long deadlineSeconds = deadlineSeconds(uris.size());
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(deadlineSeconds);
        LOG.debug("Lance multi-dataset open starting: queryId={}, {} distinct datasets, {} batches ({} "
                + "concurrently), planning deadline {}s", currentQueryId(), uris.size(),
                batchCount(uris.size()), MAX_CONCURRENT_OPENS, deadlineSeconds);

        List<LanceTableMetadata> datasets = new ArrayList<>(uris.size());
        for (int start = 0; start < uris.size(); start += MAX_CONCURRENT_OPENS) {
            List<LanceTableMetadata> opened = openBatch(uris, start,
                    Math.min(start + MAX_CONCURRENT_OPENS, uris.size()), properties,
                    deadlineNanos, deadlineSeconds);
            for (LanceTableMetadata metadata : opened) {
                requireReadable(metadata, datasets);
                datasets.add(metadata);
            }
        }
        return new Loaded(reduce(datasets), datasets.get(0).getSchema());
    }


    private static List<LancePlannedDataset> reduce(List<LanceTableMetadata> datasets) {
        List<LancePlannedDataset> planned = new ArrayList<>(datasets.size());
        for (LanceTableMetadata metadata : datasets) {
            planned.add(new LancePlannedDataset(
                    metadata.getDatasetUri(), metadata.getVersion(), metadata.getFragments()));
        }
        return planned;
    }

    /**
     * Checks one opened dataset against the budgets and against the reference schema.
     *
     * @param opened the datasets accepted so far
     * @throws AnalysisException if the version is invalid, the schema is missing or too large, the
     *         fragment budget is exceeded, or the schema differs from the reference
     */
    @VisibleForTesting
    static void requireReadable(LanceTableMetadata metadata, List<LanceTableMetadata> opened)
            throws AnalysisException {
        String uri = metadata.getDatasetUri();
        if (metadata.getVersion() <= 0) {
            throw new AnalysisException("Lance dataset '" + uri + "' returned an invalid version "
                    + metadata.getVersion());
        }
        if (metadata.getSchema() == null) {
            throw new AnalysisException("Lance dataset '" + uri + "' returned no schema");
        }
        int fields = metadata.getSchema().getFields().size();
        if (fields > MAX_FIELDS_PER_SCHEMA) {
            throw new AnalysisException("Lance dataset '" + uri + "' has " + fields
                    + " fields, which exceeds " + MAX_FIELDS_PER_SCHEMA);
        }
        long fragments = metadata.getFragments().size();
        for (LanceTableMetadata previous : opened) {
            fragments += previous.getFragments().size();
        }
        if (fragments > MAX_TOTAL_FRAGMENTS) {
            throw new AnalysisException("The Lance datasets named by '" + Uris.KEY + "' hold more than "
                    + MAX_TOTAL_FRAGMENTS + " fragments in total");
        }
        if (opened.isEmpty()) {
            return;
        }
        LanceTableMetadata reference = opened.get(0);
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                reference.getSchema(), metadata.getSchema());
        if (difference.isPresent()) {
            throw new AnalysisException("Lance dataset '" + uri + "' does not have the same schema as '"
                    + reference.getDatasetUri() + "': " + difference.get()
                    + ". A Lance TVF reads only datasets whose schemas are exactly equal.");
        }
    }

    private static List<LanceTableMetadata> openBatch(List<String> uris, int start, int end,
            List<StorageProperties> properties, long deadlineNanos, long deadlineSeconds)
            throws AnalysisException {
        int totalBatches = batchCount(uris.size());
        int batchIndex = start / MAX_CONCURRENT_OPENS + 1;
        LOG.debug("Lance dataset open progress: queryId={}, batch {}/{}, datasets [{}, {})",
                currentQueryId(), batchIndex, totalBatches, start, end);
        List<Future<LanceTableMetadata>> futures = new ArrayList<>(end - start);
        List<TrackedOpen> tasks = new ArrayList<>(end - start);
        boolean success = false;
        try {
            for (int slot = start; slot < end; slot++) {
                if (remainingNanos(deadlineNanos) <= 0) {
                    throw deadlineExceeded(deadlineSeconds, slot, uris.size());
                }
                final int index = slot;
                try {
                    TrackedOpen task = new TrackedOpen(uris.get(index), properties, PER_OPEN_TIMEOUT_SECONDS);
                    tasks.add(task);
                    futures.add(EXECUTOR.submit(task));
                } catch (RejectedExecutionException e) {
                    throw new AnalysisException("Lance dataset open capacity is exhausted (queryId="
                            + currentQueryId() + "): the open queue is full because too many datasets are "
                            + "being opened across all queries at once; reduce concurrency or retry");
                }
            }

            List<LanceTableMetadata> opened = new ArrayList<>(futures.size());
            long fullPerOpen = TimeUnit.SECONDS.toNanos(PER_OPEN_TIMEOUT_SECONDS);
            for (int offset = 0; offset < futures.size(); offset++) {
                String uri = uris.get(start + offset);
                long remainingNanos = remainingNanos(deadlineNanos);
                if (remainingNanos <= 0) {
                    throw deadlineExceeded(deadlineSeconds, start + offset, uris.size());
                }
                long waitNanos = Math.min(fullPerOpen, remainingNanos);
                TrackedOpen task = tasks.get(offset);
                try {
                    opened.add(futures.get(offset).get(waitNanos, TimeUnit.NANOSECONDS));
                } catch (TimeoutException e) {
                    if (waitNanos < fullPerOpen) {
                        throw deadlineExceeded(deadlineSeconds, start + offset, uris.size());
                    }
                    // The per-open budget is exhausted. The query thread and the worker race the same
                    // PENDING->(STARTED|CANCELLED) CAS (see TrackedOpen): claiming CANCELLED means the
                    // task never ran, otherwise the worker began Dataset.open() and this is a genuine
                    // open timeout.
                    if (task.phase.compareAndSet(TrackedOpen.Phase.PENDING, TrackedOpen.Phase.CANCELLED)) {
                        throw new AnalysisException("Opening Lance dataset '" + uri + "' (queryId="
                                + currentQueryId() + ") did not start within " + PER_OPEN_TIMEOUT_SECONDS
                                + "s: the shared open pool is saturated by concurrent dataset opens across "
                                + "all queries, so this query's tasks never got a thread. Reduce concurrency "
                                + "or retry.");
                    }
                    throw new AnalysisException("Opening Lance dataset '" + uri + "' (queryId="
                            + currentQueryId() + ") timed out after " + PER_OPEN_TIMEOUT_SECONDS
                            + "s: Dataset.open() did not finish (slow or unresponsive storage).");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AnalysisException("Interrupted while opening the Lance datasets named by '"
                            + Uris.KEY + "'");
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    String causeMessage = cause == null ? null : cause.getMessage();
                    throw new AnalysisException("Failed to open Lance dataset '" + uri + "': "
                            + (cause == null ? "unknown" : cause.getClass().getSimpleName())
                            + (causeMessage == null || causeMessage.isEmpty() ? "" : ": " + causeMessage));
                }
            }
            success = true;
            return opened;
        } finally {
            // A failed batch must not leave its futures running in the shared pool: an abandoned open
            // holds a worker thread and may delay unrelated queries once storage recovers. Cancel every
            // pending future to reclaim the slot. cancel(true) interrupts the worker; a native open
            // already past its Java entry keeps running only until its own per-open client timeout
            // (PER_OPEN_TIMEOUT_SECONDS) bounds how long it can hold the thread.
            if (!success) {
                for (Future<LanceTableMetadata> future : futures) {
                    future.cancel(true);
                }
            }
        }
    }

    /**
     * Rejects URIs that are not valid Lance dataset locations: each must be scheme://authority/path,
     * with no empty path segment ("//") and no trailing slash.
     */
    private static void requireWellFormed(List<String> uris) throws AnalysisException {
        for (int index = 0; index < uris.size(); index++) {
            String uri = uris.get(index);
            int schemeEnd = uri.indexOf(SCHEME_DELIMITER);
            if (schemeEnd <= 0) {
                throw new AnalysisException("'" + Uris.KEY + "' element at index " + index + " ('" + uri
                        + "') must name a storage location as scheme://authority/path");
            }
            String rest = uri.substring(schemeEnd + SCHEME_DELIMITER.length());
            int authorityEnd = rest.indexOf('/');
            if (authorityEnd <= 0) {
                throw new AnalysisException("'" + Uris.KEY + "' element at index " + index + " ('" + uri
                        + "') must name an authority and a path, as scheme://authority/path");
            }
            String path = rest.substring(authorityEnd);
            if (path.contains("//")) {
                throw new AnalysisException("'" + Uris.KEY + "' element at index " + index + " ('" + uri
                        + "') must not contain an empty path segment: '//' is a valid object key "
                        + "character and is never folded into '/'");
            }
            if (uri.endsWith("/")) {
                throw new AnalysisException("'" + Uris.KEY + "' element at index " + index + " ('" + uri
                        + "') must not end with '/': remove the trailing slash");
            }
        }
    }

    /** Requires every URI to resolve to the same storage provider. */
    private static void requireOneProvider(List<String> uris) throws AnalysisException {
        Class<?> provider = null;
        for (String uri : uris) {
            Class<?> current = LanceStorageProvider.forDataset(uri).getClass();
            if (provider == null) {
                provider = current;
            } else if (provider != current) {
                throw new AnalysisException("'" + Uris.KEY + "' must name datasets on one storage "
                        + "provider: '" + uris.get(0) + "' and '" + uri + "' resolve to different ones");
            }
        }
    }


    private static long deadlineSeconds(int numDatasets) {
        // Size the load deadline to the work: every dataset may open at the per-open budget,
        // MAX_CONCURRENT_OPENS at a time, plus a margin. A fixed cap sized for small queries would
        // kill a healthy-but-slow query that merely has many datasets, so the deadline grows with
        // the dataset count instead of being held at a constant.
        long perBatch = batchCount(numDatasets) * PER_OPEN_TIMEOUT_SECONDS;
        long workSized = Math.max(DEADLINE_SECONDS, perBatch + DEADLINE_SECONDS);
        ConnectContext context = ConnectContext.get();
        int queryTimeout = context == null ? 0 : context.getQueryTimeoutS();
        // Respect an explicit session query timeout as a floor, never as a ceiling that re-introduces
        // the fixed cap.
        return queryTimeout > 0 ? Math.max(queryTimeout, workSized) : workSized;
    }

    private static long remainingNanos(long deadlineNanos) {
        return deadlineNanos - System.nanoTime();
    }

    /** Formats the current session query id for log lines, or "n/a" when no context is set. */
    private static String currentQueryId() {
        ConnectContext context = ConnectContext.get();
        return context == null ? "n/a" : DebugUtil.printId(context.queryId());
    }

    /** Number of 4-at-a-time batches needed to open {@code datasetCount} datasets. */
    private static int batchCount(int datasetCount) {
        return (datasetCount + MAX_CONCURRENT_OPENS - 1) / MAX_CONCURRENT_OPENS;
    }

    private static AnalysisException deadlineExceeded(long deadlineSeconds, int reached, int total) {
        return new AnalysisException("Opening the Lance datasets named by '" + Uris.KEY
                + "' (queryId=" + currentQueryId() + ") exceeded the planning deadline of " + deadlineSeconds
                + "s after " + reached + " of " + total + " datasets");
    }

    /**
     * Wraps one dataset open so a timeout can tell a task that never got a worker thread from one that
     * started but hung in Dataset.open().
     *
     * <p>The state is a single three-value machine (PENDING/STARTED/CANCELLED). The worker claims
     * STARTED and the querying thread claims CANCELLED, both through the same PENDING->(STARTED|
     * CANCELLED) CAS, so exactly one claim wins:
     * <ul>
     *   <li>worker wins -> it runs Dataset.open(); the query sees STARTED and reports a real open
     *       timeout, then finally cancels this future to abort the in-flight open;</li>
     *   <li>query wins -> the worker's claim fails and it skips Dataset.open(), so a "did not start"
     *       verdict is accurate and no native open is left running.</li>
     * </ul>
     */
    private static final class TrackedOpen implements Callable<LanceTableMetadata> {
        private final String uri;
        private final List<StorageProperties> properties;

        private enum Phase { PENDING, STARTED, CANCELLED }

        private final long openTimeoutSeconds;
        private final AtomicReference<Phase> phase = new AtomicReference<>(Phase.PENDING);

        TrackedOpen(String uri, List<StorageProperties> properties, long openTimeoutSeconds) {
            this.uri = uri;
            this.properties = properties;
            this.openTimeoutSeconds = openTimeoutSeconds;
        }

        @Override
        public LanceTableMetadata call() throws Exception {
            // Run the open only if we claim STARTED. If the query thread already claimed CANCELLED
            // for a timeout, this task must not start Dataset.open() (see the class note).
            if (!phase.compareAndSet(Phase.PENDING, Phase.STARTED)) {
                throw new CancellationException(
                        "Lance dataset open was cancelled before it started");
            }
            return LanceMetadataLoader.loadLatestForTvf(uri, properties, openTimeoutSeconds);
        }
    }
}
