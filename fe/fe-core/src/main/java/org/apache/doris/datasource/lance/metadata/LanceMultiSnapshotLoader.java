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
import org.apache.doris.datasource.lance.storage.LanceStorageProvider;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.Uris;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Opens every dataset a {@code uris} array names and pins one snapshot per dataset.
 *
 * <p>Each dataset is opened on its own and keeps the version, schema and fragments that one open
 * read. The result is ordered the way the caller wrote the array, so the first dataset is the
 * reference the rest are compared against.
 *
 * <p>Any failure fails the whole query, and a failure names the path and the operation.
 */
public final class LanceMultiSnapshotLoader {

    /** The maximum number of distinct datasets, after deduplication. */
    public static final int MAX_DATASETS = 500;

    /** The maximum number of fragments, summed over every dataset. */
    public static final long MAX_TOTAL_FRAGMENTS = 100_000L;

    /** Fields in one dataset's schema. */
    public static final int MAX_FIELDS_PER_SCHEMA = 1_000;

    private static final int MAX_CONCURRENT_OPENS = 4;
    // Pending opens allowed to wait in the executor queue.
    private static final int MAX_QUEUED_OPENS = 4096;
    private static final long DEADLINE_SECONDS = 60L;
    private static final long PER_OPEN_TIMEOUT_SECONDS = 20L;
    private static final String SCHEME_DELIMITER = "://";

    private static final ThreadPoolExecutor EXECUTOR = ThreadPoolManager.newDaemonFixedThreadPool(
            MAX_CONCURRENT_OPENS, MAX_QUEUED_OPENS, "lance-multi-dataset-open", true,
            new ThreadPoolExecutor.AbortPolicy());

    private LanceMultiSnapshotLoader() {
    }

    /** The datasets the plan keeps, together with the one schema it still has to read. */
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
        long deadlineSeconds = deadlineSeconds();
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(deadlineSeconds);

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
        List<Future<LanceTableMetadata>> futures = new ArrayList<>(end - start);
        for (int slot = start; slot < end; slot++) {
            if (remainingNanos(deadlineNanos) <= 0) {
                throw deadlineExceeded(deadlineSeconds, slot, uris.size());
            }
            final int index = slot;
            try {
                futures.add(EXECUTOR.submit(
                        () -> LanceMetadataLoader.loadLatestForTvf(uris.get(index), properties)));
            } catch (RejectedExecutionException e) {
                throw new AnalysisException("Lance dataset open capacity is exhausted: too many datasets "
                        + "are being opened across all queries at once; reduce concurrency or retry");
            }
        }

        List<LanceTableMetadata> opened = new ArrayList<>(futures.size());
        for (int offset = 0; offset < futures.size(); offset++) {
            String uri = uris.get(start + offset);
            long remainingNanos = remainingNanos(deadlineNanos);
            if (remainingNanos <= 0) {
                throw deadlineExceeded(deadlineSeconds, start + offset, uris.size());
            }
            long waitNanos = Math.min(TimeUnit.SECONDS.toNanos(PER_OPEN_TIMEOUT_SECONDS), remainingNanos);
            try {
                opened.add(futures.get(offset).get(waitNanos, TimeUnit.NANOSECONDS));
            } catch (TimeoutException e) {
                throw new AnalysisException("Opening Lance dataset '" + uri + "' timed out after "
                        + PER_OPEN_TIMEOUT_SECONDS + "s");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AnalysisException("Interrupted while opening the Lance datasets named by '"
                        + Uris.KEY + "'");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                throw new AnalysisException("Failed to open Lance dataset '" + uri + "': "
                        + (cause == null ? "unknown" : cause.getClass().getSimpleName())
                        + ". A Lance TVF reads every dataset it names, so one dataset that cannot be "
                        + "opened fails the whole query.");
            }
        }
        return opened;
    }

    /**
     * Rejects the URI shapes Lance will not read as a dataset location: a path below the authority,
     * no empty path segment, and no trailing slash.
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


    private static long deadlineSeconds() {
        ConnectContext context = ConnectContext.get();
        if (context == null) {
            return DEADLINE_SECONDS;
        }
        int queryTimeout = context.getQueryTimeoutS();
        return queryTimeout > 0 ? Math.min(queryTimeout, DEADLINE_SECONDS) : DEADLINE_SECONDS;
    }

    private static long remainingNanos(long deadlineNanos) {
        return deadlineNanos - System.nanoTime();
    }

    private static AnalysisException deadlineExceeded(long deadlineSeconds, int reached, int total) {
        return new AnalysisException("Opening the Lance datasets named by '" + Uris.KEY
                + "' exceeded the planning deadline of " + deadlineSeconds + "s after " + reached
                + " of " + total + " datasets");
    }
}
