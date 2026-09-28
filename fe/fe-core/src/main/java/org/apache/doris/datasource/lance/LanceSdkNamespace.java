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

package org.apache.doris.datasource.lance;

import org.apache.doris.datasource.lance.storage.LanceStorageOptions;

import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DescribeTableVersionRequest;
import org.lance.namespace.model.DescribeTableVersionResponse;
import org.lance.namespace.model.ListTableVersionsRequest;
import org.lance.namespace.model.ListTableVersionsResponse;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * The namespace the Lance SDK is handed to open one namespace-managed dataset: the catalog's
 * namespace, with two things the SDK gets wrong on its own.
 *
 * <p>The SDK opens with the options it is handed plus whatever its own describe vends, spelled as
 * the namespace spells them. Lance then adds the process environment for any option whose
 * canonical key is missing, so a vended {@code endpoint} next to an {@code AWS_ENDPOINT} in the
 * FE environment leaves the FE on whichever endpoint object_store folds last, while the BE, handed
 * the canonical {@code aws_endpoint}, keeps the vended one. {@link #describeTable} therefore
 * returns the vended options in the vocabulary Doris uses for everything else.
 *
 * <p>The SDK also caches the object store of a namespace-opened dataset in the catalog Session by
 * the namespace's id and the table id alone, ignoring the options: a read overlapping another one
 * that still holds a store for the same table reuses that store, whatever endpoint it was built
 * for (lance-io {@code StorageOptionsAccessor::accessor_id}, {@code ObjectStoreRegistry::get_store}).
 * {@link #namespaceId} therefore also identifies the options the store is built with, less the
 * credentials the SDK re-reads through its credential provider anyway.
 *
 * <p>A namespace Lance does not implement natively is called back through JNI, which reports an
 * exception thrown by the callback only as "Java exception was thrown". The last one is kept for
 * {@link #unwrapCallbackFailure}, so a missing version or branch is still reported as such.
 *
 * <p>One instance is created per open. The datasets checked out from it resolve versions through
 * it, and the store the open builds refreshes credentials through it, also for later reads that
 * share that store.
 */
final class LanceSdkNamespace implements LanceNamespace {
    private static final Logger LOG = LogManager.getLogger(LanceSdkNamespace.class);

    /** What jni-rs reports for a Java exception a callback threw. */
    private static final String CALLBACK_FAILURE = "Java exception was thrown";

    /**
     * The options the SDK serves to an object store through its credential provider, refreshing
     * them from the namespace, rather than fixing them when the store is built: every spelling
     * lance-io's {@code DynamicCredentials} conversions read for AWS, Azure and GCS, and the
     * refresh deadline. OSS credentials are not refreshed, but are left out as well: a store is
     * shared across credentials as it was before, and a namespace that vends new credentials on
     * every describe would otherwise leave one registry entry behind per read.
     */
    private static final Set<String> CREDENTIAL_OPTIONS = ImmutableSet.of(
            "aws_access_key_id", "access_key_id", "aws_secret_access_key", "secret_access_key",
            "aws_session_token", "aws_token", "aws_security_token", "session_token", "token",
            "azure_storage_sas_token", "azure_storage_sas_key", "sas_token", "sas_key",
            "azure_storage_token", "bearer_token", "azure_storage_account_key", "azure_storage_access_key",
            "azure_storage_master_key", "access_key", "master_key", "account_key",
            "google_storage_token",
            "oss_access_key_id", "oss_secret_access_key", "oss_security_token",
            "expires_at_millis");

    private final LanceNamespace catalogNamespace;
    private final Map<String, String> sdkStorageOptions;
    private final AtomicReference<RuntimeException> callbackFailure = new AtomicReference<>();
    /** The thread that opens the dataset and issues the SDK's describe; every other call is a JNI callback. */
    private final Thread openingThread = Thread.currentThread();
    /** Set by the SDK's own describe while it opens the dataset. */
    private volatile String storeIdentity;

    /**
     * @param sdkStorageOptions the options the SDK is handed in its read options, which it opens
     *     with under what its describe vends
     */
    LanceSdkNamespace(LanceNamespace catalogNamespace, Map<String, String> sdkStorageOptions) {
        this.catalogNamespace = catalogNamespace;
        this.sdkStorageOptions = sdkStorageOptions;
    }

    @Override
    public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
        throw new UnsupportedOperationException("A Lance SDK namespace wraps an initialized catalog namespace");
    }

    /**
     * Read by the SDK once, when it opens the dataset: Lance 12 describes the table in
     * {@code OpenDatasetBuilder.buildFromNamespaceClient} first and reads the id when the JNI
     * wraps this namespace. It keys the store cache, through the credential provider the SDK
     * builds from this namespace.
     */
    @Override
    public String namespaceId() {
        String identity = storeIdentity;
        if (identity == null) {
            throw new IllegalStateException("The Lance SDK read the namespace id before describing the table");
        }
        return "DorisSdkNamespace[" + catalogNamespace.namespaceId() + ", store=" + identity + "]";
    }

    /**
     * The catalog namespace's describe, with the vended options normalized. The first call is the
     * SDK's own describe while it opens the dataset, whose options the store is built with; later
     * ones refresh credentials.
     */
    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
        return record(() -> {
            DescribeTableResponse response = catalogNamespace.describeTable(request);
            Map<String, String> vended = LanceStorageOptions.normalizeVendedStorageOptions(
                    response.getLocation(), response.getStorageOptions());
            // Left null when nothing was vended: a credential refresh then keeps the options it has.
            if (response.getStorageOptions() != null) {
                response.setStorageOptions(vended);
            }
            if (storeIdentity == null) {
                Map<String, String> opened = new HashMap<>(sdkStorageOptions);
                opened.putAll(vended);
                storeIdentity = storeIdentity(opened);
            }
            return response;
        });
    }

    @Override
    public ListTableVersionsResponse listTableVersions(ListTableVersionsRequest request) {
        return record(() -> catalogNamespace.listTableVersions(request));
    }

    @Override
    public DescribeTableVersionResponse describeTableVersion(DescribeTableVersionRequest request) {
        return record(() -> catalogNamespace.describeTableVersion(request));
    }

    /**
     * The exception a failed SDK call reported as a failed callback, in place of that report, or
     * {@code sdkError} itself. Each kept exception is handed out once.
     */
    Exception unwrapCallbackFailure(Exception sdkError) {
        if (!isCallbackFailure(sdkError)) {
            return sdkError;
        }
        RuntimeException failure = callbackFailure.getAndSet(null);
        if (failure == null) {
            return sdkError;
        }
        failure.addSuppressed(sdkError);
        return failure;
    }

    private static boolean isCallbackFailure(Throwable error) {
        return ExceptionUtils.getThrowableList(error).stream()
                .anyMatch(cause -> cause.getMessage() != null && cause.getMessage().contains(CALLBACK_FAILURE));
    }

    private <T> T record(Supplier<T> call) {
        try {
            return call.get();
        } catch (RuntimeException e) {
            callbackFailure.set(e);
            if (Thread.currentThread() != openingThread) {
                // The JNI leaves the exception pending on a thread it attached only for this call,
                // and the JVM reports it as uncaught when that thread detaches. It is not: it
                // reaches the read through unwrapCallbackFailure.
                Thread.currentThread().setUncaughtExceptionHandler(
                        (thread, error) -> LOG.debug("Lance namespace callback failed", error));
            }
            throw e;
        }
    }

    /**
     * A digest of every option that fixes where and how a store connects. The options can name
     * endpoints and account names, so only the digest reaches the id, which Lance logs.
     */
    static String storeIdentity(Map<String, String> options) {
        Hasher hasher = Hashing.sha256().newHasher();
        new TreeMap<>(options).forEach((key, value) -> {
            if (!CREDENTIAL_OPTIONS.contains(key)) {
                hasher.putInt(key.length()).putString(key, StandardCharsets.UTF_8)
                        .putInt(value.length()).putString(value, StandardCharsets.UTF_8);
            }
        });
        return hasher.hash().toString();
    }
}
