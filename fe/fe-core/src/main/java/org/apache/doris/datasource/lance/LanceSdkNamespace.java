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
import org.apache.commons.lang3.StringUtils;
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
import org.lance.namespace.model.TableVersion;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * The namespace the Lance SDK is handed to open one namespace-managed dataset: the catalog's
 * namespace, with three things the SDK gets wrong on its own.
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
 * {@link #namespaceId} therefore also identifies the table location and the options the store is
 * built with, less credentials the store refreshes from the namespace anyway.
 *
 * <p>Lance opens a finalized manifest a namespace records wherever it is, while the BE opens a
 * version by the dataset URI and number, at the canonical path. {@link #describeTableVersion} and
 * {@link #listTableVersions} therefore reject a finalized manifest anywhere else.
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

    private static final String EXPIRES_AT_MILLIS = "expires_at_millis";

    /** The values lance-core's {@code str_is_truthy} accepts, lower case. */
    private static final Set<String> TRUTHY = ImmutableSet.of("1", "true", "on", "yes", "y");

    private static final String MANIFEST_EXTENSION = ".manifest";

    /** A URL scheme; lance-io takes a single letter before the colon for a Windows drive instead. */
    private static final Pattern URL_SCHEME = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]+:");

    /**
     * The credentials a store takes from its credential provider rather than fixing them when it
     * is built: every spelling lance-io's {@code DynamicCredentials} conversions read for AWS,
     * Azure and GCS, the OSS keys its dynamic OpenDAL store re-reads, and the refresh deadline.
     * The provider refreshes them from the namespace only when they carry
     * {@value #EXPIRES_AT_MILLIS} and the store is not an OpenDAL one; see {@link #storeIdentity}.
     */
    private static final Set<String> CREDENTIAL_OPTIONS = ImmutableSet.of(
            "aws_access_key_id", "access_key_id", "aws_secret_access_key", "secret_access_key",
            "aws_session_token", "aws_token", "aws_security_token", "session_token", "token",
            "azure_storage_sas_token", "azure_storage_sas_key", "sas_token", "sas_key",
            "azure_storage_token", "bearer_token", "azure_storage_account_key", "azure_storage_access_key",
            "azure_storage_master_key", "access_key", "master_key", "account_key",
            "google_storage_token",
            "oss_access_key_id", "oss_secret_access_key", "oss_security_token",
            EXPIRES_AT_MILLIS);

    /** Keys the store digest, so an id in a log cannot be checked against guessed credentials. */
    private static final byte[] IDENTITY_KEY = new byte[32];

    static {
        new SecureRandom().nextBytes(IDENTITY_KEY);
    }

    private final LanceNamespace catalogNamespace;
    private final Map<String, String> sdkStorageOptions;
    private final AtomicReference<RuntimeException> callbackFailure = new AtomicReference<>();
    /** The thread that opens the dataset and issues the SDK's describe; every other call is a JNI callback. */
    private final Thread openingThread = Thread.currentThread();
    /** Set by the SDK's own describe while it opens the dataset. */
    private volatile String storeIdentity;
    /** The table location the SDK's own describe returned; set with {@link #storeIdentity}. */
    private volatile String tableLocation;

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
                tableLocation = response.getLocation();
                storeIdentity = storeIdentity(tableLocation, opened);
            }
            return response;
        });
    }

    /**
     * The catalog namespace's version list. Lance takes the newest entry's manifest as the head of
     * a chain, so every entry is checked with {@link #checkManifestPath}.
     */
    @Override
    public ListTableVersionsResponse listTableVersions(ListTableVersionsRequest request) {
        return record(() -> {
            ListTableVersionsResponse response = catalogNamespace.listTableVersions(request);
            if (response.getVersions() != null) {
                response.getVersions().forEach(version -> checkManifestPath(request.getBranch(), version));
            }
            return response;
        });
    }

    /** The catalog namespace's describe of one version, whose manifest Lance opens; see {@link #checkManifestPath}. */
    @Override
    public DescribeTableVersionResponse describeTableVersion(DescribeTableVersionRequest request) {
        return record(() -> {
            DescribeTableVersionResponse response = catalogNamespace.describeTableVersion(request);
            checkManifestPath(request.getBranch(), response.getVersion());
            return response;
        });
    }

    /**
     * Rejects a finalized manifest the BE would not open. The BE opens a version by the dataset URI
     * and number, at {@code <chain>/_versions/<u64::MAX - v>.manifest} (or {@code <v>.manifest} for
     * the V1 naming scheme). Lance opens a manifest path ending in {@code .manifest} as recorded,
     * and copies any other (staged) one to that canonical path first
     * ({@code ExternalManifestCommitHandler::resolve_version_location}), so only a finalized path
     * elsewhere can leave the FE and the BE reading different manifests.
     */
    private void checkManifestPath(String branch, TableVersion version) {
        String path = version == null ? null : version.getManifestPath();
        // Lance parses the path first, which drops surrounding slashes.
        String recorded = path == null ? "" : StringUtils.strip(path, "/");
        if (version == null || version.getVersion() == null || !recorded.endsWith(MANIFEST_EXTENSION)) {
            return;
        }
        if (storeIdentity == null) {
            throw new IllegalStateException("The Lance SDK resolved a version before describing the table");
        }
        String chain = objectStorePath(tableLocation);
        if (branch != null) {
            chain = (chain.isEmpty() ? "" : chain + "/") + "tree/" + branch;
        }
        String versions = (chain.isEmpty() ? "" : chain + "/") + "_versions/";
        long number = version.getVersion();
        String canonical = versions + String.format("%020d",
                BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE).subtract(BigInteger.valueOf(number)))
                + MANIFEST_EXTENSION;
        if (!recorded.equals(canonical) && !recorded.equals(versions + number + MANIFEST_EXTENSION)) {
            throw new IllegalStateException("Lance namespace records version " + number
                    + (branch == null ? "" : " of branch '" + branch + "'") + " at manifest '" + path
                    + "', but Doris reads a version from its canonical path '" + canonical + "'");
        }
    }

    /**
     * A dataset location as Lance's object store addresses it, derived as lance-io derives it: the
     * path of a URL after its authority (the bucket, container or host), percent-decoded
     * ({@code Path::from_url_path}), and a location without a scheme as is. Surrounding slashes
     * are dropped, so a table at the root of a bucket has an empty path.
     */
    static String objectStorePath(String location) {
        if (location == null) {
            throw new IllegalArgumentException("Lance namespace returned no table location");
        }
        if (!URL_SCHEME.matcher(location).find()) {
            return StringUtils.strip(location, "/");
        }
        String path = StringUtils.substringBefore(StringUtils.substringBefore(location, "?"), "#");
        path = path.substring(path.indexOf(':') + 1);
        if (path.startsWith("//")) {
            int slash = path.indexOf('/', 2);
            path = slash < 0 ? "" : path.substring(slash);
        }
        return StringUtils.strip(percentDecode(path), "/");
    }

    /** Decodes {@code %XX} escapes as UTF-8 bytes and keeps anything else, as Rust's percent_decode does. */
    private static String percentDecode(String text) {
        if (text.indexOf('%') < 0) {
            return text;
        }
        byte[] raw = text.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream decoded = new ByteArrayOutputStream(raw.length);
        for (int i = 0; i < raw.length; i++) {
            int high = i + 2 < raw.length && raw[i] == '%' ? Character.digit(raw[i + 1], 16) : -1;
            int low = high < 0 ? -1 : Character.digit(raw[i + 2], 16);
            if (low < 0) {
                decoded.write(raw[i]);
            } else {
                decoded.write(high * 16 + low);
                i += 2;
            }
        }
        return new String(decoded.toByteArray(), StandardCharsets.UTF_8);
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

    /** {@code options} without any credential: what fixes where and how a store connects. */
    static Map<String, String> withoutCredentials(Map<String, String> options) {
        Map<String, String> result = new HashMap<>(options);
        result.keySet().removeAll(CREDENTIAL_OPTIONS);
        return result;
    }

    /**
     * A digest of the table location and every option that fixes where and how a store connects.
     * The options can name endpoints and account names, so only the digest reaches the id, which
     * Lance logs. The location counts because a namespace may vend credentials that only cover the
     * table's own prefix; its query is left out, since it may carry credentials a namespace vends
     * anew on every describe.
     *
     * <p>Credentials the store refreshes are left out: it refreshes them from the namespace before
     * they expire, and a namespace that vends new ones on every describe would otherwise leave one
     * registry entry behind per read. That takes an expiry, and a store other than an OpenDAL one
     * ({@code use_opendal}), which lance-io builds from the options once for S3, Azure and GCS.
     * Otherwise the store keeps the credentials it was built with for as long as any read holds
     * it, so they count, as they do for a store Lance opens without a namespace. OSS always builds
     * a refreshing OpenDAL store; counting its credentials under {@code use_opendal} only splits
     * stores that could be shared. lance-io only reads an expiry that parses as an unsigned 64-bit
     * integer, as {@link Long#parseUnsignedLong} does.
     */
    static String storeIdentity(String location, Map<String, String> options) {
        String openDal = options.get("use_opendal");
        boolean refreshed = parsesAsUnsignedLong(options.get(EXPIRES_AT_MILLIS))
                && (openDal == null || !TRUTHY.contains(openDal.toLowerCase(Locale.ROOT)));
        Hasher hasher = Hashing.hmacSha256(IDENTITY_KEY).newHasher();
        String where = location == null ? "" : StringUtils.removeEnd(StringUtils.substringBefore(location, "?"), "/");
        hasher.putInt(where.length()).putString(where, StandardCharsets.UTF_8);
        new TreeMap<>(options).forEach((key, value) -> {
            if (!refreshed || !CREDENTIAL_OPTIONS.contains(key)) {
                hasher.putInt(key.length()).putString(key, StandardCharsets.UTF_8)
                        .putInt(value.length()).putString(value, StandardCharsets.UTF_8);
            }
        });
        return hasher.hash().toString();
    }

    private static boolean parsesAsUnsignedLong(String value) {
        if (value == null) {
            return false;
        }
        try {
            Long.parseUnsignedLong(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
