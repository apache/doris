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

import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Where a namespace-managed version's manifest must be for Doris to read that version.
 *
 * <p>Doris reads a managed version as it reads any other, by the dataset URI and the version
 * number, and so does the BE: Lance then opens {@code <chain>/_versions/<u64::MAX - v>.manifest},
 * or {@code <v>.manifest} in the V1 naming scheme, where the chain is the table root or
 * {@code <root>/tree/<branch>}. The namespace records a manifest path for each version. Lance's
 * own namespaces finalize a commit in CreateTableVersion and record that canonical path. A
 * namespace that records the path Lance's client sends records the staged manifest beside it,
 * named {@code <canonical>-<id>} ({@code make_staging_manifest_path}), and keeps it after the
 * commit is finalized, since Lance's namespace store cannot update a record. A recorded path
 * anywhere else names a manifest Doris would not read. It is also what a namespace answers once
 * it has moved the table away from the location this read described, if the move changed the
 * path inside the bucket; a path is relative to its bucket or container, so a move to another one
 * under the same path is not seen here.
 */
final class LanceManifestPaths {

    /** How the namespace records a version whose manifest is where Doris reads it. */
    enum Recorded {
        /** At its canonical path. */
        CANONICAL,
        /**
         * At a staged manifest beside the canonical path. The version may not have been finalized
         * yet, or was finalized after the namespace recorded the staged path.
         */
        STAGED
    }

    private static final String MANIFEST_EXTENSION = ".manifest";

    private static final BigInteger U64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    /** A URL scheme; lance-io takes a single letter before the colon for a Windows drive instead. */
    private static final Pattern URL_SCHEME = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]+:");

    private LanceManifestPaths() {
    }

    /**
     * How the namespace records version {@code version} of the chain of {@code tableUri} (the
     * table root) on {@code branch}.
     *
     * @throws IllegalStateException if the recorded path is neither the canonical path nor a
     *     staged manifest beside it
     */
    static Recorded check(String tableUri, Optional<String> branch, long version, String manifestPath,
            String tableName) {
        String chain = objectStorePath(tableUri);
        if (branch.isPresent()) {
            chain = (chain.isEmpty() ? "" : chain + "/") + "tree/" + branch.get();
        }
        String versions = (chain.isEmpty() ? "" : chain + "/") + "_versions/";
        // Padded by hand: String.format would use the FE's locale digits, and Lance writes ASCII.
        String canonical = versions + StringUtils.leftPad(U64_MAX.subtract(BigInteger.valueOf(version)).toString(),
                20, '0') + MANIFEST_EXTENSION;
        // Lance parses the recorded path first, which drops surrounding slashes.
        String recorded = manifestPath == null ? "" : StringUtils.strip(manifestPath, "/");
        for (String name : new String[] {canonical, versions + version + MANIFEST_EXTENSION}) {
            if (recorded.equals(name)) {
                return Recorded.CANONICAL;
            }
            if (recorded.startsWith(name + "-") && recorded.indexOf('/', name.length()) < 0) {
                return Recorded.STAGED;
            }
        }
        throw new IllegalStateException("Lance namespace records version " + version + " of " + tableName
                + branch.map(name -> "@" + name).orElse("") + " at manifest '" + manifestPath
                + "', but Doris reads that version from '" + canonical
                + "'; if the table was moved during the query, retry it");
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
}
