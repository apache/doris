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

import com.google.common.collect.ImmutableSet;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * One storage provider's option vocabulary, as Lance reads it.
 *
 * <p>Lance routes a dataset to a provider by URL scheme, and each provider accepts its own set of
 * option names. Nothing here may be applied without knowing which provider a dataset uses: the
 * spellings overlap but do not agree, so rewriting an option for the wrong provider destroys it.
 * {@code endpoint}, for instance, is what object_store's Azure parser and Lance's OSS provider
 * read, while its S3 parser also accepts {@code aws_endpoint} - renaming onto the S3 spelling
 * silently breaks the other two.
 *
 * <p>An implementation therefore only ever speaks for datasets {@link #forDataset} routed to it.
 */
public interface LanceStorageProvider {

    /** Schemes lance-io registers for its AWS provider (rust/lance-io/.../providers.rs). */
    Set<String> S3_SCHEMES = ImmutableSet.of("s3", "s3+ddb");

    /**
     * Converts Doris's own normalized storage properties into this provider's Lance options.
     *
     * <p>Empty when Doris has no static configuration vocabulary for the provider, which is every
     * provider but S3 today - those datasets are reachable only through what a namespace vends.
     */
    Map<String, String> fromDorisProperties(Map<String, String> backendProperties);

    /**
     * Rewrites the options a namespace vended onto the spelling {@link #fromDorisProperties}
     * emits, so that the two cannot reach Lance as competing entries for one config key.
     *
     * <p>Only the options Doris itself emits are rewritten. Everything else is passed through
     * untouched: the Lance Namespace specification describes {@code storage_options} as
     * configuration "passed directly to Lance", so a client cannot assume a vocabulary beyond the
     * one it contributes to itself.
     */
    Map<String, String> normalizeVended(Map<String, String> vendedOptions);

    /** The provider Lance will route this dataset to. */
    static LanceStorageProvider forDataset(String datasetUri) {
        return S3_SCHEMES.contains(schemeOf(datasetUri))
                ? LanceS3StorageProvider.INSTANCE : LancePassThroughStorageProvider.INSTANCE;
    }

    /**
     * The provider for a catalog's own storage configuration, which is described by Doris
     * properties rather than by a dataset URL. Only S3-compatible storage can be spelled that way.
     */
    static LanceStorageProvider forDorisCatalog() {
        return LanceS3StorageProvider.INSTANCE;
    }

    static String schemeOf(String datasetUri) {
        if (datasetUri == null) {
            return "";
        }
        int separator = datasetUri.indexOf("://");
        return separator < 0 ? "" : datasetUri.substring(0, separator).toLowerCase(Locale.ROOT);
    }
}
