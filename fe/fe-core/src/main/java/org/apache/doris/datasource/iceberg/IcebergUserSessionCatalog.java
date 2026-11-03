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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;

import org.apache.iceberg.rest.RESTSessionCatalog;

/**
 * Capability interface for an Iceberg catalog that supports per-user dynamic session
 * (i.e. {@code iceberg.rest.session=user}). Only {@link IcebergRestExternalCatalog} implements it; every other
 * Iceberg catalog type is not session-aware.
 *
 * <p>{@link IcebergMetadataOps} depends on this capability rather than on the concrete REST catalog class or its
 * {@link IcebergRestProperties}, so it never has to {@code instanceof}-and-dig for the REST-specific behaviors it
 * needs (dynamic session, views, nested namespaces). This mirrors how Iceberg itself models optional capabilities
 * (e.g. {@code SupportsNamespaces}, {@code ViewCatalog}).
 */
public interface IcebergUserSessionCatalog {

    /**
     * Whether the given request should use a per-user session catalog. Single source of truth for the decision,
     * used both for cache bypass and for routing metadata calls. Returns {@code false} when dynamic identity is
     * disabled (use the shared default path) and {@code true} when it is enabled and the request carries a
     * delegated credential.
     *
     * @throws IllegalStateException when dynamic identity is enabled but the request has no delegated credential.
     *     Such a catalog has no shared identity to fall back on, so a tokenless session is rejected rather than
     *     served by borrowing another request's credential.
     */
    boolean useSessionCatalog(SessionContext ctx);

    /** The session-aware Iceberg REST catalog backing this catalog (may be null before initialization). */
    RESTSessionCatalog getRestSessionCatalog();

    /** The delegated-token mode used when attaching the user's credential to session requests. */
    IcebergRestProperties.DelegatedTokenMode getDelegatedTokenMode();

    /** Whether Iceberg view endpoints are enabled for this catalog. */
    boolean isViewEnabled();

    /** Whether nested namespaces are enabled for this catalog. */
    boolean isNestedNamespaceEnabled();
}
