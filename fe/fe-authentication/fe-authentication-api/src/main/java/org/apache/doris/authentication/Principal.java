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

package org.apache.doris.authentication;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Represents an authenticated principal (user identity).
 *
 * <p>This interface follows the standard Java security principal concept
 * (similar to {@link java.security.Principal}) but extends it with
 * additional attributes needed for Doris authentication.
 *
 * <p>Design reference: Trino's authentication returns a Principal after
 * successful authentication, which is then passed to the access control
 * layer for authorization decisions.
 *
 * <p>The Principal represents WHO the user is (authentication result),
 * while authorization (WHAT the user can do) is handled separately by
 * the access control layer.
 */
public interface Principal {

    /**
     * Returns the name of this principal.
     * This is typically the username that will be used within Doris.
     *
     * @return the name of this principal
     */
    String getName();

    /**
     * Returns the authenticator name that authenticated this principal.
     *
     * @return the authenticator name (e.g., "password", "ldap", "oidc")
     */
    String getAuthenticator();

    /**
     * Returns the external principal identifier if authenticated via
     * external identity provider (e.g., LDAP DN, OIDC subject).
     *
     * @return optional external principal identifier
     */
    default Optional<String> getExternalPrincipal() {
        return Optional.empty();
    }

    /**
     * Returns external groups the principal belongs to.
     * These can be used for role mapping.
     *
     * @return set of external group names, empty set if none
     */
    default Set<String> getExternalGroups() {
        return Collections.emptySet();
    }

    /**
     * Returns additional attributes about the principal.
     * These may include email, display name, etc.
     *
     * @return map of attribute names to values
     */
    default Map<String, String> getAttributes() {
        return Collections.emptyMap();
    }

    /**
     * Returns whether this is a service principal (non-human / machine account).
     *
     * <p>This flag is intended to distinguish service accounts from human users
     * for policy and audit purposes. It does NOT refer to Kerberos "service
     * principal" names.</p>
     *
     * @return true if this is a service account
     */
    default boolean isServicePrincipal() {
        return false;
    }
}
