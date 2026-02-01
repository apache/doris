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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Authorization subject representing a user session.
 *
 * <p>Subject is the authorization-layer representation of an authenticated user.
 * It contains the user identity (via {@link Principal}) plus role information
 * needed for access control decisions.
 *
 * <p>Design note: Subject focuses purely on authorization concerns:
 * <ul>
 *   <li>WHO - the authenticated principal</li>
 *   <li>WHAT ROLES - active and available roles</li>
 *   <li>WHERE FROM - source IP for IP-based access control</li>
 * </ul>
 *
 * <p>Protocol-specific information (connection type, client info) belongs
 * in the session context, not the Subject.
 *
 * <p>This design follows the standard security pattern where:
 * <ul>
 *   <li>Authentication produces a Principal</li>
 *   <li>Principal + Roles = Subject</li>
 *   <li>Subject is used for authorization decisions</li>
 * </ul>
 *
 * @see Principal
 * @see Identity
 */
public final class Subject {

    /**
     * The authenticated principal.
     */
    private final Principal principal;

    /**
     * Currently active roles for this session.
     * These are the roles that will be used for permission checks.
     */
    private volatile Set<String> activeRoles;

    /**
     * All roles available to this user.
     * User can activate/deactivate roles from this set.
     */
    private final Set<String> availableRoles;

    /**
     * Client IP address.
     * Used for IP-based access control and audit logging.
     */
    private final String sourceIp;

    private Subject(Builder builder) {
        this.principal = Objects.requireNonNull(builder.principal, "principal is required");
        this.activeRoles = Collections.unmodifiableSet(new HashSet<>(builder.activeRoles));
        this.availableRoles = Collections.unmodifiableSet(new HashSet<>(builder.availableRoles));
        this.sourceIp = builder.sourceIp;
    }

    /**
     * Returns the authenticated principal.
     *
     * @return the principal
     */
    public Principal getPrincipal() {
        return principal;
    }

    /**
     * Returns the username.
     * Convenience method equivalent to {@code getPrincipal().getName()}.
     *
     * @return the username
     */
    public String getUser() {
        return principal.getName();
    }

    /**
     * Returns the currently active roles.
     *
     * @return immutable set of active role names
     */
    public Set<String> getActiveRoles() {
        return activeRoles;
    }

    /**
     * Sets the active roles.
     * Roles must be a subset of available roles.
     *
     * @param roles the roles to activate
     * @throws IllegalArgumentException if any role is not in available roles
     */
    public void setActiveRoles(Set<String> roles) {
        if (roles != null) {
            for (String role : roles) {
                if (!availableRoles.contains(role)) {
                    throw new IllegalArgumentException("Role not available: " + role);
                }
            }
        }
        this.activeRoles = roles != null
                ? Collections.unmodifiableSet(new HashSet<>(roles))
                : Collections.emptySet();
    }

    /**
     * Returns all available roles.
     *
     * @return immutable set of available role names
     */
    public Set<String> getAvailableRoles() {
        return availableRoles;
    }

    /**
     * Returns the client source IP address.
     *
     * @return source IP, may be null
     */
    public String getSourceIp() {
        return sourceIp;
    }

    /**
     * Checks if the user has a specific role active.
     *
     * @param role the role to check
     * @return true if role is active
     */
    public boolean hasRole(String role) {
        return activeRoles.contains(role);
    }

    /**
     * Checks if the user has any of the specified roles active.
     *
     * @param roles the roles to check
     * @return true if any role is active
     */
    public boolean hasAnyRole(Set<String> roles) {
        for (String role : roles) {
            if (activeRoles.contains(role)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets the identity if the principal is an Identity instance.
     * This provides access to extended identity information like expiration.
     *
     * @return the Identity, or null if principal is not an Identity
     */
    public Identity getIdentity() {
        return principal instanceof Identity ? (Identity) principal : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Subject subject = (Subject) o;
        return Objects.equals(principal.getName(), subject.principal.getName())
                && Objects.equals(sourceIp, subject.sourceIp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal.getName(), sourceIp);
    }

    @Override
    public String toString() {
        return "Subject{"
                + "user='" + principal.getName() + '\''
                + ", activeRoles=" + activeRoles
                + ", sourceIp='" + sourceIp + '\''
                + '}';
    }

    /**
     * Creates a new builder for Subject.
     *
     * @return new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link Subject}.
     */
    public static final class Builder {
        private Principal principal;
        private Set<String> activeRoles = new HashSet<>();
        private Set<String> availableRoles = new HashSet<>();
        private String sourceIp;

        private Builder() {
        }

        public Builder principal(Principal principal) {
            this.principal = principal;
            return this;
        }

        public Builder activeRoles(Set<String> activeRoles) {
            this.activeRoles = activeRoles != null ? activeRoles : new HashSet<>();
            return this;
        }

        public Builder availableRoles(Set<String> availableRoles) {
            this.availableRoles = availableRoles != null ? availableRoles : new HashSet<>();
            return this;
        }

        public Builder sourceIp(String sourceIp) {
            this.sourceIp = sourceIp;
            return this;
        }

        public Subject build() {
            return new Subject(this);
        }
    }
}
