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

package org.apache.doris.authorization;

import java.util.Objects;

/**
 * Whose access is being decided.
 *
 * <p>A subject is a MySQL style account: a user name together with the host it connects from, which may be
 * written either as an address pattern or as a domain name. All three parts identify the account - two
 * accounts differing only in host are different accounts with different grants - so all three are carried
 * here, and nothing else is: everything the engine knows about a connection beyond its account (the
 * certificate it presented, for one) takes no part in deciding access.</p>
 *
 * <p><b>Roles are deliberately absent.</b> They belong to {@code AuthorizationContext}, where a plugin asks
 * for them and pays for them only if it needs them. Resolving them up front would cost every check the price
 * of the one plugin that wants them - the built-in model never does, and filtering what a user may see walks
 * thousands of objects per statement.</p>
 */
public final class AuthorizedSubject {

    private final String user;
    private final String host;
    private final boolean domain;

    private AuthorizedSubject(String user, String host, boolean domain) {
        this.user = Objects.requireNonNull(user, "user is required");
        this.host = Objects.requireNonNull(host, "host is required");
        this.domain = domain;
    }

    /** An account whose host is an address or address pattern. */
    public static AuthorizedSubject of(String user, String host) {
        return new AuthorizedSubject(user, host, false);
    }

    /**
     * @param domain whether {@code host} names a domain to be resolved rather than an address pattern
     */
    public static AuthorizedSubject of(String user, String host, boolean domain) {
        return new AuthorizedSubject(user, host, domain);
    }

    /** The account name, qualified as the engine qualifies it. */
    public String getUser() {
        return user;
    }

    /** The host part of the account: an address pattern, or a domain name when {@link #isDomain()}. */
    public String getHost() {
        return host;
    }

    public boolean isDomain() {
        return domain;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AuthorizedSubject)) {
            return false;
        }
        AuthorizedSubject that = (AuthorizedSubject) o;
        return domain == that.domain && user.equals(that.user) && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, host, domain);
    }

    @Override
    public String toString() {
        return "'" + user + "'@'" + host + "'";
    }
}
