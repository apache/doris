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

package org.apache.doris.kerberos;

/**
 * Neutral Hadoop authentication type for external metastore/storage connections.
 *
 * <p>Mirrors the closed {SIMPLE, KERBEROS} set of fe-common
 * {@code org.apache.doris.common.security.authentication.AuthType}, but lives in this
 * leaf module so connectors (which cannot depend on fe-common) can express the auth
 * type as a neutral fact.
 */
public enum AuthType {
    SIMPLE("simple"),
    KERBEROS("kerberos");

    private final String desc;

    AuthType(String desc) {
        this.desc = desc;
    }

    /** Returns the lowercase wire name ("simple" / "kerberos"). */
    public String getDesc() {
        return desc;
    }

    /**
     * Resolves an auth-type string to {@link #KERBEROS} when (and only when) it equals
     * {@code "kerberos"} case-insensitively; every other value — including {@code null},
     * blank, {@code "none"} and {@code "simple"} — resolves to {@link #SIMPLE}.
     *
     * <p>This matches the legacy semantics that treat the connection as Kerberos-secured
     * solely when the auth type is explicitly {@code kerberos}, and otherwise fall back to
     * simple authentication.
     */
    public static AuthType fromString(String value) {
        if (value != null && KERBEROS.desc.equalsIgnoreCase(value.trim())) {
            return KERBEROS;
        }
        return SIMPLE;
    }
}
