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

package org.apache.doris.authentication.plugin.oidc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class OidcIdentity {

    private final String username;
    private final String subject;
    private final Set<String> groups;
    private final Map<String, String> attributes;

    public OidcIdentity(String username, String subject, Set<String> groups, Map<String, String> attributes) {
        this.username = Objects.requireNonNull(username, "username is required");
        this.subject = Objects.requireNonNull(subject, "subject is required");
        this.groups = Collections.unmodifiableSet(new LinkedHashSet<>(groups));
        this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
    }

    public String getUsername() {
        return username;
    }

    public String getSubject() {
        return subject;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }
}
