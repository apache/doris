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

package org.apache.doris.catalog.authorizer.openfga;

import java.util.Objects;

/**
 * One OpenFGA relationship check: does {@code user} have {@code relation} on {@code object}.
 *
 * <p>All three fields are already-formatted OpenFGA strings, for example
 * user {@code user:analyst}, relation {@code can_select}, object {@code table:internal/db1/tbl1}.
 */
public class OpenFgaCheckRequest {
    private final String user;
    private final String relation;
    private final String object;

    public OpenFgaCheckRequest(String user, String relation, String object) {
        this.user = user;
        this.relation = relation;
        this.object = object;
    }

    public String getUser() {
        return user;
    }

    public String getRelation() {
        return relation;
    }

    public String getObject() {
        return object;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OpenFgaCheckRequest that = (OpenFgaCheckRequest) o;
        return Objects.equals(user, that.user)
                && Objects.equals(relation, that.relation)
                && Objects.equals(object, that.object);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, relation, object);
    }

    @Override
    public String toString() {
        return "OpenFgaCheckRequest{user=" + user + ", relation=" + relation + ", object=" + object + "}";
    }
}
