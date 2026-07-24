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

import org.apache.doris.catalog.authorizer.ranger.doris.DorisAccessType;

/**
 * Maps a Doris access type to the relation name used in the OpenFGA authorization model.
 *
 * <p>The relation names here must stay in sync with the relations declared in
 * {@code schema.fga} (see fe/fe-core/src/main/resources/authorizer/openfga/). A single Doris
 * privilege check is turned into exactly one OpenFGA {@code Check(user, relation, object)} call
 * by looking up the relation for the wanted {@link DorisAccessType}.
 */
public enum OpenFgaRelation {
    NODE(DorisAccessType.NODE, "can_node"),
    ADMIN(DorisAccessType.ADMIN, "can_admin"),
    GRANT(DorisAccessType.GRANT, "can_grant"),
    SELECT(DorisAccessType.SELECT, "can_select"),
    LOAD(DorisAccessType.LOAD, "can_load"),
    ALTER(DorisAccessType.ALTER, "can_alter"),
    CREATE(DorisAccessType.CREATE, "can_create"),
    DROP(DorisAccessType.DROP, "can_drop"),
    USAGE(DorisAccessType.USAGE, "can_usage"),
    SHOW_VIEW(DorisAccessType.SHOW_VIEW, "can_show_view");

    private final DorisAccessType accessType;
    private final String relation;

    OpenFgaRelation(DorisAccessType accessType, String relation) {
        this.accessType = accessType;
        this.relation = relation;
    }

    public DorisAccessType getAccessType() {
        return accessType;
    }

    public String getRelation() {
        return relation;
    }

    /**
     * Returns the OpenFGA relation for the given access type, or null if the access type does not
     * map to a checkable relation (for example {@link DorisAccessType#NONE}). A null result means
     * the privilege has no OpenFGA representation and must be treated as "not allowed".
     */
    public static String relationOf(DorisAccessType accessType) {
        for (OpenFgaRelation value : values()) {
            if (value.accessType == accessType) {
                return value.relation;
            }
        }
        return null;
    }
}
