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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class AuthorizedResourceTest {

    @Test
    public void testEachFactoryReportsItsKind() {
        Assertions.assertEquals(ResourceKind.GLOBAL, AuthorizedResource.global().getKind());
        Assertions.assertEquals(ResourceKind.CATALOG, AuthorizedResource.catalog("ctl").getKind());
        Assertions.assertEquals(ResourceKind.DATABASE, AuthorizedResource.database("ctl", "db").getKind());
        Assertions.assertEquals(ResourceKind.TABLE, AuthorizedResource.table("ctl", "db", "tbl").getKind());
        Assertions.assertEquals(ResourceKind.COLUMNS,
                AuthorizedResource.columns("ctl", "db", "tbl", Set.of("c")).getKind());
        Assertions.assertEquals(ResourceKind.RESOURCE, AuthorizedResource.resource("r").getKind());
        Assertions.assertEquals(ResourceKind.WORKLOAD_GROUP, AuthorizedResource.workloadGroup("wg").getKind());
        Assertions.assertEquals(ResourceKind.STORAGE_VAULT, AuthorizedResource.storageVault("v").getKind());
        Assertions.assertEquals(ResourceKind.CLOUD_STAGE,
                AuthorizedResource.cloud(ResourceKind.CLOUD_STAGE, "s").getKind());
    }

    @Test
    public void testNameAloneDoesNotIdentifyASystemObject() {
        // A workload group and a resource may well share a name; treating them as the same object would
        // answer a question about one of them with the policies of the other.
        Assertions.assertNotEquals(AuthorizedResource.resource("shared"),
                AuthorizedResource.workloadGroup("shared"));
        Assertions.assertNotEquals(AuthorizedResource.storageVault("shared"),
                AuthorizedResource.cloud(ResourceKind.CLOUD_STORAGE_VAULT, "shared"));
    }

    @Test
    public void testOnlyCloudKindsAreAcceptedAsCloudObjects() {
        for (ResourceKind kind : ResourceKind.values()) {
            if (kind.name().startsWith("CLOUD_")) {
                Assertions.assertEquals(kind, AuthorizedResource.cloud(kind, "name").getKind());
            } else {
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> AuthorizedResource.cloud(kind, "name"));
            }
        }
    }

    @Test
    public void testColumnsKeepTheOrderTheyWereAskedIn() {
        // A refusal names the first column that failed. If the set reordered them, which column the user is
        // told about would depend on hashing rather than on the query.
        List<String> asked = Arrays.asList("z_col", "a_col", "m_col");

        AuthorizedResource.Columns columns =
                AuthorizedResource.columns("ctl", "db", "tbl", new LinkedHashSet<>(asked));

        Assertions.assertEquals(asked, new ArrayList<>(columns.getColumns()));
    }

    @Test
    public void testColumnsCannotBeChangedAfterwards() {
        AuthorizedResource.Columns columns =
                AuthorizedResource.columns("ctl", "db", "tbl", Set.of("c1"));

        Assertions.assertThrows(UnsupportedOperationException.class, () -> columns.getColumns().add("c2"));
    }

    @Test
    public void testResourcesAreCompared() {
        Assertions.assertEquals(AuthorizedResource.table("c", "d", "t"), AuthorizedResource.table("c", "d", "t"));
        Assertions.assertEquals(AuthorizedResource.table("c", "d", "t").hashCode(),
                AuthorizedResource.table("c", "d", "t").hashCode());
        Assertions.assertNotEquals(AuthorizedResource.table("c", "d", "t"),
                AuthorizedResource.table("c", "d", "other"));
        Assertions.assertNotEquals(AuthorizedResource.database("c", "d"), AuthorizedResource.catalog("c"));
        Assertions.assertSame(AuthorizedResource.global(), AuthorizedResource.global());
    }

    @Test
    public void testMissingNamesAreRefused() {
        Assertions.assertThrows(NullPointerException.class, () -> AuthorizedResource.catalog(null));
        Assertions.assertThrows(NullPointerException.class, () -> AuthorizedResource.database("c", null));
        Assertions.assertThrows(NullPointerException.class, () -> AuthorizedResource.table("c", "d", null));
        Assertions.assertThrows(NullPointerException.class,
                () -> AuthorizedResource.columns("c", "d", "t", null));
        Assertions.assertThrows(NullPointerException.class, () -> AuthorizedResource.resource(null));
    }
}
