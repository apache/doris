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

package org.apache.doris.connector.adbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The handle keeps all three remote levels, so nothing ever has to recover them from the Doris name.
 */
class AdbcTableHandleTest {

    @Test
    void remotePartsSurviveACatalogNameContainingADot() {
        // The case that makes re-derivation impossible rather than merely awkward: with catalog "MY.DB" and
        // schema "PUBLIC", a joined name reads equally well as ("MY", "DB.PUBLIC") or ("MY.DB", "PUBLIC"),
        // and the wrong reading produces SQL against a table that does not exist. Storing the parts is what
        // keeps the ambiguity out of the code.
        AdbcTableHandle handle = new AdbcTableHandle(new AdbcNamespace("MY.DB", "PUBLIC"), "ORDERS");

        Assertions.assertEquals("MY.DB", handle.getRemoteCatalog());
        Assertions.assertEquals("PUBLIC", handle.getRemoteDbSchema());
        Assertions.assertEquals("ORDERS", handle.getRemoteTable());
        Assertions.assertEquals("PUBLIC", handle.getDorisDbName());
    }

    @Test
    void tableNameContainingADotIsAlsoKeptWhole() {
        AdbcTableHandle handle = new AdbcTableHandle(new AdbcNamespace("", "sales"), "q1.orders");

        Assertions.assertEquals("q1.orders", handle.getRemoteTable());
        Assertions.assertEquals("sales", handle.getDorisDbName());
    }

    @Test
    void namespaceRoundTripsThroughTheHandle() {
        AdbcNamespace namespace = new AdbcNamespace("main", "");
        Assertions.assertEquals(namespace, new AdbcTableHandle(namespace, "t1").getNamespace());
    }
}
