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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.junit.Assert;
import org.junit.Test;

public class RangerHiveAccessControllerFactoryTest {

    /**
     * The two strings an operator selects this source by, frozen.
     *
     * <p>The class name matters most here: this source governs one catalog, and a catalog names it in
     * {@code access_controller.class}, which is persisted with the catalog and read back verbatim by every
     * later release - the regression suite for Hive catalogs writes exactly this string. It survived the move
     * out of fe-core unchanged, which is the only reason those catalogs kept working, so nothing but this
     * test stands between the next package move and a catalog nobody can query.
     *
     * <p>Written as literals on purpose: derived from the class, they would travel with it and a package move
     * would leave this green. Moving this class means adding the name it has today to the table of superseded
     * class names in {@code AccessControllerManager}.
     */
    @Test
    public void testTheSelectorsThisSourceIsNamedBy() {
        RangerHiveAccessControllerFactory factory = new RangerHiveAccessControllerFactory();

        Assert.assertEquals("ranger-hive", factory.name());
        Assert.assertEquals(
                "org.apache.doris.catalog.authorizer.ranger.hive.RangerHiveAccessControllerFactory",
                factory.getClass().getName());
    }
}
