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

package org.apache.doris.mysql.privilege;

import org.apache.doris.common.Config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Every string an older release let an operator select an authorization source by still selects it.
 *
 * <p>One of the two channels is not typed but persisted: a catalog's {@code access_controller.class} is
 * written into the catalog's properties when the catalog is created, and read back verbatim by every FE
 * afterwards - including one upgraded across the release that moved the Ranger sources out of fe-core into
 * plugins of their own. Nothing rewrites those properties, so a value that ever worked has to keep working
 * or the catalog stops being reachable at the first statement that touches it.
 *
 * <p><b>Every selector below is written as a literal.</b> Derived instead from the class that publishes the
 * source - {@code SomeFactory.class.getName()} - the expectation would travel with the code it is supposed to
 * pin, and moving a factory to another package, which is exactly the change this test exists for, would leave
 * it green.
 */
public class AuthorizationSourceSelectorCompatibilityTest {

    /**
     * The one identifier this work invalidated. That factory lived in this very package until the Ranger
     * sources moved out of fe-core; leaving it here would have split the package across two jars.
     */
    private static final String DORIS_RANGER_FACTORY_BEFORE_THE_MOVE =
            "org.apache.doris.mysql.privilege.RangerDorisAccessControllerFactory";

    @Test
    public void theClassNameASourceHadBeforeLeavingTheKernelStillNamesIt() {
        AccessControllerManager manager = new AccessControllerManager(new Auth());

        Assertions.assertEquals("ranger-doris",
                manager.getPluginIdentifierForAccessController(DORIS_RANGER_FACTORY_BEFORE_THE_MOVE),
                "a catalog created before the Ranger sources became plugins names a class that is gone;"
                        + " it has to keep resolving to the source now publishing that behaviour");
    }

    @Test
    public void sourceStillPublishedByItsOriginalClassNeedsNoAlias() {
        // What keeps the Hive source's class name working - it did not move, so the runtime table filled at
        // registration answers for it, with no entry in the alias table. Asserted on the Hive source itself
        // because this module has it as a test dependency, and on a stub as well, so that the mechanism is
        // still covered if that dependency ever goes away.
        AccessControllerManager manager = new AccessControllerManager(new Auth());

        Assertions.assertEquals("ranger-hive", manager.getPluginIdentifierForAccessController(
                        "org.apache.doris.catalog.authorizer.ranger.hive.RangerHiveAccessControllerFactory"),
                "the class name a Hive catalog persisted in access_controller.class no longer resolves, so"
                        + " every such catalog is unusable");
        Assertions.assertEquals("stub-ranger-doris", manager.getPluginIdentifierForAccessController(
                        "org.apache.doris.mysql.privilege.StubRangerAccessControllerFactory"),
                "a factory class that still exists must resolve without the alias table");
    }

    @Test
    public void theNameASourceIsPublishedUnderIsASelectorToo() {
        AccessControllerManager manager = new AccessControllerManager(new Auth());

        // The form fe.conf uses. Its instance-wide channel never accepted a class name - access_controller_type
        // has always been a name - so for that channel this is the whole of the compatibility question, once
        // the plugin directory is in place.
        Assertions.assertEquals("ranger-doris",
                manager.getPluginIdentifierForAccessController("ranger-doris"));
        Assertions.assertEquals("default", InternalAuthorizationPlugin.NAME,
                "the value access_controller_type has meant 'the built-in privilege model' since before"
                        + " any of this");
        // Read from the setting, not asserted about it: an FE that has configured nothing selects the built-in
        // model, and that is the value every deployment not naming a source is running under.
        Assertions.assertEquals(InternalAuthorizationPlugin.NAME, Config.access_controller_type,
                "access_controller_type no longer defaults to the built-in privilege model, so an FE that"
                        + " configures nothing now selects something else");
    }

    @Test
    public void anUnknownSourceSaysWhereAPluginHasToBeInstalled() {
        AccessControllerManager manager = new AccessControllerManager(new Auth());

        RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                () -> manager.getPluginIdentifierForAccessController("org.example.NoSuchFactory"));

        // The message an upgraded deployment gets when its plugin directory was not carried across. Naming
        // the directory is the whole of its usefulness: the source it is asking for exists, in a place the
        // operator has not looked.
        Assertions.assertTrue(e.getMessage().contains(Config.authorization_plugins_dir),
                "the failure does not say where an authorization plugin belongs: " + e.getMessage());
    }
}
