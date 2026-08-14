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

package org.apache.doris.jni.bootstrap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;

/**
 * The default this class falls back to when start_be.sh did not set the property is a second,
 * independent copy of a path BE also holds in its own config. Nothing makes the compiler check
 * that the two agree, and a disagreement does not fail a build - it makes every plugin report
 * "is not deployed" at query time. These tests are what holds them together.
 */
public class PluginRegistryDefaultDirTest {

    private static final String PROPERTY = "doris.jni.plugin.dir";

    private String saved;

    @Before
    public void clearProperty() {
        saved = System.getProperty(PROPERTY);
        System.clearProperty(PROPERTY);
    }

    @After
    public void restoreProperty() {
        if (saved == null) {
            System.clearProperty(PROPERTY);
        } else {
            System.setProperty(PROPERTY, saved);
        }
    }

    /**
     * lib/ is the engine tree a package upgrade replaces wholesale. A plugin deployed there would
     * not survive one, which is why the family root sits under plugins/ instead. An operator who
     * sets nothing gets exactly this path, so it is the one that has to be right.
     */
    @Test
    public void defaultLivesUnderPluginsNotLib() {
        Path dir = PluginRegistry.pluginDir();
        Assert.assertTrue(dir.toString(), dir.endsWith("plugins/jni"));
        Assert.assertFalse(dir.toString(), dir.toString().contains("/lib/"));
    }

    /** An explicitly configured directory wins; the default is only a fallback. */
    @Test
    public void configuredPropertyWinsOverTheDefault() {
        System.setProperty(PROPERTY, "/somewhere/else");
        Assert.assertTrue(PluginRegistry.pluginDir().toString(), PluginRegistry.pluginDir().endsWith("somewhere/else"));
    }
}
