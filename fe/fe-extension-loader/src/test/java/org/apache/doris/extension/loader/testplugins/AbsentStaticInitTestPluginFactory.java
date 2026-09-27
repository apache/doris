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

package org.apache.doris.extension.loader.testplugins;

import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginFactory;

/**
 * This class defines cleanly - the omitted dependency appears only inside its static initializer,
 * whose first run is the loader's {@code newInstance()}. The failure therefore lands one step later
 * than {@link AbsentSuperclassTestPluginFactory}'s, at the other catch site.
 */
public class AbsentStaticInitTestPluginFactory implements PluginFactory {

    private static final Object MARKER = AbsentDependencyProbe.marker();

    @Override
    public String name() {
        return "absent-static-init-test";
    }

    @Override
    public String description() {
        return "Plugin whose static initializer reaches a class that is not on any classpath: " + MARKER;
    }

    @Override
    public Plugin create() {
        return new Plugin() {
        };
    }
}
