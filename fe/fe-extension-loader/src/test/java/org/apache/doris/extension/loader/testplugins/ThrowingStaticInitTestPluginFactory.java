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
 * This class defines cleanly and its static initializer throws a plain {@link IllegalStateException}:
 * the "keytab not found" shape of a plugin that is installed but misconfigured. Being a non-Error, the
 * JVM wraps it in {@link ExceptionInInitializerError}, whose own message is null - so unless the loader
 * carries the cause into the failure message, the operator is left with the bare class name.
 */
public class ThrowingStaticInitTestPluginFactory implements PluginFactory {

    public static final String REASON = "keytab not found: /etc/doris/plugin.keytab";

    private static final Object MARKER = fail();

    private static Object fail() {
        throw new IllegalStateException(REASON);
    }

    @Override
    public String name() {
        return "throwing-static-init-test";
    }

    @Override
    public String description() {
        return "Plugin whose static initializer throws: " + MARKER;
    }

    @Override
    public Plugin create() {
        return new Plugin() {
        };
    }
}
