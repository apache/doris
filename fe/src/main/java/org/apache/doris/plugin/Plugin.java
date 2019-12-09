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

package org.apache.doris.plugin;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public abstract class Plugin implements Closeable {
    public static final int PLUGIN_DEFAULT_FLAGS = 0;
    public static final int PLUGIN_NOT_DYNAMIC_INSTALL = 1;
    public static final int PLUGIN_NOT_DYNAMIC_UNINSTALL = 2;
    public static final int PLUGIN_INSTALL_EARLY = 4;

    /*
     * just one constructor
     *
     * public Plugin() {}
     *
     * public Plugin(Path installPath) {}
     */

    /**
     * invoke when the plugin install
     */
    public void init() { }

    /**
     * invoke when the plugin uninstall
     */
    @Override
    public void close() throws IOException { }

    public int flags() {
        return PLUGIN_DEFAULT_FLAGS;
    }

    public void setVariable(String key, String value) {

    }

    public Map<String, String> variable() {
        return Collections.EMPTY_MAP;
    }

    public Map<String, String> status() {
        return Collections.EMPTY_MAP;
    }
}
