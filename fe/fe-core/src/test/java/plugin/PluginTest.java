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

package plugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginInfo;

public class PluginTest extends Plugin {

    private Map<String, String> map = new HashMap<>();

    @Override
    public void init(PluginInfo info, PluginContext ctx) {
        System.out.println("this is init");
    }


    @Override
    public void close() throws IOException {
        super.close();
        System.out.println("this is close");
    }

    @Override
    public int flags() {
        return 2;
    }

    @Override
    public void setVariable(String key, String value) {
        map.put(key, value);
    }

    @Override
    public Map<String, String> variable() {
        return map;
    }

    @Override
    public Map<String, String> status() {
        return new HashMap<>();
    }
}
