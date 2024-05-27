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

package org.apache.doris.qe.help;

import com.google.common.base.Strings;

import java.util.Map;

// HelpCategory is category of help information.
public class HelpCategory implements HelpObjectIface {
    private static final String PARENT = "parent";
    private static final String URL = "url";
    private String name = "";
    private String parent = "";
    private String url = "";

    public String getName() {
        return name;
    }

    public String getParent() {
        return parent;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public void loadFrom(Map.Entry<String, Map<String, String>> doc) {
        // Name, must be not empty.
        if (Strings.isNullOrEmpty(doc.getKey())) {
            return;
        }
        name = doc.getKey();
        // Parent
        String parent = doc.getValue().get(PARENT);
        if (!Strings.isNullOrEmpty(parent)) {
            this.parent = parent;
        }
        // Example
        String url = doc.getValue().get(URL);
        if (!Strings.isNullOrEmpty(url)) {
            this.url = url;
        }
    }
}
