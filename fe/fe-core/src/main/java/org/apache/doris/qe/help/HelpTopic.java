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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

// Stolen from MySQL help_topic table.
// Used to store one help topic.
public class HelpTopic implements HelpObjectIface {
    public static final String DESCRIPTION = "description";
    public static final String EXAMPLE = "example";
    public static final String KEYWORDS = "keywords";

    private static final String URL = "url";
    private static final String CATEGORY = "category";
    private String name = "";
    private String description = "";
    private String example = "";
    private String url = "";
    private List<String> keywords = Lists.newArrayList();
    private String category = "";

    public String getCategory() {
        return category;
    }

    public List<String> getKeywords() {
        return keywords;
    }

    public String getUrl() {
        return url;
    }

    public String getExample() {
        return example;
    }

    public String getDescription() {
        return description;
    }

    public String getName() {
        return name;
    }

    @Override
    public void loadFrom(Map.Entry<String, Map<String, String>> doc) {
        // Name, must be not empty.
        if (Strings.isNullOrEmpty(doc.getKey())) {
            return;
        }
        name = doc.getKey().toUpperCase();
        // Description
        String desc = doc.getValue().get(DESCRIPTION);
        if (!Strings.isNullOrEmpty(desc)) {
            description = desc;
        }
        // Example
        String example = doc.getValue().get(EXAMPLE);
        if (!Strings.isNullOrEmpty(example)) {
            this.example = example;
        }
        // Url
        String url = doc.getValue().get(URL);
        if (!Strings.isNullOrEmpty(url)) {
            this.url = url;
        }
        // Keyword
        String keyword = doc.getValue().get(KEYWORDS);
        if (!Strings.isNullOrEmpty(keyword)) {
            List<String> keywordTexts = Lists.newArrayList(
                    Splitter.onPattern("\n").trimResults().omitEmptyStrings().split(keyword));
            for (String keywordText : keywordTexts) {
                if (keywordText.startsWith("```")) {
                    continue;
                }
                this.keywords.addAll(Lists.newArrayList(
                        Splitter.onPattern(",").trimResults().omitEmptyStrings().split(keywordText)));
            }
        }
        // Category
        String category = doc.getValue().get(CATEGORY);
        if (!Strings.isNullOrEmpty(category)) {
            this.category = category;
        }
    }

    @Override
    public String toString() {
        return "name: " + name + ", desc: " + description;
    }
}
