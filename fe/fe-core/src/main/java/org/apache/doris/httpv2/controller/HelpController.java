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

package org.apache.doris.httpv2.controller;

import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.qe.HelpModule;
import org.apache.doris.qe.HelpTopic;

import com.google.common.base.Strings;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/rest/v1")
public class HelpController {

    @RequestMapping(path = "/help", method = RequestMethod.GET)
    public Object helpSearch(HttpServletRequest request) {
        String queryString = request.getParameter("query");
        if (Strings.isNullOrEmpty(queryString)) {
            // ATTN: according to Mysql protocol, the default query should be "contents"
            //       when you want to get server side help.
            queryString = "contents";
        } else {
            queryString = queryString.trim();
        }
        Map<String, Object> result = new HashMap<>();
        appendHelpInfo(result, queryString);
        return ResponseEntityBuilder.ok(result);
    }

    private void appendHelpInfo(Map<String, Object> result, String queryString) {
        appendExactMatchTopic(result, queryString);
        appendFuzzyMatchTopic(result, queryString);
        appendCategories(result, queryString);
    }

    private void appendExactMatchTopic(Map<String, Object> result, String queryString) {
        HelpModule module = HelpModule.getInstance();
        HelpTopic topic = module.getTopic(queryString);
        if (topic == null) {
            result.put("matching", "No Exact Matching Topic.");
        } else {
            Map<String, Object> subMap = new HashMap<>();
            appendOneTopicInfo(subMap, topic, "matching");
            result.put("matchingTopic", subMap);
        }
    }

    private void appendFuzzyMatchTopic(Map<String, Object> result, String queryString) {
        HelpModule module = HelpModule.getInstance();
        List<String> topics = module.listTopicByKeyword(queryString);
        if (topics.isEmpty()) {
            result.put("fuzzy", "No Fuzzy Matching Topic");
        } else if (topics.size() == 1) {
            result.put("fuzzy", "Find only one topic, show you the detail info below");
            Map<String, Object> subMap = new HashMap<>();
            appendOneTopicInfo(subMap, module.getTopic(topics.get(0)), "fuzzy");
            result.put("fuzzyTopic", subMap);
        } else {
            result.put("size", topics.size());
            result.put("datas", topics);
        }
    }

    private void appendCategories(Map<String, Object> result, String queryString) {
        HelpModule module = HelpModule.getInstance();
        List<String> categories = module.listCategoryByName(queryString);
        if (categories.isEmpty()) {
            result.put("matching", "No Matching Category");
        } else if (categories.size() == 1) {
            result.put("matching", "Find only one category, so show you the detail info below");
            List<String> topics = module.listTopicByCategory(categories.get(0));

            if (topics.size() > 0) {
                List<Map<String, String>> topicList = new ArrayList<>();
                result.put("topicSize", topics.size());
                for (String topic : topics) {
                    Map<String, String> top = new HashMap<>();
                    top.put("name", topic);
                    topicList.add(top);
                }
                result.put("topicdatas", topicList);
            }

            List<String> subCategories = module.listCategoryByCategory(categories.get(0));
            if (subCategories.size() > 0) {
                List<Map<String, String>> subCate = new ArrayList<>();
                result.put("subCateSize", subCategories.size());
                for (String sub : subCategories) {
                    Map<String, String> subMap = new HashMap<>();
                    subMap.put("name", sub);
                    subCate.add(subMap);
                }
                result.put("subdatas", subCate);
            }
        } else {
            List<Map<String, String>> categoryList = new ArrayList<>();
            if (categories.size() > 0) {
                result.put("categoriesSize", categories.size());
                for (String cate : categories) {
                    Map<String, String> subMap = new HashMap<>();
                    subMap.put("name", cate);
                    categoryList.add(subMap);
                }
                result.put("categoryDatas", categoryList);
            }
        }
    }

    // The browser will combine continuous whitespace to one, we use <pre> tag to solve this issue.
    private void appendOneTopicInfo(Map<String, Object> result, HelpTopic topic, String prefix) {
        result.put(prefix + "topic", escapeHtmlInPreTag(topic.getName()));
        result.put(prefix + "description", escapeHtmlInPreTag(topic.getDescription()));
        result.put(prefix + "example", escapeHtmlInPreTag(topic.getExample()));
        result.put(prefix + "Keyword", escapeHtmlInPreTag(topic.getKeywords().toString()));
        result.put(prefix + "Url", escapeHtmlInPreTag(topic.getUrl()));
    }

    protected String escapeHtmlInPreTag(String oriStr) {
        if (oriStr == null) {
            return "";
        }
        String content = oriStr.replaceAll("\n", "</br>");
        return content;
    }

}
