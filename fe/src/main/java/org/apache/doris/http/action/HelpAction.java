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

package org.apache.doris.http.action;

import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.qe.HelpModule;
import org.apache.doris.qe.HelpTopic;

import com.google.common.base.Strings;

import java.util.List;

import io.netty.handler.codec.http.HttpMethod;

public class HelpAction extends WebBaseAction {
    private static final String DIV_BACKGROUND_COLOR = "#FCFCFC";

    String queryString = null;
    
    public HelpAction(ActionController controller) {
        super(controller);
    }
    
    public static void registerAction (ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/help", new HelpAction(controller));
    }
    
    @Override
    public boolean needAdmin() {
        return false;
    }
    
    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());
        appendHelpStyle(response.getContent());
        
        queryString = request.getSingleParameter("query");
        if (Strings.isNullOrEmpty(queryString)) {
            // ATTN: according to Mysql protocol, the default query should be "contents" 
            //       when you want to get server side help.
            queryString = "contents";
        } else {
            queryString = queryString.trim();
        }
        appendHelpInfo(response.getContent());
        
        getPageFooter(response.getContent());
        writeResponse(request, response);
    }
    
    private void appendHelpInfo(StringBuilder buffer) {
        buffer.append("<h2>Help Info</h2>");
        buffer.append("<p>This page lists the help info, "
                + "like 'help contents' in Mysql client.</p>");
        
        appendSearchButton(buffer);
        appendExactMatchTopic(buffer);
        appendFuzzyMatchTopic(buffer);
        appendCategories(buffer);
    }
    
    private void appendSearchButton(StringBuilder buffer) {
        buffer.append("<form class=\"form-search\">"
                + "<div class=\"col-lg-3\" style=\"padding-left: 0px;\">"
                + "    <div class=\"input-group\">"
                + "        <input name = \"query\" type=\"text\" class=\"form-control\" placeholder=\"input here...\">"
                + "        <span class=\"input-group-btn\">"
                + "            <button class=\"btn btn-default\" type=\"submit\">Search</button>"
                + "        </span>"
                + "    </div>"
                + "</div>"
                + "<a href=\"/help\" class=\"btn btn-primary\">Back To Home</a>"
                + "</form>");
    }

    private void appendExactMatchTopic(StringBuilder buffer) {
        buffer.append("<h3>Exact Matching Topic</h3>");
        buffer.append("<div style=\"background-color:" + DIV_BACKGROUND_COLOR + ";"
                + "padding:0px, 1px, 1px, 0px;"
                + "\">");
        HelpModule module = HelpModule.getInstance();
        HelpTopic topic = module.getTopic(queryString);
        if (topic == null) {
            buffer.append("<pre>No Exact Matching Topic.</pre>");
        } else {
            appendOneTopicInfo(buffer, topic);
        }
        buffer.append("</div>");
    }

    private void appendFuzzyMatchTopic(StringBuilder buffer) {
        buffer.append("<h3>Fuzzy Matching Topic(By Keyword)</h3>");
        buffer.append("<div style=\"background-color:" + DIV_BACKGROUND_COLOR + ";"
                + "padding:0px, 1px, 1px, 0px;"
                + "\">");
        HelpModule module = HelpModule.getInstance();
        List<String> topics = module.listTopicByKeyword(queryString);
        if (topics.isEmpty()) {
            buffer.append("<pre>No Fuzzy Matching Topic.</pre>");
        } else if (topics.size() == 1) {
            buffer.append("<p class=\"text-info\"> "
                    + "Find only one topic, show you the detail info below.</p>");
            appendOneTopicInfo(buffer, module.getTopic(topics.get(0)));
        } else {
            buffer.append("<p class=\"text-info\"> Find " + topics.size() + " topics:</p>");
            appendNameList(buffer, topics, "Topics");
        }
        buffer.append("</div>");
    }

    private void appendCategories(StringBuilder buffer) {
        buffer.append("<h3>Category Info</h3>");
        buffer.append("<div style=\"background-color:" + DIV_BACKGROUND_COLOR + ";"
                + "padding:0px, 1px, 1px, 0px;"
                + "\">");
        HelpModule module = HelpModule.getInstance();
        List<String> categories = module.listCategoryByName(queryString);
        if (categories.isEmpty()) {
            buffer.append("<pre>No Matching Category.</pre>");
        } else if (categories.size() == 1) {
            buffer.append("<p class=\"text-info\"> "
                    + "Find only one category, so show you the detail info below. </p>");
            List<String> topics = module.listTopicByCategory(categories.get(0));
            if (topics.size() > 0) {
                buffer.append("<p class=\"text-info\"> Find " 
                        + topics.size()
                        + " sub topics. </p>");
                appendNameList(buffer, topics, "Sub Topics");
            }
            
            List<String> subCategories = module.listCategoryByCategory(categories.get(0));
            if (subCategories.size() > 0) {
                buffer.append("<p  class=\"text-info\"> Find "
                        + subCategories.size()
                        + " sub categories. </p>");
                appendNameList(buffer, subCategories, "Sub Categories");
            }
        } else {
            buffer.append("<p> Find " + categories.size() + " category: </p>");
            appendNameList(buffer, categories, "Categories");
        }
        buffer.append("</div>");
    }

    // The browser will combine continuous whitespace to one, we use <pre> tag to solve this issue.
    private void appendOneTopicInfo(StringBuilder buffer, HelpTopic topic) {
        buffer.append("<div style=\""
                + "padding:9.5px; "
                + "margin: 0 0 10px; "
                + "background-color: #f5f5f5;"
                + "border: 1px solid rgba(0, 0, 0, 0.15);"
                + "-webkit-border-radius: 4px;"
                + "-moz-border-radius: 4px;"
                + "border-radius: 4px;"
                + "font-family: Consolas;"
                + "\">");
        buffer.append("<h4>'" + escapeHtmlInPreTag(topic.getName()) + "'</h4>");
        
        buffer.append("<strong>Description</strong>");
        buffer.append("<pre class=\"topic_text\" style=\"border: 0px;\">" 
                + escapeHtmlInPreTag(topic.getDescription())
                + "</pre>");
        
        buffer.append("<strong>Example</strong>");
        buffer.append("<pre class=\"topic_text\" style=\"border: 0px\">" 
                + escapeHtmlInPreTag(topic.getExample()) 
                + "</pre>");
        
        buffer.append("<strong>Keyword</strong>");
        buffer.append("<pre class=\"topic_text\" style=\"border: 0px\">" 
                + escapeHtmlInPreTag(topic.getKeywords().toString()) 
                + "</pre>");
        
        buffer.append("<strong>Url</strong>");
        buffer.append("<pre class=\"topic_text\" style=\"border: 0px\">" 
                + escapeHtmlInPreTag(topic.getUrl()) 
                + "</pre>");
        buffer.append("</div>");
    }

    private void appendNameList(StringBuilder buffer, List<String> names, String tableHeadName) {
        buffer.append("<div style=\"padding:0px, 1px, 1px, 0px\">");
        buffer.append("<table " 
                + "class=\"table table-hover table-bordered table-striped table-hover\"><tr>");
        buffer.append("<th>" + tableHeadName + "</th>");
        
        final String href = "?query=";
        for (String name : names) {
            buffer.append("<tr><td><a href=\"" + href + name + "\">" + name + "</a><br/></td></tr>");
        }
        buffer.append("</table>");
        buffer.append("</div>");
    }
    
    private void appendHelpStyle(StringBuilder buffer) {
        buffer.append("<style type=\"text/css\">"
                + ".topic_text {"
                + "  font-family: \"Consolas\";"
                + "  margin-left: 5px;"
                + "}"
                + "</style>");
    }
}

