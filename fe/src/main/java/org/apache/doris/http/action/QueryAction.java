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

import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;

import com.google.common.base.Strings;

import io.netty.handler.codec.http.HttpMethod;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class QueryAction extends WebBaseAction {
    private static final Logger LOG = LogManager.getLogger(QueryAction.class);

    public QueryAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/query", new QueryAction(controller));
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());
        
        addFinishedQueryInfo(response.getContent());
        
        getPageFooter(response.getContent());
        
        writeResponse(request, response);
    }
    
    // Note: we do not show 'Query ID' column in web page
    private void addFinishedQueryInfo(StringBuilder buffer) {
        buffer.append("<h2>Finished Queries</h2>");
        buffer.append("<p>This table lists the latest 100 queries</p>");
        
        List<List<String>> finishedQueries = ProfileManager.getInstance().getAllQueries();
        List<String> columnHeaders = ProfileManager.PROFILE_HEADERS;
        int queryIdIndex = 0; // the first column is 'Query ID' by default
        for (int i = 0; i < columnHeaders.size(); ++i) {
            if (columnHeaders.get(i).equals(ProfileManager.QUERY_ID)) {
                queryIdIndex = i;
            }
        }
        appendFinishedQueryTableHeader(buffer, columnHeaders, queryIdIndex);
        appendFinishedQueryTableBody(buffer, finishedQueries, columnHeaders, queryIdIndex);
        appendTableFooter(buffer);
    }
    
    private void appendFinishedQueryTableHeader(
            StringBuilder buffer,
            final List<String> columnHeaders,
            int queryIdIndex) {
        buffer.append("<table " 
                + "class=\"table table-hover table-bordered table-striped table-hover\"><tr>");
        for (int i = 0; i < columnHeaders.size(); ++i) {
            if (i == queryIdIndex) {
                continue;
            }
            buffer.append("<th>" + columnHeaders.get(i) + "</th>");
        }
        
        buffer.append("<th>Profile</th>");
        buffer.append("</tr>");
    }
    
    private void appendFinishedQueryTableBody(
            StringBuilder buffer,
            List<List<String>> bodies,
            List<String> columnHeaders,
            int queryIdIndex) {
        for ( List<String> row : bodies) {
            buffer.append("<tr>");
            String queryId = row.get(queryIdIndex);
            
            for (int i = 0; i < row.size(); ++i) {
                if (i == queryIdIndex) {
                    continue;
                }
                buffer.append("<td>");
                buffer.append(row.get(i));
                buffer.append("</td>");
            }
            
            // add 'Profile' column
            if (Strings.isNullOrEmpty(queryId)) {
                LOG.warn("query id is null or empty, maybe we forget to push it "
                        + "into array when generate profile info.");
                buffer.append("<td>Empty Query ID</td>");
            } else {
                buffer.append("<td>");
                buffer.append("<a href=\"/query_profile?query_id="
                        + queryId
                        + "\">Profile</a>");
                buffer.append("</td>");
            }
            
            buffer.append("</tr>");
        }
    }
}
