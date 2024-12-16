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

package org.apache.doris.common.util;

import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.util.TreeMap;

public class URI {
    private static final String SCHEME_DELIM = "://";
    private static final String AUTH_DELIM = "@";
    private static final String PATH_DELIM = "/";
    private static final String QUERY_DELIM = "\\?";
    private static final String FRAGMENT_DELIM = "#";
    private static final String PASS_DELIM = ":";
    private static final String PORT_DELIM = ":";
    private static final String QUERY_ITEM_DELIM = "&";
    private static final String QUERY_KV_DELIM = "=";

    @SerializedName("l")
    private final String location;
    private String scheme;
    private String authority;
    private String host;
    private int port = -1;
    private String userInfo;
    private String userName;
    private String passWord;
    private String path = "";
    private String query;
    private String fragment;
    private TreeMap<String, String> queryMap;

    private URI(String location) throws AnalysisException {
        if (Strings.isNullOrEmpty(location)) {
            throw new AnalysisException("location can not be null");
        }
        this.location = location.trim();
        parse();
    }

    public static URI create(String location) throws AnalysisException {
        return new URI(location);
    }

    public String getLocation() {
        return location;
    }

    public String getScheme() {
        return scheme;
    }

    public String getAuthority() {
        return authority;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUserInfo() {
        return userInfo;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassWord() {
        return passWord;
    }

    public String getPath() {
        return path;
    }

    public String getQuery() {
        return query;
    }

    public String getFragment() {
        return fragment;
    }

    public TreeMap<String, String> getQueryMap() {
        return queryMap;
    }

    private void parse() throws AnalysisException {
        String[] schemeSplit = this.location.split(SCHEME_DELIM);
        String hierarchical;
        if (schemeSplit.length == 1) {
            hierarchical = schemeSplit[0];
        } else if (schemeSplit.length == 2) {
            scheme = schemeSplit[0];
            hierarchical = schemeSplit[1];
        } else {
            throw new AnalysisException("Invalid uri: " + this.location);
        }

        String[] authoritySplit = hierarchical.split(PATH_DELIM, 2);
        String hostPortPath;
        if (authoritySplit.length == 1) {
            if (StringUtils.isBlank(scheme)) {
                hostPortPath = authoritySplit[0];
            } else {
                authority = authoritySplit[0];
                parseAuthority(authority);
                return;
            }
        } else if (authoritySplit.length == 2) {
            authority = authoritySplit[0];
            parseAuthority(authority);
            hostPortPath = authoritySplit[1];
        } else {
            throw new AnalysisException("Invalid uri: " + this.location);
        }
        String[] fragSplit = hostPortPath.split(FRAGMENT_DELIM);
        if (fragSplit.length == 2) {
            fragment = fragSplit[1];
        }
        String[] querySplit = fragSplit[0].split(QUERY_DELIM);
        if (querySplit.length == 1) {
            path =  StringUtils.isBlank(host) ? querySplit[0] : PATH_DELIM + querySplit[0];
        } else if (querySplit.length == 2) {
            path = StringUtils.isBlank(host) ? querySplit[0] : PATH_DELIM + querySplit[0];
            query = querySplit[1];
            parseQuery();
        } else {
            throw new AnalysisException("Invalid path: " + this.location);
        }

    }

    private void parseQuery() {
        if (StringUtils.isBlank(query)) {
            return;
        }
        String[] split = query.split(QUERY_ITEM_DELIM);
        queryMap = new TreeMap<>();
        for (String item : split) {
            String[] itemSplit = item.split(QUERY_KV_DELIM);
            if (itemSplit.length == 2) {
                queryMap.put(itemSplit[0], itemSplit[1]);
            }
        }

    }

    private void parseAuthority(String str) throws AnalysisException {
        if (StringUtils.isBlank(str)) {
            return;
        }
        String[] authSplit = str.split(AUTH_DELIM);
        String hostPort;
        if (authSplit.length == 1) {
            hostPort = authSplit[0];
        } else if (authSplit.length == 2) {
            userInfo = authSplit[0];
            hostPort = authSplit[1];
            String[] userSplit = userInfo.split(PASS_DELIM);
            if (userSplit.length == 1) {
                userName = userInfo;
            } else if (userSplit.length == 2) {
                userName = userSplit[0];
                passWord = userSplit[1];
            } else {
                throw new AnalysisException("Invalid userinfo: " + userInfo);
            }
        } else {
            throw new AnalysisException("Invalid authority: " + str);
        }
        String[] hostSplit = hostPort.split(PORT_DELIM);
        if (hostSplit.length == 1) {
            host = hostSplit[0];
        } else if (hostSplit.length == 2) {
            host = hostSplit[0];
            port = Integer.parseInt(hostSplit[1]);
        } else {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }
    }

    @Override
    public String toString() {
        return location;
    }
}
