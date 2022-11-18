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

package org.apache.doris.job;

import org.apache.doris.analysis.UserIdentity;

import java.util.Map;
import java.util.regex.Pattern;

public class MatchingPolicy {
    public static final String USER = "user";
    public static final String IP = "ip";

    private final String user;
    private final String ip;

    private final Pattern userPattern;
    private final Pattern ipPattern;

    public MatchingPolicy(String user, String ip) {
        this.user = user;
        this.ip = ip;
        userPattern = Pattern.compile(user.replaceAll("\\*", ".*"));
        ipPattern = Pattern.compile(ip.replaceAll("\\*", ".*"));
    }

    public MatchingPolicy(Map<String, String> polices) {
        this(polices.getOrDefault(USER, "root"), polices.getOrDefault(IP, "*"));
    }

    public boolean match(UserIdentity userIdentity) {
        return userPattern.matcher(userIdentity.getQualifiedUser()).matches() && ipPattern.matcher(
                userIdentity.getHost()).matches();
    }
}
