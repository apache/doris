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

package org.apache.doris.analysis;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterUserClause extends AlterClause {
    private static final Logger LOG = LogManager.getLogger(AlterUserClause.class);
    private List<String> hostOrIps;
    
    private List<String> ips; // for 123.321.1.1
    private List<String> starIps; // for 123.*.*.*
    private List<String> hosts;   // for www.baidu.com
    private AlterUserType type;
    
    public AlterUserClause(AlterUserType type, List<String> hostOrIps) {
        super(AlterOpType.ALTER_OTHER);
        this.type = type;
        this.hostOrIps = hostOrIps;
        this.ips = Lists.newArrayList();
        this.starIps = Lists.newArrayList();
        this.hosts = Lists.newArrayList();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(type);
        for (int i = 0; i < hostOrIps.size(); i++) {
            sb.append("\"").append(hostOrIps.get(i)).append("\"");
            if (i != hostOrIps.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
    
    private boolean isHostName(String host) throws AnalysisException {
        if (Strings.isNullOrEmpty(host)) {
            throw new AnalysisException("host=[" + host + "] is empty");
        }

        for (char ch : host.toCharArray()) {
            if (Character.isLetter(ch)) {
                return true;
            }
        }
        String[] ipArray = host.split("\\.");
        if (ipArray.length != 4) {
            String msg = "ip wrong, ip=" + host;
            LOG.warn("{}", msg);
            throw new AnalysisException(msg);
        }
        return false;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        // 1. del duplicate
        Set<String> sets = Sets.newHashSet(hostOrIps);

        // 2. extract hosts and realIps from hostOrIp
        for (String host : sets) {
            if (isHostName(host)) {
                // may be bns or hostname
                hosts.add(host);
            } else if (host.contains("*")) {
                starIps.add(host);
            } else {
                ips.add(host);
            }
        }
        // NOTICE: if we del hostname from whiteList, the hostname must be totally equal with catalog's hostname;
    }
    
    public List<String> getIps() {
        return ips;
    }
    
    public List<String> getStarIps() {
        return starIps;
    } 
    
    public List<String> getHosts() {
        return hosts;
    }
    
    public AlterUserType getAlterUserType() {
        return type;
    }

    @Override
    public Map<String, String> getProperties() {
        throw new NotImplementedException();
    }
}
