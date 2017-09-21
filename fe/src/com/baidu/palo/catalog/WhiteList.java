// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.catalog;

import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WhiteList implements Writable {
    private static final Logger LOG = LogManager.getLogger(WhiteList.class);

    // Ip (123.123.1.1)
    protected Set<String> ipWhiteLists = Sets.newHashSet();
    // startIp (123.123.*.*)
    protected Set<String> starIpWhiteLists = Sets.newHashSet();
    // hostName(www.baidu.com), which need to dns analysis
    protected Set<String> hostWhiteLists = Sets.newHashSet();
    protected Map<String, Set<String>> ipOfHostWhiteLists;
    private String user;

    public WhiteList() {
    }

    // for limit the max whiteListsize
    public int getSize() {
        return ipWhiteLists.size() + starIpWhiteLists.size() + hostWhiteLists.size();
    }

    public boolean hasAccess(String ip) {
        // whileList is null, all people can access
        if (getSize() == 0) {
            return true;
        }

        // 1. check if specified ip in white list
        if (ipWhiteLists.contains(ip)) {
            return true;
        }

        // 2. check if specified ip in start white list
        for (String starIp : starIpWhiteLists) {
            String[] splittedStarIp = starIp.split("\\.");
            String[] splittedSpecifiedIp = ip.split("\\.");
            int starIpLen = splittedStarIp.length;
            int specifiedIpLen = splittedSpecifiedIp.length;
            if (!(specifiedIpLen == 4 && starIpLen == 4)) {
                String msg = String.format("Invalid IP format: %s", ip);
                LOG.warn(msg);
                throw new RuntimeException(msg);
            }

            boolean hit = true;
            for (int i = 0; i < 4; i++) {
                if (splittedSpecifiedIp[i].equals(splittedStarIp[i])) {
                    continue;
                } else if (splittedStarIp[i].equals("*")) {
                    continue;
                } else {
                    hit = false;
                    break;
                }
            }

            if (hit) {
                return true;
            }
        }

        ipOfHostWhiteLists = DomainParserServer.getInstance().getUserHostIp(user);
        // 3. check ipWhiteList
        if (ipOfHostWhiteLists != null) {
            for (String entryIp : ipOfHostWhiteLists.keySet()) {
                Set<String> ipSet = ipOfHostWhiteLists.get(entryIp);
                if (ipSet == null || ipSet.size() == 0) {
                    LOG.warn("dns error ip={}", entryIp);
                    continue;
                }
                if (ipSet.contains(ip)) {
                    return true;
                }
            }
        }
        LOG.warn("can't match whitelist ip={}", ip);
        return false;
    }

    public void addWhiteList(List<String> ips, List<String> starIps, List<String> hosts) throws DdlException {
        ipWhiteLists.addAll(ips);
        starIpWhiteLists.addAll(starIps);
        hostWhiteLists.addAll(hosts);
        DomainParserServer.getInstance().register(user, hosts);
    }

    public void deleteWhiteList(List<String> ips, List<String> starIps, List<String> hosts) {
        if (ips != null && ips.size() > 0) {
            ipWhiteLists.removeAll(ips);
        }
        if (starIps != null && starIps.size() > 0) {
            starIpWhiteLists.removeAll(starIps);
        }
        if (hosts != null && hosts.size() > 0) {
            hostWhiteLists.removeAll(hosts);
        }
        if (hosts != null && hosts.size() > 0) {
            DomainParserServer.getInstance().unregister(user, hosts);
        }

    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (String ip : ipWhiteLists) {
            builder.append(ip);
            builder.append(",");
        }
        for (String ip : starIpWhiteLists) {
            builder.append(ip);
            builder.append(",");
        }
        for (String ip : hostWhiteLists) {
            builder.append(ip);
            builder.append(",");
        }
        String result = builder.toString();
        String newResult = result;
        // del the last ,
        if (result.length() > 0) {
            newResult = result.substring(0, result.length() - 1);
        }
        return newResult;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(ipWhiteLists.size());
        for (String ip : ipWhiteLists) {
            Text.writeString(out, ip);
        }
        out.writeInt(starIpWhiteLists.size());
        for (String ip : starIpWhiteLists) {
            Text.writeString(out, ip);
        }
        out.writeInt(hostWhiteLists.size());
        for (String ip : hostWhiteLists) {
            Text.writeString(out, ip);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int ipWhiteListsLen = in.readInt();
        for (int i = 0; i < ipWhiteListsLen; i++) {
            String ip = Text.readString(in);
            ipWhiteLists.add(ip);
        }
        int starIpWhiteListsLen = in.readInt();
        for (int i = 0; i < starIpWhiteListsLen; i++) {
            String ip = Text.readString(in);
            starIpWhiteLists.add(ip);
        }
        int hostWhiteListsLen = in.readInt();
        for (int i = 0; i < hostWhiteListsLen; i++) {
            String ip = Text.readString(in);
            hostWhiteLists.add(ip);
        }

        if (hostWhiteLists != null && hostWhiteLists.size() > 0) {
            DomainParserServer.getInstance().register(user, hostWhiteLists);
        }
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
