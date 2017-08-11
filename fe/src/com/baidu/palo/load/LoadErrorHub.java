// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.load;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.thrift.TErrorHubType;
import com.baidu.palo.thrift.TLoadErrorHubInfo;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;

public abstract class LoadErrorHub {
    private static final Logger LOG = LogManager.getLogger(LoadErrorHub.class);

    public static final String MYSQL_PROTOCOL = "MYSQL";

    private static final String DEFAULT_TABLE = "load_errors";

    public class ErrorMsg {
        private long jobId;
        private String msg;

        public ErrorMsg(long id, String message) {
            jobId = id;
            msg = message;
        }

        public long getJobId() {
            return jobId;
        }

        public String getMsg() {
            return msg;
        }
    }

    // we only support mysql for now
    public static enum HubType {
        MYSQL_TYPE,
        NULL_TYPE
    }

    public static class Param implements Writable {
        private HubType type;
        private MysqlLoadErrorHub.MysqlParam mysqlParam;

        // for replay
        public Param() {
            type = HubType.NULL_TYPE;
        }

        public Param(HubType type, MysqlLoadErrorHub.MysqlParam mysqlParam) {
            this.type = type;
            this.mysqlParam = mysqlParam;
        }

        public HubType getType() {
            return type;
        }

        public MysqlLoadErrorHub.MysqlParam getMysqlParam() {
            return mysqlParam;
        }

        public String toString() {
            Objects.ToStringHelper helper = Objects.toStringHelper(this);
            helper.add("type", type.toString());
            switch (type) {
                case MYSQL_TYPE:
                    helper.add("mysql_info", mysqlParam.toString());
                    break;
                case NULL_TYPE:
                    helper.add("mysql_info", "null");
                    break;
                default:
                    Preconditions.checkState(false, "unknown hub type");
            }

            return helper.toString();
        }

        public TLoadErrorHubInfo toThrift() {
            TLoadErrorHubInfo info = new TLoadErrorHubInfo();
            switch (type) {
                case MYSQL_TYPE:
                    info.setType(TErrorHubType.MYSQL);
                    info.setMysql_info(mysqlParam.toThrift());
                    break;
                case NULL_TYPE:
                    info.setType(TErrorHubType.NULL_TYPE);
                    break;
                default:
                    Preconditions.checkState(false, "unknown hub type");
            }
            return info;
        }

        public Map<String, Object> toDppConfigInfo() {
            Map<String, Object> dppHubInfo = Maps.newHashMap();

            dppHubInfo.put("type", type.toString());
            switch (type) {
                case MYSQL_TYPE:
                    dppHubInfo.put("info", mysqlParam);
                    break;
                case NULL_TYPE:
                    break;
                default:
                    Preconditions.checkState(false, "unknown hub type");
            }
            return dppHubInfo;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, type.name());
            switch (type) {
                case MYSQL_TYPE:
                    mysqlParam.write(out);
                    break;
                case NULL_TYPE:
                    break;
                default:
                    Preconditions.checkState(false, "unknown hub type");
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            type = HubType.valueOf(Text.readString(in));
            switch (type) {
                case MYSQL_TYPE:
                    mysqlParam = new MysqlLoadErrorHub.MysqlParam();
                    mysqlParam.readFields(in);
                    break;
                case NULL_TYPE:
                    break;
                default:
                    Preconditions.checkState(false, "unknown hub type");
            }
        }
    }

    public abstract ArrayList<ErrorMsg> fetchLoadError(long jobId);

    public abstract boolean prepare();

    public abstract boolean close();

    public static LoadErrorHub createHub(Param param) {
        switch (param.getType()) {
            case MYSQL_TYPE:
                LoadErrorHub hub = new MysqlLoadErrorHub(param.getMysqlParam());
                hub.prepare();
                return hub;
            default:
                Preconditions.checkState(false, "unknown hub type");
        }

        return null;
    }

    public static Param analyzeUrl(String url) throws AnalysisException {
        String protocol = null;
        String host = null;
        int port = 0;
        String user = "";
        String passwd = "";
        String db = null;
        String table = null;

        String userInfo = null;
        String path = null;
        try {
            URI uri = new URI(url);
            protocol = uri.getScheme();
            host = uri.getHost();
            port = uri.getPort();
            userInfo = uri.getUserInfo();
            path = uri.getPath();
        } catch (URISyntaxException e) {
            new AnalysisException(e.getMessage());
        }

        LOG.debug("debug: protocol={}, host={}, port={}, userInfo={}, path={}", protocol, host, port, userInfo, path);

        // protocol
        HubType hubType = HubType.NULL_TYPE;
        if (!Strings.isNullOrEmpty(protocol)) {
            if (protocol.equalsIgnoreCase("mysql")) {
                hubType = HubType.MYSQL_TYPE;
            } else {
                throw new AnalysisException("error protocol: " + protocol);
            }
        }

        // host
        try {
            // validate host
            if (!InetAddressValidator.getInstance().isValid(host)) {
                // maybe this is a hostname
                // if no IP address for the host could be found, 'getByName' will throw
                // UnknownHostException
                InetAddress inetAddress = null;
                inetAddress = InetAddress.getByName(host);
                host = inetAddress.getHostAddress();
            }
        } catch (UnknownHostException e) {
            throw new AnalysisException("host is invalid: " + host);
        }

        // port
        if (port <= 0 || port >= 65536) {
            throw new AnalysisException("Port is out of range: " + port);
        }

        // user && password
        // Note:
        //  1. user cannot contain ':' character, but password can.
        //  2. user cannot be empty, but password can.
        // valid sample:
        //   user:pwd   user   user:    user:pw:d
        // invalid sample:
        //   :pwd    --- not valid
        if (Strings.isNullOrEmpty(userInfo) || userInfo.startsWith(":")) {
            throw new AnalysisException("user:passwd is wrong: [" + userInfo + "]");
        } else {
            // password may contain ":", so we split as much as 2 parts.
            String[] parts = userInfo.split(":", 2);
            if (parts.length == 1) {
                // "passwd:"
                user = parts[0];
                passwd = "";
            } else if (parts.length == 2) {
                user = parts[0];
                passwd = parts[1];
            }
        }

        // db && table
        path = trimChar(path, '/');
        LOG.debug("debug: path after trim = [{}]", path);
        if (!Strings.isNullOrEmpty(path)) {
            String[] parts = path.split("/");
            if (parts.length == 1) {
                db = path;
                table = DEFAULT_TABLE;
            } else if (parts.length == 2) {
                db = parts[0];
                table = parts[1];
            } else {
                throw new AnalysisException("path is wrong: [" + path + "]");
            }
        } else {
            db = String.valueOf(Catalog.getInstance().getClusterId());
            table = DEFAULT_TABLE;
        }

        MysqlLoadErrorHub.MysqlParam mysqlParam = new MysqlLoadErrorHub.MysqlParam(host, port, user, passwd, db, table);
        Param param = new Param(hubType, mysqlParam);
        return param;
    }

    // remove specified char at the leading and trailing。
    private static String trimChar(String str, char c) {
        if (Strings.isNullOrEmpty(str)) {
            return "";
        }

        int beginIdx = 0;
        for (int i = 0; i < str.length(); ++i) {
            if (str.charAt(i) == c) {
                beginIdx = i;
            } else {
                break;
            }
        }

        if (beginIdx == str.length() - 1) {
            return "";
        }

        int endIdx = str.length();
        for (int i = str.length() - 1; i > beginIdx; --i) {
            if (str.charAt(i) == c) {
                endIdx = i;
            } else {
                break;
            }
        }

        return str.substring(beginIdx + 1, endIdx);
    }
}
