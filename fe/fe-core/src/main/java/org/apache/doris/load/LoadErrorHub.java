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

package org.apache.doris.load;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public abstract class LoadErrorHub {

    public static class MysqlParam {
        @SerializedName("h")
        private String host;
        @SerializedName("p")
        private int port;
        @SerializedName("u")
        private String user;
        @SerializedName("pwd")
        private String passwd;
        @SerializedName("db")
        private String db;
        @SerializedName("tb")
        private String table;

        public MysqlParam() {
            host = "";
            port = 0;
            user = "";
            passwd = "";
            db = "";
            table = "";
        }

        public MysqlParam(String host, int port, String user, String passwd, String db, String table) {
            this.host = host;
            this.port = port;
            this.user = user;
            this.passwd = passwd;
            this.db = db;
            this.table = table;
        }

        public String getBrief() {
            Map<String, String> briefMap = Maps.newHashMap();
            briefMap.put("host", host);
            briefMap.put("port", String.valueOf(port));
            briefMap.put("user", user);
            briefMap.put("password", passwd);
            briefMap.put("database", db);
            briefMap.put("table", table);
            PrintableMap<String, String> printableMap = new PrintableMap<>(briefMap, "=", true, false, true);
            return printableMap.toString();
        }
    }

    public static class BrokerParam {
        @SerializedName("b")
        private String brokerName;
        @SerializedName("pa")
        private String path;
        @SerializedName("pr")
        private Map<String, String> prop = Maps.newHashMap();

        // for persist
        public BrokerParam() {
        }

        public BrokerParam(String brokerName, String path, Map<String, String> prop) {
            this.brokerName = brokerName;
            this.path = path;
            this.prop = prop;
        }
    }

    public static final String MYSQL_PROTOCOL = "MYSQL";
    public static final String BROKER_PROTOCOL = "BROKER";

    public static enum HubType {
        MYSQL_TYPE,
        BROKER_TYPE,
        NULL_TYPE
    }

    public static class Param implements Writable {
        @SerializedName(value = "t")
        private HubType type;
        @SerializedName(value = "m")
        private MysqlParam mysqlParam;
        @SerializedName(value = "b")
        private BrokerParam brokerParam;

        // for replay
        public Param() {
            type = HubType.NULL_TYPE;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }

        public static Param read(DataInput in) throws IOException {
            return GsonUtils.GSON.fromJson(Text.readString(in), Param.class);
        }
    }
}
