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
import org.apache.doris.thrift.TErrorHubType;
import org.apache.doris.thrift.TLoadErrorHubInfo;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class LoadErrorHub {
    private static final Logger LOG = LogManager.getLogger(LoadErrorHub.class);

    public static final String MYSQL_PROTOCOL = "MYSQL";
    public static final String BROKER_PROTOCOL = "BROKER";

    public static enum HubType {
        MYSQL_TYPE,
        BROKER_TYPE,
        NULL_TYPE
    }

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

    public static class Param implements Writable {
        private HubType type;
        private MysqlLoadErrorHub.MysqlParam mysqlParam;
        private BrokerLoadErrorHub.BrokerParam brokerParam;

        // for replay
        public Param() {
            type = HubType.NULL_TYPE;
        }

        public static Param createMysqlParam(MysqlLoadErrorHub.MysqlParam mysqlParam) {
            Param param = new Param();
            param.type = HubType.MYSQL_TYPE;
            param.mysqlParam = mysqlParam;
            return param;
        }

        public static Param createBrokerParam(BrokerLoadErrorHub.BrokerParam brokerParam) {
            Param param = new Param();
            param.type = HubType.BROKER_TYPE;
            param.brokerParam = brokerParam;
            return param;
        }

        public static Param createNullParam() {
            Param param = new Param();
            param.type = HubType.NULL_TYPE;
            return param;
        }

        public HubType getType() {
            return type;
        }

        public MysqlLoadErrorHub.MysqlParam getMysqlParam() {
            return mysqlParam;
        }

        public BrokerLoadErrorHub.BrokerParam getBrokerParam() {
            return brokerParam;
        }

        public String toString() {
            ToStringHelper helper = MoreObjects.toStringHelper(this);
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
                    info.setMysqlInfo(mysqlParam.toThrift());
                    break;
                case BROKER_TYPE:
                    info.setType(TErrorHubType.BROKER);
                    info.setBrokerInfo(brokerParam.toThrift());
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
                case BROKER_TYPE:
                    Preconditions.checkState(false, "hadoop load do not support broker error hub");
                case NULL_TYPE:
                    break;
                default:
                    Preconditions.checkState(false, "unknown hub type");
            }
            return dppHubInfo;
        }

        public List<String> getInfo() {
            List<String> info = Lists.newArrayList();
            info.add(type.name());
            switch (type) {
                case MYSQL_TYPE:
                    info.add(mysqlParam.getBrief());
                    break;
                case BROKER_TYPE:
                    info.add(brokerParam.getBrief());
                    break;
                default:
                    info.add("");
            }
            return info;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, type.name());
            switch (type) {
                case MYSQL_TYPE:
                    mysqlParam.write(out);
                    break;
                case BROKER_TYPE:
                    brokerParam.write(out);
                    break;
                case NULL_TYPE:
                    break;
                default:
                    Preconditions.checkState(false, "unknown hub type");
            }
        }

        public void readFields(DataInput in) throws IOException {
            type = HubType.valueOf(Text.readString(in));
            switch (type) {
                case MYSQL_TYPE:
                    mysqlParam = new MysqlLoadErrorHub.MysqlParam();
                    mysqlParam.readFields(in);
                    break;
                case BROKER_TYPE:
                    brokerParam = new BrokerLoadErrorHub.BrokerParam();
                    brokerParam.readFields(in);
                    break;
                case NULL_TYPE:
                    break;
                default:
                    Preconditions.checkState(false, "unknown hub type");
            }
        }
    }

    public abstract List<ErrorMsg> fetchLoadError(long jobId);

    public abstract boolean prepare();

    public abstract boolean close();

    public static LoadErrorHub createHub(Param param) {
        switch (param.getType()) {
            case MYSQL_TYPE: {
                LoadErrorHub hub = new MysqlLoadErrorHub(param.getMysqlParam());
                hub.prepare();
                return hub;
            }
            case BROKER_TYPE: {
                LoadErrorHub hub = new BrokerLoadErrorHub(param.getBrokerParam());
                hub.prepare();
                return hub;
            }
            default:
                Preconditions.checkState(false, "unknown hub type");
        }

        return null;
    }
}
