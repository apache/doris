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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.thrift.TBrokerErrorHubInfo;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BrokerLoadErrorHub extends LoadErrorHub {

    private BrokerParam brokerParam;

    public BrokerLoadErrorHub(BrokerParam brokerParam) {
        this.brokerParam = brokerParam;
    }

    public BrokerParam getBrokerParam() {
        return brokerParam;
    }

    public static class BrokerParam implements Writable {
        private String brokerName;
        private String path;
        private Map<String, String> prop = Maps.newHashMap();

        // for persist
        public BrokerParam() {
        }

        public BrokerParam(String brokerName, String path, Map<String, String> prop) {
            this.brokerName = brokerName;
            this.path = path;
            this.prop = prop;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, brokerName);
            Text.writeString(out, path);
            out.writeInt(prop.size());
            for (Map.Entry<String, String> entry : prop.entrySet()) {
                Text.writeString(out, entry.getKey());
                Text.writeString(out, entry.getValue());
            }
        }

        public void readFields(DataInput in) throws IOException {
            brokerName = Text.readString(in);
            path = Text.readString(in);
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = Text.readString(in);
                String val = Text.readString(in);
                prop.put(key, val);
            }
        }

        public TBrokerErrorHubInfo toThrift() {
            FsBroker fsBroker = Catalog.getCurrentCatalog().getBrokerMgr().getAnyBroker(brokerName);
            if (fsBroker == null) {
                return null;
            }
            TBrokerErrorHubInfo info = new TBrokerErrorHubInfo(new TNetworkAddress(fsBroker.ip, fsBroker.port),
                    path, prop);
            return info;
        }

        public String getBrief() {
            Map<String, String> briefMap = Maps.newHashMap(prop);
            briefMap.put("name", brokerName);
            briefMap.put("path", path);
            PrintableMap<String, String> printableMap = new PrintableMap<>(briefMap, "=", true, false, true);
            return printableMap.toString();
        }
    }

    // Broker load error hub does not support showing detail error msg in 'show load warnings' stmt.
    // User need to file error file in remote storage with file name showed in 'show load' stmt
    @Override
    public List<ErrorMsg> fetchLoadError(long jobId) {
        List<ErrorMsg> result = Lists.newArrayList();
        final String hint = "Find detail load error info on '" 
                + brokerParam.path + "' with file name showed in 'SHOW LOAD' stmt";
        ErrorMsg errorMsg = new ErrorMsg(0, hint);
        result.add(errorMsg);
        return result;
    }

    @Override
    public boolean prepare() {
        return true;
    }

    @Override
    public boolean close() {
        return true;
    }

}
