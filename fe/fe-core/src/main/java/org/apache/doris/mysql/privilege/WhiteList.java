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

package org.apache.doris.mysql.privilege;

import org.apache.doris.common.io.Text;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

//only for compatible version before VERSION_116
@Deprecated
public class WhiteList {
    private static final Logger LOG = LogManager.getLogger(WhiteList.class);

    private Map<String, byte[]> passwordMap = Maps.newConcurrentMap();

    public Map<String, byte[]> getPasswordMap() {
        return passwordMap;
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String domain = Text.readString(in);
            int passLen = in.readInt();
            byte[] password = new byte[passLen];
            in.readFully(password);
            passwordMap.put(domain, password);
        }
    }
}
