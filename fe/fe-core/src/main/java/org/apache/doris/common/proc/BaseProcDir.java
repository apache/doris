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

package org.apache.doris.common.proc;

import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;


// 通用PROC DIR类，可以进行注册，返回底层节点内容。
// 非线程安全的，需要调用者考虑线程安全内容。
public class BaseProcDir implements ProcDirInterface {
    protected Map<String, ProcNodeInterface> nodeMap;

    public BaseProcDir() {
        nodeMap = Maps.newConcurrentMap();
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        nodeMap.put(name, node);
        return true;
    }

    @Override
    public ProcNodeInterface lookup(String name) {
        return nodeMap.get(name);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(Lists.newArrayList("name"));
        List<String> nameList = Lists.newArrayList(nodeMap.keySet());
        Collections.sort(nameList);
        for (String name : nameList) {
            result.addRow(Lists.newArrayList(name));
        }
        return result;
    }
}
