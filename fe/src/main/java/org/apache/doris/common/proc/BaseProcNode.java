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

import java.util.List;

public class BaseProcNode implements ProcNodeInterface {
    private ProcResult result;

    // 为只有一个值的构造函数
    public BaseProcNode(String val) {
        BaseProcResult result = new BaseProcResult();
        result.setNames(Lists.newArrayList("value"));
        result.addRow(Lists.newArrayList(val));
        this.result = result;
    }

    public BaseProcNode(List<String> col, List<List<String>> val) {
        this.result = new BaseProcResult(col, val);
    }

    public BaseProcNode(ProcResult result) {
        this.result = result;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        return result;
    }
}
