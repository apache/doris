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

package org.apache.doris.transaction;

import org.apache.doris.thrift.TErrorTabletInfo;

import com.google.common.collect.Lists;

import java.util.List;

public class ErrorTabletInfo {
    private long tabletId;
    private String msg;

    public ErrorTabletInfo(long tabletId, String msg) {
        this.tabletId = tabletId;
        this.msg = msg;
    }

    public long getTabletId() {
        return tabletId;
    }

    public String getMsg() {
        return msg;
    }

    public static List<ErrorTabletInfo> fromThrift(List<TErrorTabletInfo> errorTabletInfos) {
        List<ErrorTabletInfo> errorInfos = Lists.newArrayList();
        for (TErrorTabletInfo tErrorInfo : errorTabletInfos) {
            errorInfos.add(new ErrorTabletInfo(tErrorInfo.getTabletId(), tErrorInfo.getMsg()));
        }
        return errorInfos;
    }
}
