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

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.common.proc.BackendsProcDir;
import com.baidu.palo.qe.ShowResultSetMetaData;

public class ShowBackendsStmt extends ShowStmt {

    public ShowBackendsStmt() {  
    }
    
    @Override
    public ShowResultSetMetaData getMetaData() {
         ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
         for (String title : BackendsProcDir.TITLE_NAMES) {
             if (title.equals("HostName") || title.equals("HeartbeatPort") 
                     || title.equals("BePort") || title.equals("HttpPort")) {
                 continue;
             }
            builder.addColumn(new Column(title, ColumnType.createVarchar(30)));
        }
        return builder.build();
    }
}
