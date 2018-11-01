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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.load.LoadErrorHub;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by lingbin on 17/4/14.
 */
public class LoadErrorProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Protocol").add("Host").add("Port")
            .add("User") .add("Password")
            .add("DB").add("Table")
            .build();

    private Catalog catalog;

    public LoadErrorProcNode(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public ProcResult fetchResult() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        LoadErrorHub.Param param = catalog.getLoadInstance().getLoadErrorHubInfo();
        if (param != null && param.getType() == LoadErrorHub.HubType.MYSQL_TYPE) {
            List<String> info = Lists.newArrayList();
            info.add(param.getType().toString());
            info.add(param.getMysqlParam().getHost());
            info.add(String.valueOf(param.getMysqlParam().getPort()));
            info.add(param.getMysqlParam().getUser());
            info.add(param.getMysqlParam().getPasswd());
            info.add(param.getMysqlParam().getDb());
            info.add(param.getMysqlParam().getTable());
            result.addRow(info);
        }

        return result;
    }
}

