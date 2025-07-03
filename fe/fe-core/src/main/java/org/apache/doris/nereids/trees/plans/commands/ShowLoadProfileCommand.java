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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.profile.ProfileManager;
import org.apache.doris.common.profile.ProfileManager.ProfileType;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;

/**
 * show load profile command
 */
public class ShowLoadProfileCommand extends ShowCommand {
    public static final ShowResultSetMetaData META_DATA_QUERY_IDS;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String key : SummaryProfile.SUMMARY_CAPTIONS) {
            if (key.equals(SummaryProfile.DISTRIBUTED_PLAN)) {
                continue;
            }
            builder.addColumn(new Column(key, ScalarType.createStringType()));
        }
        META_DATA_QUERY_IDS = builder.build();
    }

    private long limit = 20;

    /**
     * constructor
     */
    public ShowLoadProfileCommand(String path, long limit) {
        super(PlanType.SHOW_LOAD_PROFILE_COMMAND);
        this.limit = limit;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA_QUERY_IDS;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> rows = ProfileManager.getInstance().getProfileMetaWithType(ProfileType.LOAD, this.limit);
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowLoadProfileCommand(this, context);
    }
}
