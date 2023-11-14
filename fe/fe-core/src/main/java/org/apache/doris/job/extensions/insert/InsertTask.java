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

package org.apache.doris.job.extensions.insert;

import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;

import lombok.extern.slf4j.Slf4j;

/**
 * todo implement this later
 */
@Slf4j
public class InsertTask extends AbstractTask {

    private String labelName;

    private InsertIntoTableCommand command;
    private LoadJob.LoadStatistic statistic;
    private FailMsg failMsg;

    private InsertIntoState insertIntoState;

    @Override
    public void before() {
        super.before();
    }

    public InsertTask(String labelName, InsertIntoTableCommand command, LoadJob.LoadStatistic statistic,
                      FailMsg failMsg, InsertIntoState insertIntoState) {
        this.labelName = labelName;
        this.command = command;
        this.statistic = statistic;
        this.failMsg = failMsg;
        this.insertIntoState = insertIntoState;
    }

    @Override
    public void run() {
        //just for test
        log.info(getJobId() + "InsertTask run" + TimeUtils.longToTimeString(System.currentTimeMillis()));
    }

    @Override
    public void onFail() {
        super.onFail();
    }

    @Override
    public void onSuccess() {
        super.onSuccess();
    }

    @Override
    public void cancel() {
        super.cancel();
    }

}
