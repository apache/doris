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

package org.apache.doris.scheduler.executor;

import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.nereids.jobs.load.InsertLoadTask;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TRow;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * we use this executor to execute sql job
 *
 */
@Slf4j
public class TvfLoadTask extends AbstractTask {

    protected InsertLoadTask task;
    protected ConnectContext ctx;
    protected StmtExecutor executor;

    public TvfLoadTask(ConnectContext ctx, StmtExecutor executor, InsertLoadTask task) {
        this.ctx = ctx;
        this.executor = executor;
        this.task = task;
    }

    @Override
    public void run() throws JobException {
        task.run(executor, ctx);
        task.onFinished();
    }

    @Override
    public void cancel() throws JobException {

    }

    @Override
    public List<String> getShowInfo() {
        return null;
    }

    @Override
    public TRow getTvfInfo() {
        return null;
    }
}
