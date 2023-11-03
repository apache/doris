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

import org.apache.doris.common.io.Writable;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.nereids.jobs.load.InsertLoadTask;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.exception.JobException;

import lombok.extern.slf4j.Slf4j;

import java.io.DataOutput;
import java.io.IOException;

/**
 * we use this executor to execute sql job
 *
 */
@Slf4j
public class LoadTaskExecutor implements TransientTaskExecutor, Writable {

    protected String labelName;
    protected JobState state;

    public LoadTaskExecutor(ConnectContext ctx, InsertLoadTask task) {
        task.prepare();
    }

    @Override
    public Long getId() {
        return null;
    }

    @Override
    public void execute() throws JobException {
        task.

    }

    @Override
    public void cancel() throws JobException {

    }

    @Override
    public void write(DataOutput out) throws IOException {

    }
}
