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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Env;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.FutureTask;

public class AnalysisTaskWrapper extends FutureTask<Void> {

    private static final Logger LOG = LogManager.getLogger(AnalysisTaskWrapper.class);

    private final BaseAnalysisTask task;

    private long startTime;

    private final AnalysisTaskExecutor executor;

    public AnalysisTaskWrapper(AnalysisTaskExecutor executor, BaseAnalysisTask job) {
        super(() -> {
            job.execute();
            return null;
        });
        this.executor = executor;
        this.task = job;
    }

    @Override
    public void run() {
        startTime = System.currentTimeMillis();
        Throwable except = null;
        try {
            executor.putJob(this);
            super.run();
            Object result = get();
            if (result instanceof Throwable) {
                except = (Throwable) result;
            }
        } catch (Exception e) {
            except = e;
        } finally {
            executor.decr();
            if (except != null) {
                LOG.warn("Failed to execute task", except);
                Env.getCurrentEnv().getAnalysisManager()
                        .updateTaskStatus(task.info,
                                AnalysisState.FAILED, except.getMessage(), -1);
            } else {
                Env.getCurrentEnv().getAnalysisManager()
                        .updateTaskStatus(task.info,
                                AnalysisState.FINISHED, "", System.currentTimeMillis());
            }
            LOG.warn("{} finished, cost time:{}", task.toString(), System.currentTimeMillis() - startTime);
        }
    }

    public boolean cancel() {
        try {
            LOG.warn("{} cancelled, cost time:{}", task.toString(), System.currentTimeMillis() - startTime);
            task.cancel();
        } catch (Exception e) {
            LOG.warn(String.format("Cancel job failed job info : %s", task.toString()));
        } finally {
            executor.decr();
        }
        return super.cancel(false);
    }

    public long getStartTime() {
        return startTime;
    }
}
