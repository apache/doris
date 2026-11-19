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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.mtmv.MTMVTask;

import com.google.common.base.Strings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IvmRefreshSummaryTest {

    /**
     * Consecutive failures are the failed tasks since the latest successful one: a success clears them, including one
     * that had nothing to commit, and a canceled task leaves them as they are.
     */
    @Test
    public void testConsecutiveFailuresAreTheFailuresSinceTheLatestSuccess() {
        IvmRefreshSummary summary = new IvmRefreshSummary();

        summary.update(failed(1000L, 2000L, IvmRefreshFailureStage.EXECUTION_FAILED.name(), "first"));
        summary.update(failed(3000L, 4000L, IvmRefreshFailureStage.EXECUTION_FAILED.name(), "second"));
        Assertions.assertEquals(2, summary.getConsecutiveFailures());
        summary.update(canceled());
        Assertions.assertEquals(2, summary.getConsecutiveFailures());

        summary.update(succeeded(5000L, 6000L, IvmRefreshMethod.NONE));
        Assertions.assertEquals(0, summary.getConsecutiveFailures());
        summary.update(failed(7000L, 8000L, IvmRefreshFailureStage.OTHER.name(), "third"));
        Assertions.assertEquals(1, summary.getConsecutiveFailures());
    }

    /**
     * The latest failure keeps its time, reason and message, and the message is cut to a bounded length; the latest
     * success does not clear them. How the latest task refreshed the MV, and how long it took, follow every task that
     * succeeded or failed.
     */
    @Test
    public void testTheLatestSuccessAndFailureAreKept() {
        IvmRefreshSummary summary = new IvmRefreshSummary();

        String longMessage = Strings.repeat("x", IvmRefreshSummary.MAX_ERROR_MESSAGE_LENGTH * 2);
        summary.update(failed(1000L, 1500L, IvmFailureReason.MIN_MAX_BOUNDARY_HIT.name(), longMessage));
        Assertions.assertEquals(1500L, summary.getLastFailureTimeMs());
        Assertions.assertEquals(IvmFailureReason.MIN_MAX_BOUNDARY_HIT.name(), summary.getLastFailureReason());
        Assertions.assertEquals(IvmRefreshSummary.MAX_ERROR_MESSAGE_LENGTH, summary.getLastErrorMessage().length());
        Assertions.assertTrue(longMessage.startsWith(summary.getLastErrorMessage().substring(0, 100)));
        Assertions.assertEquals(IvmRefreshMethod.NONE, summary.getLastRefreshMethod());
        Assertions.assertEquals(500L, summary.getLastRefreshDurationMs());
        Assertions.assertEquals(0L, summary.getLastSuccessTimeMs());

        summary.update(succeeded(2000L, 4500L, IvmRefreshMethod.COMPLETE));
        Assertions.assertEquals(4500L, summary.getLastSuccessTimeMs());
        Assertions.assertEquals(IvmRefreshMethod.COMPLETE, summary.getLastRefreshMethod());
        Assertions.assertEquals(2500L, summary.getLastRefreshDurationMs());
        Assertions.assertEquals(1500L, summary.getLastFailureTimeMs());
        Assertions.assertEquals(IvmFailureReason.MIN_MAX_BOUNDARY_HIT.name(), summary.getLastFailureReason());

        // A canceled task has no finish time, so it changes neither the latest method nor the latest duration.
        summary.update(canceled());
        Assertions.assertEquals(IvmRefreshMethod.COMPLETE, summary.getLastRefreshMethod());
        Assertions.assertEquals(2500L, summary.getLastRefreshDurationMs());
    }

    /** Fallbacks count the tasks that succeeded or failed after falling back; a canceled one is not counted. */
    @Test
    public void testFallbacksCountTheTasksThatFellBack() {
        IvmRefreshSummary summary = new IvmRefreshSummary();

        summary.update(succeeded(1000L, 2000L, IvmRefreshMethod.INCREMENTAL));
        Assertions.assertEquals(0L, summary.getFallbackCount());

        MTMVTask fellBack = succeeded(3000L, 4000L, IvmRefreshMethod.COMPLETE);
        fellBack(fellBack, IvmFailureReason.MIN_MAX_BOUNDARY_HIT.name());
        summary.update(fellBack);
        MTMVTask fellBackAndFailed = failed(5000L, 6000L, IvmRefreshFailureStage.EXECUTION_FAILED.name(), "failed");
        fellBack(fellBackAndFailed, IvmFailureReason.CHANGELOG_BROKEN.name());
        summary.update(fellBackAndFailed);
        MTMVTask fellBackAndCanceled = canceled();
        fellBack(fellBackAndCanceled, IvmFailureReason.PLAN_SIGNATURE_MISMATCH.name());
        summary.update(fellBackAndCanceled);

        Assertions.assertEquals(2L, summary.getFallbackCount());
        Assertions.assertEquals(IvmFailureReason.CHANGELOG_BROKEN.name(), summary.getLastFallbackReason());
        Assertions.assertEquals(6000L, summary.getLastFallbackTimeMs());
    }

    /** Only a task that succeeded gives the MV the baseline the incremental refresh keeps up. */
    @Test
    public void testOnlyASuccessMarksTheMvRefreshed() {
        IvmRefreshSummary summary = new IvmRefreshSummary();

        summary.update(failed(1000L, 2000L, IvmRefreshFailureStage.OTHER.name(), "failed"));
        summary.update(canceled());
        Assertions.assertFalse(summary.hasSucceeded());

        summary.update(succeeded(3000L, 4000L, IvmRefreshMethod.COMPLETE));
        summary.update(failed(5000L, 6000L, IvmRefreshFailureStage.OTHER.name(), "failed"));
        Assertions.assertTrue(summary.hasSucceeded());
    }

    /** A result is a task that has ended. */
    @Test
    public void testATaskThatHasNotEndedIsNotAResult() {
        MTMVTask running = new MTMVTask();
        running.setStatus(TaskStatus.RUNNING);

        Assertions.assertThrows(IllegalStateException.class, () -> new IvmRefreshSummary().update(running));
    }

    private static MTMVTask succeeded(long startTimeMs, long finishTimeMs, IvmRefreshMethod method) {
        MTMVTask task = ended(TaskStatus.SUCCESS, startTimeMs, finishTimeMs);
        Deencapsulation.setField(task, "ivmRefreshMethod", method);
        return task;
    }

    private static MTMVTask failed(long startTimeMs, long finishTimeMs, String reason, String message) {
        MTMVTask task = ended(TaskStatus.FAILED, startTimeMs, finishTimeMs);
        Deencapsulation.setField(task, "ivmRefreshMethod", IvmRefreshMethod.NONE);
        Deencapsulation.setField(task, "ivmFailureReason", reason);
        task.setErrMsg(message);
        return task;
    }

    // A STOP sets neither a finish time nor a failure reason.
    private static MTMVTask canceled() {
        MTMVTask task = new MTMVTask();
        task.setStatus(TaskStatus.CANCELED);
        task.setStartTimeMs(1L);
        Deencapsulation.setField(task, "ivmRefreshMethod", IvmRefreshMethod.NONE);
        return task;
    }

    private static MTMVTask ended(TaskStatus status, long startTimeMs, long finishTimeMs) {
        MTMVTask task = new MTMVTask();
        task.setStatus(status);
        task.setStartTimeMs(startTimeMs);
        task.setFinishTimeMs(finishTimeMs);
        return task;
    }

    private static void fellBack(MTMVTask task, String reason) {
        Deencapsulation.setField(task, "ivmFallback", true);
        Deencapsulation.setField(task, "ivmFallbackReason", reason);
    }
}
