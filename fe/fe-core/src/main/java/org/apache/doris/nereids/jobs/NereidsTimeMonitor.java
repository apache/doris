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

package org.apache.doris.nereids.jobs;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Stopwatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Record time of execution
 */
public class NereidsTimeMonitor {
    public static final Logger LOG = LogManager.getLogger(NereidsTimeMonitor.class);
    private static final long TIMEOUT_MILLIS = 5000;
    private final Stopwatch stopwatch;
    private final Map<JobType, Long> jobTime;
    private final Map<RuleType, Long> ruleTime;
    private final Set<Scope<?>> unfinishedScope;

    NereidsTimeMonitor(Stopwatch stopwatch) {
        jobTime = new EnumMap<>(JobType.class);
        ruleTime = new EnumMap<>(RuleType.class);
        unfinishedScope = new HashSet<>();
        this.stopwatch = stopwatch;
    }

    /**
     * create NereidsTimeMonitor
     */
    public static NereidsTimeMonitor createUnstarted() {
        return new NereidsTimeMonitor(Stopwatch.createUnstarted());
    }

    /**
     * execute Job
     */
    public void executeJobWithinTimeLimit(Runnable runnable, Job job) {
        if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > TIMEOUT_MILLIS) {
            LOG.warn(this.toString());
            throw new RuntimeException(String.format("Nereids cost too much time ( > %s s )", TIMEOUT_MILLIS / 1000));
        }
        Scope<Job> scope = new Scope<>(job);
        runnable.run();
        scope.close();
    }

    @Override
    public String toString() {
        List<Map.Entry<JobType, Long>> sortedJobTime = new ArrayList<>(jobTime.entrySet());
        sortedJobTime.sort(Map.Entry.comparingByValue(Collections.reverseOrder()));
        List<Map.Entry<RuleType, Long>> sortedRuleTime = new ArrayList<>(ruleTime.entrySet());
        sortedRuleTime.sort(Map.Entry.comparingByValue(Collections.reverseOrder()));
        List<Scope<?>> sortedScope = new ArrayList<>(unfinishedScope);
        sortedScope.sort((a, b) -> (int)(b.runTime - a.runTime));
        StringBuilder sb = new StringBuilder();
        sb.append("job:\n");
        sortedJobTime.forEach(entry -> sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n"));

        sb.append("rule\n");
        sortedRuleTime.forEach(entry -> sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n"));

        sb.append("unfinished:\n");
        sortedScope.forEach(s -> sb.append(s).append("\n"));
        return sb.toString();
    }

    /**
     * execute Rule
     */
    public List<Plan> executeRule(Supplier<List<Plan>> lambda, Rule rule) {
        Scope<Rule> scope = new Scope<>(rule);
        List<Plan> res = lambda.get();
        scope.close();
        return res;
    }

    public void start() {
        stopwatch.start();
    }

    public void stop() {
        stopwatch.stop();
    }

    class Scope<T> {
        private final T item;
        private final long startTime;
        private long runTime;

        Scope(T item) {
            this.item = item;
            this.startTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            this.runTime = 0;
        }

        private void updateTime(long now) {
            runTime = now - startTime;
        }

        public void close() {
            unfinishedScope.forEach(scope -> scope.updateTime(stopwatch.elapsed(TimeUnit.MILLISECONDS)));
            if (item instanceof Rule) {
                RuleType type = ((Rule) item).getRuleType();
                ruleTime.put(type, ruleTime.getOrDefault(type, 0L) + runTime);
            } else if (item instanceof Job) {
                JobType type = ((Job) item).getType();
                jobTime.put(type, jobTime.getOrDefault(type, 0L) + runTime);
            }
            unfinishedScope.remove(this);
        }

        @Override
        public String toString() {
            if (item instanceof Rule) {
                return String.format("%s : %s", ((Rule) item).getRuleType(), runTime);
            } else {
                return String.format("%s : %s", ((Job) item).getType(), runTime);
            }
        }
    }
}
