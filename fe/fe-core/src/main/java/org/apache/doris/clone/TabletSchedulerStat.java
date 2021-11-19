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

package org.apache.doris.clone;

import com.google.common.collect.Lists;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/*
 * A simple statistic of tablet checker and scheduler
 */
public class TabletSchedulerStat {

    @Target({ ElementType.FIELD })
    @Retention(RetentionPolicy.RUNTIME)
    public static @interface StatField {
        String value() default "";

        // add this prefix for analyzing log more easily.
        String prefix() default "TStat";
    }

    /*
     * TabletChecker related
     */
    @StatField("num of tablet check round")
    public AtomicLong counterTabletCheckRound = new AtomicLong(0L);
    @StatField("cost of tablet check(ms)")
    public AtomicLong counterTabletCheckCostMs = new AtomicLong(0L);
    @StatField("num of tablet checked in tablet checker")
    public AtomicLong counterTabletChecked = new AtomicLong(0L);
    @StatField("num of unhealthy tablet checked in tablet checker")
    public AtomicLong counterUnhealthyTabletNum = new AtomicLong(0L);
    @StatField("num of tablet being added to tablet scheduler")
    public AtomicLong counterTabletAddToBeScheduled = new AtomicLong(0L);

    /*
     * TabletScheduler related
     */
    @StatField("num of tablet schedule round")
    public AtomicLong counterTabletScheduleRound = new AtomicLong(0L);
    @StatField("cost of tablet schedule(ms)")
    public AtomicLong counterTabletScheduleCostMs = new AtomicLong(0L);
    @StatField("num of tablet being scheduled")
    public AtomicLong counterTabletScheduled = new AtomicLong(0L);
    @StatField("num of tablet being scheduled succeeded")
    public AtomicLong counterTabletScheduledSucceeded = new AtomicLong(0L);
    @StatField("num of tablet being scheduled failed")
    public AtomicLong counterTabletScheduledFailed = new AtomicLong(0L);
    @StatField("num of tablet being scheduled discard")
    public AtomicLong counterTabletScheduledDiscard = new AtomicLong(0L);

    /*
     * Tablet priority related
     */
    @StatField("num of tablet priority upgraded")
    public AtomicLong counterTabletPrioUpgraded = new AtomicLong(0L);
    @StatField("num of tablet priority downgraded")
    public AtomicLong counterTabletPrioDowngraded = new AtomicLong(0L);

    /*
     * Clone task related
     */
    @StatField("num of clone task")
    public AtomicLong counterCloneTask = new AtomicLong(0L);
    @StatField("num of clone task succeeded")
    public AtomicLong counterCloneTaskSucceeded = new AtomicLong(0L);
    @StatField("num of clone task failed")
    public AtomicLong counterCloneTaskFailed = new AtomicLong(0L);
    @StatField("num of clone task timeout")
    public AtomicLong counterCloneTaskTimeout = new AtomicLong(0L);

    /*
     * replica unhealthy type
     */
    @StatField("num of replica missing error")
    public AtomicLong counterReplicaMissingErr = new AtomicLong(0L);
    @StatField("num of replica version missing error")
    public AtomicLong counterReplicaVersionMissingErr = new AtomicLong(0L);
    @StatField("num of replica unavailable error")
    public AtomicLong counterReplicaUnavailableErr = new AtomicLong(0L);
    @StatField("num of replica redundant error")
    public AtomicLong counterReplicaRedundantErr = new AtomicLong(0L);
    @StatField("num of replica missing in cluster error")
    public AtomicLong counterReplicaMissingInClusterErr = new AtomicLong(0L);
    @StatField("num of replica missing for tag error")
    public AtomicLong counterReplicaMissingForTagErr = new AtomicLong(0L);
    @StatField("num of balance scheduled")
    public AtomicLong counterBalanceSchedule = new AtomicLong(0L);
    @StatField("num of colocate replica mismatch")
    public AtomicLong counterReplicaColocateMismatch = new AtomicLong(0L);
    @StatField("num of colocate replica redundant")
    public AtomicLong counterReplicaColocateRedundant = new AtomicLong(0L);

    private TabletSchedulerStat lastSnapshot = null;

    /*
     * make a snapshot of current stat,
     * in order to calculate the incremental stat when next call of incrementalBrief()
     */
    private void snapshot() {
        lastSnapshot = new TabletSchedulerStat();
        try {
            Class<?> clazz = Class.forName(this.getClass().getName());
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                if (!field.isAnnotationPresent(StatField.class)) {
                    continue;
                }
                
                ((AtomicLong) field.get(lastSnapshot)).set(((AtomicLong) field.get(this)).get());
            }
        } catch (ClassNotFoundException | IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            lastSnapshot = null;
        }
    }

    public TabletSchedulerStat getLastSnapshot() {
        return lastSnapshot;
    }

    public List<List<String>> getBrief() {
        List<List<String>> result = Lists.newArrayList();
        try {
            Class<?> clazz = Class.forName(this.getClass().getName());
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                if (!field.isAnnotationPresent(StatField.class)) {
                    continue;
                }
                
                List<String> info = Lists.newArrayList();
                info.add(field.getAnnotation(StatField.class).value());
                info.add(String.valueOf(((AtomicLong) field.get(this)).get()));
                result.add(info);
            }
        } catch (ClassNotFoundException | IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            return Lists.newArrayList();
        }
        return result;
    }

    /*
     * print the brief of statistics, also print the incremental part since last call of incrementalBrief()
     */
    public String incrementalBrief() {
        StringBuilder sb = new StringBuilder("TStat :\n");
        try {
            Class<?> clazz = Class.forName(this.getClass().getName());
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                if (!field.isAnnotationPresent(StatField.class)) {
                    continue;
                }

                long current = ((AtomicLong) field.get(this)).get();
                long last = lastSnapshot == null ? 0 : ((AtomicLong) field.get(lastSnapshot)).get();
                sb.append(field.getAnnotation(StatField.class).prefix()).append(" ");
                sb.append(field.getAnnotation(StatField.class).value()).append(": ");
                sb.append(current).append(" (+").append(current - last).append(")\n");
            }
        } catch (ClassNotFoundException | IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            return "";
        }

        snapshot();
        return sb.toString();
    }

    // for test
    public static void main(String[] args) {
        TabletSchedulerStat stat = new TabletSchedulerStat();

        stat.counterCloneTask.addAndGet(300);
        System.out.println(stat.incrementalBrief());

        stat.counterCloneTask.addAndGet(3);
        stat.counterTabletChecked.incrementAndGet();
        System.out.println(stat.incrementalBrief());
    }
}
