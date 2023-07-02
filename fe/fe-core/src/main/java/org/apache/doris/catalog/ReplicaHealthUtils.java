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

package org.apache.doris.catalog;

import org.apache.doris.common.UserException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.function.Predicate;

/**
 * This class represents managing replica health base rule.
 */
public class ReplicaHealthUtils {
    private static final Logger LOG = LogManager.getLogger(ReplicaHealthUtils.class);

    public static void checkPartitionReadyCommit(OlapTable table, Partition partition,
            List<MaterializedIndex> allIndices, Predicate<Replica> isReplicaCommitSucc)
            throws UserException {
        List<Replica> succReplicas = Lists.newArrayList();
        List<Replica> failCommitReplicas = Lists.newArrayList();
        List<Replica> failVersionReplicas = Lists.newArrayList();
        int requiredReplicaNum = table.getLoadRequiredReplicaNum(partition.getId());
        for (MaterializedIndex index : allIndices) {
            for (Tablet tablet : index.getTablets()) {
                succReplicas.clear();
                failCommitReplicas.clear();
                failVersionReplicas.clear();
                for (Replica replica : tablet.getReplicas()) {
                    if (!isReplicaCommitSucc.test(replica)) {
                        failCommitReplicas.add(replica);
                    } else if (replica.getLastFailedVersion() > 0) {
                        failVersionReplicas.add(replica);
                    } else {
                        succReplicas.add(replica);
                    }
                }
                if (succReplicas.size() < requiredReplicaNum) {
                    throw new UserException(String.format("partition %s can not commit"
                                + " due to tablet %s succ replica num %s < required replica num %s, "
                                + " this tablet's succ replicas { %s }, fail commit replicas { %s },"
                                + " fail version replicas { %s }",
                                partition.getId(), tablet.getId(), succReplicas.size(), requiredReplicaNum,
                                Joiner.on(",").join(succReplicas),
                                Joiner.on(",").join(failCommitReplicas),
                                Joiner.on(",").join(failVersionReplicas)));
                }
            }
        }
    }

    public static void updatePartitionCommitVersion(Partition partition, long version,
            Predicate<Replica> isReplicaCommitSucc) {
        Preconditions.checkState(partition.getNextVersion() == version);
        List<MaterializedIndex> allIndices =
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
        for (MaterializedIndex index : allIndices) {
            List<Tablet> tablets = index.getTablets();
            for (Tablet tablet : tablets) {
                for (Replica replica : tablet.getReplicas()) {
                    if (!isReplicaCommitSucc.test(replica)) {
                        // TODO(cmy): do we need to update last failed version here?
                        // because in updateCatalogAfterVisible, it will be updated again.
                        replica.updateLastFailedVersion(version);
                    }
                }
            }
        }

        // commit version = next version - 1, it will increase 1 too.
        partition.setNextVersion(version + 1);
    }

    // TODO(yujun) isReplicaContainThisVersion test if a replica publish succ on this version.
    // A better way is to remove isReplicaContainThisVersion, and update replica version just before run this check.
    // But i'm not sure if it has side effect to update the replica's version
    // while the partition is not visible on that version yeah.
    public static void checkPartitionReadyVisibleOnVersion(OlapTable table, Partition partition,
            List<MaterializedIndex> allIndices, long version,
            Predicate<Replica> isReplicaContainThisVersion) throws UserException {
        if (partition.getVisibleVersion() != version - 1) {
            throw new UserException(String.format("partition %s can not visible on version %s due to"
                        + " it's not equal to partition's visible version %s + 1, need wait",
                        partition.getId(), version, partition.getVisibleVersion()));

        }

        List<Replica> succReplicas = Lists.newArrayList();
        List<Replica> failReplicas = Lists.newArrayList();
        int requiredReplicaNum = table.getLoadRequiredReplicaNum(partition.getId());
        for (MaterializedIndex index : allIndices) {
            for (Tablet tablet : index.getTablets()) {
                succReplicas.clear();
                failReplicas.clear();
                for (Replica replica : tablet.getReplicas()) {
                    if (isReplicaCatchup(replica, version, isReplicaContainThisVersion)) {
                        succReplicas.add(replica);
                    } else {
                        failReplicas.add(replica);
                    }
                }

                if (succReplicas.size() < requiredReplicaNum) {
                    throw new UserException(String.format("partition %s can not visible on version %s "
                                + " due to tablet %s succ replica num %s < required replica num %s, "
                                + " this tablet's succ replicas { %s }, fail replicas { %s }",
                                partition.getId(), version, tablet.getId(), succReplicas.size(),
                                requiredReplicaNum, Joiner.on(",").join(succReplicas),
                                Joiner.on(",").join(failReplicas)));
                }
            }
        }
    }

    public static void udpatePartitionVisibleVersion(Partition partition, long version,
            long versionTime, Predicate<Replica> isReplicaContainThisVersion) {
        Preconditions.checkState(version == partition.getVisibleVersion() + 1);
        List<MaterializedIndex> allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
        for (MaterializedIndex index : allIndices) {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    if (isReplicaCatchup(replica, version, isReplicaContainThisVersion)) {
                        replica.updateVersionInfo(version);
                    } else {
                        long lastSuccessVersion = replica.getLastSuccessVersion();
                        long lastFailedVersion = Math.max(version, replica.getLastFailedVersion());

                        if (isReplicaContainThisVersion.test(replica)) {
                            lastSuccessVersion = Math.max(version, lastSuccessVersion);

                            // (yujun)below words copy from commit b7b78ae7079.
                            //
                            // this means the replica has error in the past, but we did not observe it
                            // during upgrade, one job maybe in quorum finished state, for example,
                            // A,B,C 3 replica A,B 's version is 10, C's version is 10 but C' 10 is abnormal
                            // should be rollback then we will detect this and set C's last failed version to
                            // 10 and last success version to 11 this logic has to be replayed
                            // in checkpoint thread
                            //
                            lastFailedVersion = partition.getVisibleVersion();
                        }

                        replica.updateVersionWithFailedInfo(replica.getVersion(), lastFailedVersion,
                                lastSuccessVersion);
                    }
                }
            }
        }

        partition.updateVisibleVersionAndTime(version, versionTime);
        if (LOG.isDebugEnabled()) {
            LOG.debug("set partition {}'s version to [{}]", partition.getId(), version);
        }
    }

    private static boolean isReplicaCatchup(Replica replica, long version,
            Predicate<Replica> isReplicaContainThisVersion) {
        if (replica.checkVersionCatchUp(version, true)) {
            return true;
        }

        // BE has publish version succ, but not report yeah.
        if (replica.getVersion() >= version - 1 && isReplicaContainThisVersion.test(replica)) {
            return true;
        }

        return false;
    }
}
