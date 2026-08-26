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

package org.apache.doris.resource.computegroup;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.lang3.StringUtils;

/**
 * Validation for the {@code compute_group} property that can be declared on background jobs
 * (routine load / async materialized view).
 *
 * <p>This is a transitional binding: it adds the ability to <b>declare</b> a compute group, and
 * re-checks that declaration before every task, but does not change how the group is resolved for
 * jobs that declare none. The property name and its value space are intentionally identical to the
 * final {@code (owner, compute_group, workload_group)} design, so that metadata written by this
 * version can be read as an explicit "pin" by later versions without any conversion.
 *
 * <p>Two values are rejected on purpose:
 * <ul>
 *   <li>Any value in non-cloud mode - non-cloud support is not part of this transitional change,
 *       so no non-cloud metadata will ever carry this key.</li>
 *   <li>{@code DEFAULT} (case insensitive) - it is reserved by the final design to mean
 *       "follow the owner's default group at runtime". Allowing a job to pin a group literally
 *       named {@code DEFAULT} would silently change its behavior after upgrading.</li>
 * </ul>
 */
public class ComputeGroupBindingUtil {

    /**
     * Reserved value in the final binding design: "not pinned, follow the owner's default group".
     * Rejected here so that no job can pin a group literally named {@code DEFAULT}.
     */
    public static final String RESERVED_DEFAULT = "DEFAULT";

    public static final String ERR_NON_CLOUD =
            "Property 'compute_group' is only supported in cloud mode for now.";

    private ComputeGroupBindingUtil() {
    }

    /**
     * Validates a user declared compute group name.
     *
     * <p>An empty value means "not declared" and is treated as a no-op by the caller, which must
     * not write the key into the job's property map at all.
     *
     * @param ctx the context of the user executing CREATE / ALTER; privileges are checked against
     *            this user, matching how {@code workload_group} is validated today
     * @param computeGroup the declared name
     */
    public static void validateDeclaredComputeGroup(ConnectContext ctx, String computeGroup) throws UserException {
        if (StringUtils.isEmpty(computeGroup)) {
            return;
        }

        if (!Config.isCloudMode()) {
            throw new UserException(ERR_NON_CLOUD);
        }

        if (RESERVED_DEFAULT.equalsIgnoreCase(computeGroup)) {
            throw new UserException("'" + RESERVED_DEFAULT + "' is a reserved value for property 'compute_group'"
                    + " and can not be used as a compute group name here.");
        }

        if (ctx == null) {
            throw new UserException("Can not validate property 'compute_group' without a connect context.");
        }

        // Same two checks, and the same order, as `USE @<compute group>`.
        if (!Env.getCurrentEnv().getAccessManager().checkCloudPriv(ctx.getCurrentUserIdentity(),
                computeGroup, PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER)) {
            throw new UserException("USAGE denied to user '" + ctx.getQualifiedUser()
                    + "' for compute group '" + computeGroup + "'");
        }

        if (!((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames().contains(computeGroup)) {
            throw new UserException("Compute group '" + computeGroup + "' not found.");
        }
    }

    /**
     * Re-checks the compute group a job declared, before each of its tasks runs.
     *
     * <p>Creation-time validation alone is not enough: the group can be dropped and the owner's
     * privileges can be revoked while the job keeps running, and without this check the task would
     * silently keep using a group its owner is no longer entitled to, or fail much later with an
     * unrelated message such as "no available BE found".
     *
     * <p>Everything is checked against {@code owner}, the identity the task actually runs as, not
     * against whoever created or last altered the job.
     *
     * <p>The workload group is deliberately out of scope here. Both callers resolve it a little
     * later through {@code WorkloadGroupMgr#getWorkloadGroup(ConnectContext)} - routine load in
     * {@code KafkaTaskInfo#createRoutineLoadTask}, an MV refresh in the coordinator - and that
     * already runs the same USAGE check against the same owner and the same existence check in the
     * same compute group namespace.
     *
     * @param owner the identity the task runs as
     * @param computeGroup the compute group declared on the job; empty means the job declared none
     *        and there is nothing to re-check
     */
    public static void checkComputeGroupBeforeTask(UserIdentity owner, String computeGroup)
            throws UserException {
        if (owner == null) {
            // Jobs created before the owner was persisted; nothing to check them against.
            return;
        }

        if (!Config.isCloudMode() || StringUtils.isEmpty(computeGroup)) {
            return;
        }

        // Deliberately not ComputeGroupMgr.getComputeGroupByName() for the existence check: when
        // the group is missing that builds a hint message from the thread-local ConnectContext,
        // and the callers here are background threads that do not have one.
        if (!((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames()
                .contains(computeGroup)) {
            throw new UserException("Compute group '" + computeGroup + "' not found.");
        }

        if (!Env.getCurrentEnv().getAccessManager().checkCloudPriv(owner, computeGroup,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER)) {
            throw new UserException("USAGE denied to user '" + owner.getQualifiedUser()
                    + "' for compute group '" + computeGroup + "'");
        }
    }
}
