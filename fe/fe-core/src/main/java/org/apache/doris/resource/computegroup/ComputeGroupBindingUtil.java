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
 * <p>This is a transitional binding: it only adds the ability to <b>declare</b> a compute group,
 * it does not change how the group is resolved, checked at runtime, or how failures are handled.
 * The property name and its value space are intentionally identical to the final
 * {@code (owner, compute_group, workload_group)} design, so that metadata written by this version
 * can be read as an explicit "pin" by later versions without any conversion.
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
     * Re-checks a job's binding before each of its tasks runs.
     *
     * <p>Creation-time validation alone is not enough: the groups can be dropped and the owner's
     * privileges can be revoked while the job keeps running, and without this check the task would
     * silently keep using a group its owner is no longer entitled to, or fail much later with an
     * unrelated message such as "no available BE found".
     *
     * <p>Everything is checked against {@code owner}, the identity the task actually runs as, not
     * against whoever created or last altered the job.
     *
     * @param owner the identity the task runs as
     * @param computeGroup the compute group the task will run in, already resolved; empty means the
     *        job is not bound to a named compute group and there is nothing to check
     * @param workloadGroup the workload group declared on the job, empty when none was declared
     */
    public static void checkBindingBeforeTask(UserIdentity owner, String computeGroup, String workloadGroup)
            throws UserException {
        if (owner == null) {
            // Jobs created before the owner was persisted; nothing to check them against.
            return;
        }

        boolean computeGroupBound = Config.isCloudMode() && !StringUtils.isEmpty(computeGroup);
        if (computeGroupBound) {
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

        if (!Config.enable_workload_group || StringUtils.isEmpty(workloadGroup)) {
            return;
        }

        if (!Env.getCurrentEnv().getAccessManager()
                .checkWorkloadGroupPriv(owner, workloadGroup, PrivPredicate.USAGE)) {
            throw new UserException("USAGE denied to user '" + owner.getQualifiedUser()
                    + "' for workload group '" + workloadGroup + "'");
        }

        // A workload group lives in the namespace of a compute group, so it can only be looked up
        // once the compute group is known. Without one there is nothing to resolve it against.
        // Safe to resolve here: existence was confirmed above, so the hint-message path that needs a
        // ConnectContext cannot be reached.
        if (computeGroupBound) {
            // Throws when the workload group no longer exists under that compute group.
            Env.getCurrentEnv().getComputeGroupMgr().getComputeGroupByName(computeGroup)
                    .getWorkloadGroup(workloadGroup, Env.getCurrentEnv().getWorkloadGroupMgr());
        }
    }
}
