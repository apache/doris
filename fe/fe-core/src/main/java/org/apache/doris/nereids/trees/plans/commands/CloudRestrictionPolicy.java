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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Shared cloud-mode restriction policies for Nereids commands.
 */
public enum CloudRestrictionPolicy {
    UNSUPPORTED {
        @Override
        void validate(ConnectContext ctx, Command command) throws DdlException {
            reject(command);
        }
    },
    ROOT_ONLY {
        @Override
        void validate(ConnectContext ctx, Command command) throws DdlException {
            if (!Auth.ROOT_USER.equals(ctx.getCurrentUserIdentity().getUser())) {
                reject(command);
            }
        }
    };

    private static final Logger LOG = LogManager.getLogger(CloudRestrictionPolicy.class);

    abstract void validate(ConnectContext ctx, Command command) throws DdlException;

    private static void reject(Command command) throws DdlException {
        LOG.info("{} is not supported in cloud mode", command.getClass().getSimpleName());
        throw new DdlException("Unsupported operation");
    }
}
