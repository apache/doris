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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class CloudRestrictedCommandTest {
    private String originalDeployMode;
    private String originalCloudUniqueId;

    @BeforeEach
    public void saveCloudConfig() {
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;
    }

    @AfterEach
    public void restoreCloudConfig() {
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
    }

    @Test
    public void testUnsupportedCommandIsRejectedBeforeRunForEveryUser() {
        setCloudMode();
        for (UserIdentity userIdentity : Arrays.asList(UserIdentity.ADMIN, UserIdentity.ROOT)) {
            RecordingUnsupportedCommand command = new RecordingUnsupportedCommand();
            DdlException exception = Assertions.assertThrows(DdlException.class,
                    () -> command.execute(contextFor(userIdentity), null));
            Assertions.assertEquals("Unsupported operation", exception.getDetailMessage());
            Assertions.assertFalse(command.isRunInvoked());
        }
    }

    @Test
    public void testUnsupportedCommandRunsOutsideCloudMode() {
        setNonCloudMode();
        RecordingUnsupportedCommand command = new RecordingUnsupportedCommand();

        Assertions.assertDoesNotThrow(() -> command.execute(contextFor(UserIdentity.ADMIN), null));
        Assertions.assertTrue(command.isRunInvoked());
    }

    @Test
    public void testRootOnlyCommandRejectsAdminAndAllowsRootInCloudMode() {
        setCloudMode();
        RecordingRootOnlyCommand adminCommand = new RecordingRootOnlyCommand();
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> adminCommand.execute(contextFor(UserIdentity.ADMIN), null));
        Assertions.assertEquals("Unsupported operation", exception.getDetailMessage());
        Assertions.assertFalse(adminCommand.isRunInvoked());

        RecordingRootOnlyCommand rootCommand = new RecordingRootOnlyCommand();
        Assertions.assertDoesNotThrow(() -> rootCommand.execute(contextFor(UserIdentity.ROOT), null));
        Assertions.assertTrue(rootCommand.isRunInvoked());
    }

    @Test
    public void testRootOnlyCommandRunsForAdminOutsideCloudMode() {
        setNonCloudMode();
        RecordingRootOnlyCommand command = new RecordingRootOnlyCommand();

        Assertions.assertDoesNotThrow(() -> command.execute(contextFor(UserIdentity.ADMIN), null));
        Assertions.assertTrue(command.isRunInvoked());
    }

    @Test
    public void testUnrestrictedCommandRunsInCloudMode() {
        setCloudMode();
        RecordingCommand command = new RecordingCommand();

        Assertions.assertDoesNotThrow(() -> command.execute(contextFor(UserIdentity.ADMIN), null));
        Assertions.assertTrue(command.isRunInvoked());
    }

    @Test
    public void testExecutePreservesCommandFailure() {
        setNonCloudMode();
        RecordingCommand command = new RecordingCommand();
        RuntimeException expected = new RuntimeException("command failed");
        command.setFailure(expected);

        RuntimeException actual = Assertions.assertThrows(RuntimeException.class,
                () -> command.execute(contextFor(UserIdentity.ADMIN), null));
        Assertions.assertSame(expected, actual);
        Assertions.assertTrue(command.isRunInvoked());
    }

    @Test
    public void testCloudRestrictionDeclarations() {
        List<Class<? extends Command>> unsupportedCommands = Arrays.asList(
                AdminCancelRebalanceDiskCommand.class,
                AdminCancelRepairTableCommand.class,
                AdminCheckTabletsCommand.class,
                AdminCleanTrashCommand.class,
                AdminRebalanceDiskCommand.class,
                AdminRepairTableCommand.class,
                AdminSetPartitionVersionCommand.class,
                AdminSetReplicaStatusCommand.class,
                AdminSetReplicaVersionCommand.class,
                AlterResourceCommand.class,
                AlterStoragePolicyCommand.class,
                BackupCommand.class,
                CancelDecommissionBackendCommand.class,
                ShowTabletStorageFormatCommand.class);
        for (Class<? extends Command> commandClass : unsupportedCommands) {
            Assertions.assertTrue(CloudUnsupportedCommand.class.isAssignableFrom(commandClass),
                    commandClass.getSimpleName());
        }

        List<Class<? extends Command>> rootOnlyCommands = Arrays.asList(
                AdminSetFrontendConfigCommand.class,
                ShowReplicaDistributionCommand.class);
        for (Class<? extends Command> commandClass : rootOnlyCommands) {
            Assertions.assertTrue(CloudRootOnlyCommand.class.isAssignableFrom(commandClass),
                    commandClass.getSimpleName());
        }

        Assertions.assertFalse(CloudRestrictedCommand.class.isAssignableFrom(AdminCompactTableCommand.class));
    }

    private static ConnectContext contextFor(UserIdentity userIdentity) {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(userIdentity);
        return ctx;
    }

    private static void setCloudMode() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
    }

    private static void setNonCloudMode() {
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
    }

    private static class RecordingCommand extends EmptyCommand {
        private boolean runInvoked;
        private RuntimeException failure;

        @Override
        public void run(ConnectContext ctx, StmtExecutor executor) {
            runInvoked = true;
            if (failure != null) {
                throw failure;
            }
        }

        public boolean isRunInvoked() {
            return runInvoked;
        }

        public void setFailure(RuntimeException failure) {
            this.failure = failure;
        }
    }

    private static class RecordingUnsupportedCommand extends RecordingCommand implements CloudUnsupportedCommand {
    }

    private static class RecordingRootOnlyCommand extends RecordingCommand implements CloudRootOnlyCommand {
    }
}
