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

package org.apache.doris.cloud.transaction;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.cloud.proto.Cloud.TxnInfoPB;
import org.apache.doris.cloud.proto.Cloud.TxnStatusPB;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TxnUtilOwnerTest {
    @Test
    public void testOwnerSurvivesCloudTransactionRead() {
        UserIdentity[] owners = {
                UserIdentity.createAnalyzedUserIdentWithIp("loader@tenant", "%"),
                UserIdentity.createAnalyzedUserIdentWithDomain("loader", "example.com")
        };
        for (UserIdentity owner : owners) {
            TxnInfoPB info = TxnInfoPB.newBuilder().setDbId(1).addTableIds(2).setTxnId(3)
                    .setStatus(TxnStatusPB.TXN_STATUS_PRECOMMITTED).setOwner(TxnUtil.txnOwnerToPb(owner)).build();
            Assertions.assertEquals(owner, TxnUtil.transactionStateFromPb(info).getOwner());
        }
    }

    @Test
    public void testLegacyCloudTransactionHasNoOwner() {
        TxnInfoPB info = TxnInfoPB.newBuilder().setDbId(1).addTableIds(2).setTxnId(3)
                .setStatus(TxnStatusPB.TXN_STATUS_PRECOMMITTED).build();
        Assertions.assertNull(TxnUtil.transactionStateFromPb(info).getOwner());
    }
}
