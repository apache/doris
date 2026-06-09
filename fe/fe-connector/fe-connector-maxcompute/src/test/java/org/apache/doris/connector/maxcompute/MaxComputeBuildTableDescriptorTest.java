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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.thrift.TMCTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * FIX-READ-DESC (P4-T06d) — guards the MaxCompute read-path table descriptor contract.
 *
 * <p>WHY this matters: after the {@code max_compute} cutover, a SELECT routes through
 * {@code PluginDrivenExternalTable.toThrift()} → {@code metadata.buildTableDescriptor(...)}.
 * BE's {@code file_scanner} static_casts {@code table_desc()} to {@code MaxComputeTableDescriptor}
 * unconditionally for {@code table_format_type=="max_compute"}, and reads endpoint/quota/project/
 * table/properties as the auth + addressing contract. If this override regressed to {@code null}
 * (SPI default) or a {@code SCHEMA_TABLE} descriptor with no {@code mcTable}, BE would type-confuse
 * a {@code SchemaTableDescriptor} as a {@code MaxComputeTableDescriptor} → crash / garbage reads.
 * Each assertion below therefore encodes a BE-side requirement, not just the method's shape
 * (Rule 9): this test FAILS if the override returns null or any non-MAX_COMPUTE_TABLE descriptor.</p>
 *
 * <p>Boundary: this connector module has no fe-core dependency, so the test can only assert the
 * override's OWN output. It cannot reach the fe-core {@code toThrift} call site (passing remote
 * dbName/remoteName, numCols) — that half of the contract is covered by user-run e2e only.</p>
 *
 * <p>The ctor only assigns its args; {@code buildTableDescriptor} never dereferences odps /
 * structureHelper, so passing {@code null} for them is safe and keeps the test offline.</p>
 */
public class MaxComputeBuildTableDescriptorTest {

    @Test
    public void buildsMaxComputeTableDescriptorWithAuthAndAddressing() {
        String endpoint = "http://service.cn-hangzhou.maxcompute.aliyun.com/api";
        String quota = "test_quota";
        Map<String, String> properties = new HashMap<>();
        properties.put("mc.access_key", "test-ak");
        properties.put("mc.secret_key", "test-sk");

        MaxComputeConnectorMetadata metadata = new MaxComputeConnectorMetadata(
                null, null, "default_project", endpoint, quota, properties);

        // dbName / remoteName are already remote names at the real call site (OQ-7).
        long tableId = 42L;
        String tableName = "local_table";
        String dbName = "remote_project";
        String remoteName = "remote_table";
        int numCols = 7;
        long catalogId = 100L;

        TTableDescriptor desc = metadata.buildTableDescriptor(
                null, tableId, tableName, dbName, remoteName, numCols, catalogId);

        // (1) must not be null — null would trigger the SCHEMA_TABLE fallback in fe-core.
        Assertions.assertNotNull(desc,
                "buildTableDescriptor must return a typed descriptor, never null (BE expects MC type)");
        // (2) BE selects MaxComputeTableDescriptor only for MAX_COMPUTE_TABLE.
        Assertions.assertEquals(TTableType.MAX_COMPUTE_TABLE, desc.getTableType(),
                "table type must be MAX_COMPUTE_TABLE; SCHEMA_TABLE would crash BE's static_cast");
        // (3) BE reads mcTable for auth/addressing; it must be set.
        Assertions.assertTrue(desc.isSetMcTable(),
                "mcTable must be set; BE reads endpoint/quota/project/table/properties from it");

        TMCTable mcTable = desc.getMcTable();
        Assertions.assertEquals(endpoint, mcTable.getEndpoint(), "endpoint must reach BE auth path");
        Assertions.assertEquals(quota, mcTable.getQuota(), "quota must reach BE auth path");
        // project/table must be the REMOTE names — they must match the SPI read session (OQ-7).
        Assertions.assertEquals(dbName, mcTable.getProject(),
                "project must be the remote dbName param, consistent with the SPI read session");
        Assertions.assertEquals(remoteName, mcTable.getTable(),
                "table must be the remote remoteName param, consistent with the SPI read session");
        Assertions.assertEquals(properties, mcTable.getProperties(),
                "credentials/properties must be carried through for BE auth");
    }
}
