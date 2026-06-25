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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Index;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateIndexOpTest {

    // Regression test: analyzing/validating a "CREATE INDEX" / "ALTER TABLE ... ADD INDEX"
    // statement must not allocate a persistent index id. Allocating one calls
    // MetaIdGenerator.getNextId(), which may
    // write an OP_SAVE_NEXTID edit log entry. When an audit plugin re-parses such a statement on a
    // non-master FE (follower), that edit log write can crash the FE. The real index id must be
    // assigned only later, in the master execution path (SchemaChangeHandler.processAddIndex).
    @Test
    void testValidateDoesNotAllocateIndexId() throws Exception {
        IndexDefinition indexDef = new IndexDefinition(
                "idx_audit", false, Lists.newArrayList("col1"), "INVERTED", null, "comment");
        // tableName is null, so validate() does not touch the ConnectContext; passing null is safe.
        CreateIndexOp op = new CreateIndexOp(null, indexDef, true);
        op.validate(null);

        Assertions.assertNotNull(op.getIndex());
        Assertions.assertEquals(Index.INDEX_ID_INIT_VALUE, op.getIndex().getIndexId(),
                "CreateIndexOp.validate() must not allocate a persistent index id");
    }
}
