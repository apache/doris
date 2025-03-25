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

package org.apache.doris.datasource.maxcompute;

import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.InitDatabaseLog;

/**
 * MaxCompute external database.
 */
public class MaxComputeExternalDatabase extends ExternalDatabase<MaxComputeExternalTable> {
    /**
     * Create MaxCompute external database.
     *
     * @param extCatalog External catalog this database belongs to.
     * @param id database id.
     * @param name database name.
     */
    public MaxComputeExternalDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName) {
        super(extCatalog, id, name, remoteName, InitDatabaseLog.Type.MAX_COMPUTE);
    }

    @Override
    public MaxComputeExternalTable buildTableInternal(String remoteTableName, String localTableName, long tblId,
            ExternalCatalog catalog,
            ExternalDatabase db) {
        return new MaxComputeExternalTable(tblId, localTableName, remoteTableName,
                (MaxComputeExternalCatalog) extCatalog,
                (MaxComputeExternalDatabase) db);
    }
}
