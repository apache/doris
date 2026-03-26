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

package org.apache.doris.datasource.deltalake;

import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.InitDatabaseLog;

/**
 * Represents a database in a Delta Lake catalog.
 */
public class DeltaLakeExternalDatabase extends ExternalDatabase<DeltaLakeExternalTable> {

    public DeltaLakeExternalDatabase(ExternalCatalog extCatalog, Long id, String name, String remoteName) {
        super(extCatalog, id, name, remoteName, InitDatabaseLog.Type.DELTALAKE);
    }

    @Override
    public DeltaLakeExternalTable buildTableInternal(String remoteTableName, String localTableName, long tblId,
            ExternalCatalog catalog, ExternalDatabase db) {
        return new DeltaLakeExternalTable(tblId, localTableName, remoteTableName,
                catalog, (DeltaLakeExternalDatabase) db);
    }
}
