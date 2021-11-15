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

import org.apache.doris.analysis.CreateEncryptKeyStmt;
import org.apache.doris.analysis.DropEncryptKeyStmt;
import org.apache.doris.analysis.EncryptKeyName;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// helper class for encryptKeys, create and drop
public class EncryptKeyHelper {
    private static final Logger LOG = LogManager.getLogger(EncryptKeyHelper.class);

    public static void createEncryptKey(CreateEncryptKeyStmt stmt) throws UserException {
        EncryptKeyName name = stmt.getEncryptKeyName();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(name.getDb());
        db.addEncryptKey(stmt.getEncryptKey());
    }

    public static void replayCreateEncryptKey(EncryptKey encryptKey) throws MetaNotFoundException {
        String dbName = encryptKey.getEncryptKeyName().getDb();
        Database db = Catalog.getCurrentCatalog().getDbOrMetaException(dbName);
        db.replayAddEncryptKey(encryptKey);
    }

    public static void dropEncryptKey(DropEncryptKeyStmt stmt) throws UserException {
        EncryptKeyName name = stmt.getEncryptKeyName();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(name.getDb());
        db.dropEncryptKey(stmt.getEncryptKeysSearchDesc());
    }

    public static void replayDropEncryptKey(EncryptKeySearchDesc encryptKeySearchDesc) throws MetaNotFoundException {
        String dbName = encryptKeySearchDesc.getKeyEncryptKeyName().getDb();
        Database db = Catalog.getCurrentCatalog().getDbOrMetaException(dbName);
        db.replayDropEncryptKey(encryptKeySearchDesc);
    }

}
