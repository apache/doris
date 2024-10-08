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

package org.apache.doris.datasource.mapping;

import java.util.List;
import java.util.Map;

public interface IdentifierMapping {

    /**
     * Maps a list of remote database names to their corresponding local database names.
     *
     * @param remoteDatabaseNames the list of remote database names to be mapped
     * @return a list of corresponding local database names
     */
    List<String> fromRemoteDatabaseName(List<String> remoteDatabaseNames);

    /**
     * Maps a list of remote table names in a specified remote database to their corresponding local table names.
     *
     * @param remoteDatabaseName the name of the remote database where the tables reside
     * @param remoteTableNames the list of remote table names to be mapped
     * @return a list of corresponding local table names
     */
    List<String> fromRemoteTableName(String remoteDatabaseName, List<String> remoteTableNames);

    /**
     * Maps a list of remote columns in a specified remote database and table to their corresponding local columns.
     *
     * @param remoteDatabaseName the name of the remote database
     * @param remoteTableName the name of the remote table where the columns reside
     * @param remoteColumnNames the list of remote column names to be mapped
     * @return a list of corresponding local columns
     */
    List<String> fromRemoteColumnName(String remoteDatabaseName, String remoteTableName,
            List<String> remoteColumnNames);

    /**
     * Maps a local database name to its corresponding remote database name.
     *
     * @param localDatabaseName the name of the local database to be mapped
     * @return the corresponding remote database name
     */
    String toRemoteDatabaseName(String localDatabaseName);

    /**
     * Maps a local table name in a specified remote database to its corresponding remote table name.
     *
     * @param remoteDatabaseName the name of the remote database where the table resides
     * @param localTableName the name of the local table to be mapped
     * @return the corresponding remote table name
     */
    String toRemoteTableName(String remoteDatabaseName, String localTableName);

    /**
     * Maps local column names in a specified remote database and table to their corresponding remote column names.
     *
     * @param remoteDatabaseName the name of the remote database
     * @param remoteTableName the name of the remote table
     * @return a map of local column names to corresponding remote column names
     */
    Map<String, String> toRemoteColumnNames(String remoteDatabaseName, String remoteTableName);
}

