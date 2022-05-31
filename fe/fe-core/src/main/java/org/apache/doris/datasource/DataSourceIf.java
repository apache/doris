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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 *
 */
public interface DataSourceIf {

    // Type of this data source
    String getType();

    // Name of this data source
    String getName();

    @Nullable
    Database getDbNullable(String dbName);

    @Nullable
    Database getDbNullable(long dbId);

    Optional<Database> getDb(String dbName);

    Optional<Database> getDb(long dbId);

    <E extends Exception> Database getDbOrException(String dbName, java.util.function.Function<String, E> e) throws E;

    <E extends Exception> Database getDbOrException(long dbId, java.util.function.Function<Long, E> e) throws E;

    Database getDbOrMetaException(String dbName) throws MetaNotFoundException;

    Database getDbOrMetaException(long dbId) throws MetaNotFoundException;

    Database getDbOrDdlException(String dbName) throws DdlException;

    Database getDbOrDdlException(long dbId) throws DdlException;

    Database getDbOrAnalysisException(String dbName) throws AnalysisException;

    Database getDbOrAnalysisException(long dbId) throws AnalysisException;
}
