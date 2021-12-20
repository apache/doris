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

package org.apache.doris.qe;

import org.apache.doris.catalog.Column;

import java.util.List;

/**
 * A ResultSetMetaData object can be used to find out about the types and
 * properties of the columns in a ResultSet
 */
public interface ResultSetMetaData {

    /**
     * Whats the number of columns in the ResultSet?
     * @return the number
     */
    int getColumnCount();

    /**
     * Get all columns
     * @return all the columns as list
     */
    List<Column> getColumns();

    /**
     * Get a column at some index
     * @param idx the index of column
     * @return column data
     */
    Column getColumn(int idx);

    /**
     * Remove a column at some index
     * @param idx the index of column
     * @return column data
     */
    void removeColumn(int idx);
}
