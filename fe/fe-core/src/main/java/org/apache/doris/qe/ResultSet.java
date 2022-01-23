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

import java.util.List;

/**
 * Simplified ResultSet implementation
 */
public interface ResultSet {

    /**
     * Moves the cursor froward one row from its current position.
     *
     * @return <code>true</code> if the new current row is valid;
     *<code>false</code> if there are no more rows
     */
    boolean next();

    /**
     * Return result data in list format
     *
     * @return A list of list data of rows
     */
    List<List<String>> getResultRows();

    /**
     * Retrieves the number, types and properties of
     * this <code>ResultSet</code> object's columns.
     *
     * @return the description of this <code>ResultSet</code> object's columns
     */
    ResultSetMetaData getMetaData();

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as String
     * @param col the column index
     * @return the column value
     */
    String getString(int col);

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as byte
     * @param col the column index
     * @return the column value
     */
    byte getByte(int col);

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as short
     * @param col the column index
     * @return the column value
     */
    short getShort(int col);

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as int
     * @param col the column index
     * @return the column value
     */
    int getInt(int col);

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as long
     * @param col the column index
     * @return the column value
     */
    long getLong(int col);

}
