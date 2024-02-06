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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Query.java
// and modified by Doris

package org.apache.doris.plsql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Query {
    String sql;
    Connection conn;
    Statement stmt;
    PreparedStatement pstmt;
    ResultSet rs;
    Exception exception;

    Query() {
    }

    public Query(String sql) {
        this.sql = sql;
    }

    /**
     * Set query objects
     */
    public void set(Connection conn, Statement stmt, ResultSet rs) {
        this.conn = conn;
        this.stmt = stmt;
        this.rs = rs;
    }

    public void set(Connection conn, PreparedStatement pstmt) {
        this.conn = conn;
        this.pstmt = pstmt;
    }

    /**
     * Close statement results
     */
    public void closeStatement() {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
            if (pstmt != null) {
                pstmt.close();
                pstmt = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Set SQL statement
     */
    public void setSql(String sql) {
        this.sql = sql;
    }

    /**
     * Set an execution error
     */
    public void setError(Exception e) {
        exception = e;
    }

    /**
     * Print error stack trace
     */
    public void printStackTrace() {
        if (exception != null) {
            exception.printStackTrace();
        }
    }

    /**
     * Get the result set object
     */
    public ResultSet getResultSet() {
        return rs;
    }

    /**
     * Get the prepared statement object
     */
    public PreparedStatement getPreparedStatement() {
        return pstmt;
    }

    /**
     * Get the connection object
     */
    public Connection getConnection() {
        return conn;
    }

    /**
     * Return error information
     */
    public boolean error() {
        return exception != null;
    }

    public String errorText() {
        if (exception != null) {
            if (exception instanceof ClassNotFoundException) {
                return "ClassNotFoundException: " + exception.getMessage();
            }
            return exception.getMessage();
        }
        return "";
    }

    public Exception getException() {
        return exception;
    }
}
