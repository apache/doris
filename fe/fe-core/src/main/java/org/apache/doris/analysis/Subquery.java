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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/Subquery.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.thrift.TExprNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class representing a subquery. A Subquery consists of a QueryStmt and has
 * its own Analyzer context.
 */
public class Subquery extends Expr {
    private static final Logger LOG = LoggerFactory.getLogger(Subquery.class);

    // A subquery has its own analysis context
    protected Analyzer analyzer;

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    @Override
    public String toSqlImpl() {
        return "";
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        return "";
    }

    @Override
    public String toDigestImpl() {
        return "";
    }

    /**
     * Copy c'tor.
     */
    public Subquery(Subquery other) {
        super(other);
        analyzer = other.analyzer;
    }

    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    /**
     * Returns true if the toSql() of the Subqueries is identical. May return false for
     * equivalent statements even due to minor syntactic differences like parenthesis.
     * TODO: Switch to a less restrictive implementation.
     */
    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        return false;
    }

    @Override
    public Subquery clone() {
        Subquery ret = new Subquery(this);
        if (LOG.isDebugEnabled()) {
            LOG.debug("SUBQUERY clone old={} new={}",
                    System.identityHashCode(this),
                    System.identityHashCode(ret));
        }
        return ret;
    }

    @Override
    public Expr reset() {
        super.reset();
        analyzer = null;
        return this;
    }

    @Override
    protected void toThrift(TExprNode msg) {}

    @Override
    public boolean supportSerializable() {
        return false;
    }
}
