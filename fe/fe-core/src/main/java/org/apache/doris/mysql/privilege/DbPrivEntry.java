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

package org.apache.doris.mysql.privilege;

import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.io.Text;

import java.io.DataInput;
import java.io.IOException;

public class DbPrivEntry extends CatalogPrivEntry {
    protected static final String ANY_DB = "*";

    protected PatternMatcher dbPattern;
    protected String origDb;
    protected boolean isAnyDb;

    protected DbPrivEntry() {
    }

    protected DbPrivEntry(
            PatternMatcher ctlPattern, String origCtl,
            PatternMatcher dbPattern, String origDb,
            PrivBitSet privSet) {
        super(ctlPattern, origCtl, privSet);
        this.dbPattern = dbPattern;
        this.origDb = origDb;
        if (origDb.equals(ANY_DB)) {
            isAnyDb = true;
        }
    }

    public static DbPrivEntry create(
            String ctl, String db, PrivBitSet privs) throws AnalysisException {
        PatternMatcher ctlPattern = PatternMatcher.createFlatPattern(
                ctl, CaseSensibility.CATALOG.getCaseSensibility(), ctl.equals(ANY_CTL));

        PatternMatcher dbPattern = createDbPatternMatcher(db);

        if (privs.containsNodePriv() || privs.containsResourcePriv()) {
            throw new AnalysisException("Db privilege can not contains global or resource privileges: " + privs);
        }

        return new DbPrivEntry(ctlPattern, ctl, dbPattern, db, privs);
    }

    private static PatternMatcher createDbPatternMatcher(String db) throws AnalysisException {
        // the database 'information_schema''s name is case insensibility.
        boolean dbCaseSensibility = CaseSensibility.DATABASE.getCaseSensibility();
        if (ClusterNamespace.getNameFromFullName(db).equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
            dbCaseSensibility = false;
        }

        return PatternMatcher.createFlatPattern(db, dbCaseSensibility, db.equals(ANY_DB));
    }

    public PatternMatcher getDbPattern() {
        return dbPattern;
    }

    public String getOrigDb() {
        return origDb;
    }

    public boolean isAnyDb() {
        return isAnyDb;
    }

    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof DbPrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        DbPrivEntry otherEntry = (DbPrivEntry) other;
        return compareAssist(
                origCtl, otherEntry.origCtl,
                origDb, otherEntry.origDb);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof DbPrivEntry)) {
            return false;
        }

        DbPrivEntry otherEntry = (DbPrivEntry) other;
        return origCtl.equals(otherEntry.origCtl) && origDb.equals(otherEntry.origDb);
    }

    @Override
    public String toString() {
        return String.format("database privilege.ctl: %s, db: %s, priv: %s",
                origCtl, origDb, privSet.toString());
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        origDb = Text.readString(in);
        try {
            dbPattern = createDbPatternMatcher(origDb);
        } catch (AnalysisException e) {
            throw new IOException(e);
        }
        isAnyDb = origDb.equals(ANY_DB);
    }

    @Override
    protected PrivEntry copy() throws AnalysisException, PatternMatcherException {
        return DbPrivEntry.create(this.getOrigCtl(), this.getOrigDb(), this.getPrivSet().copy());
    }
}
