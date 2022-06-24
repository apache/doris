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
import org.apache.doris.common.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DbPrivEntry extends CatalogPrivEntry {
    protected static final String ANY_DB = "*";

    protected PatternMatcher dbPattern;
    protected String origDb;
    protected boolean isAnyDb;

    protected DbPrivEntry() {
    }

    protected DbPrivEntry(PatternMatcher userPattern, String user,
                          PatternMatcher hostPattern, String origHost,
                          PatternMatcher ctlPattern, String origCtl,
                          PatternMatcher dbPattern, String origDb,
                          boolean isDomain, PrivBitSet privSet) {
        super(userPattern, user, hostPattern, origHost, ctlPattern, origCtl, isDomain, privSet);
        this.dbPattern = dbPattern;
        this.origDb = origDb;
        if (origDb.equals(ANY_DB)) {
            isAnyDb = true;
        }
    }

    public static DbPrivEntry create(
            String user, String host,
            String ctl, String db,
            boolean isDomain, PrivBitSet privs) throws AnalysisException {
        PatternMatcher hostPattern = PatternMatcher.createMysqlPattern(host, CaseSensibility.HOST.getCaseSensibility());

        PatternMatcher ctlPattern = PatternMatcher.createFlatPattern(
                ctl, CaseSensibility.CATALOG.getCaseSensibility(), ctl.equals(ANY_CTL));

        PatternMatcher dbPattern = createDbPatternMatcher(db);

        PatternMatcher userPattern = PatternMatcher.createFlatPattern(user, CaseSensibility.USER.getCaseSensibility());

        if (privs.containsNodePriv() || privs.containsResourcePriv()) {
            throw new AnalysisException("Db privilege can not contains global or resource privileges: " + privs);
        }

        return new DbPrivEntry(userPattern, user, hostPattern, host, ctlPattern, ctl, dbPattern, db, isDomain, privs);
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
        return compareAssist(origUser, otherEntry.origUser,
                             origHost, otherEntry.origHost,
                             origCtl, otherEntry.origCtl,
                             origDb, otherEntry.origDb);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof DbPrivEntry)) {
            return false;
        }

        DbPrivEntry otherEntry = (DbPrivEntry) other;
        return origUser.equals(otherEntry.origUser) && origHost.equals(otherEntry.origHost)
                && origCtl.equals(otherEntry.origCtl) && origDb.equals(otherEntry.origDb)
                && isDomain == otherEntry.isDomain;
    }

    @Override
    public String toString() {
        return String.format("database privilege. user: %s, host: %s, ctl: %s, db: %s, priv: %s, set by resolver: %b",
                origUser, origHost, origCtl, origDb, privSet.toString(), isSetByDomainResolver);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = DbPrivEntry.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }
        super.write(out);
        Text.writeString(out, origDb);
        isClassNameWrote = false;
    }

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

}
