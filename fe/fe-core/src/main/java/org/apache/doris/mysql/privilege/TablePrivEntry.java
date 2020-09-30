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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TablePrivEntry extends DbPrivEntry {
    private static final String ANY_TBL = "*";

    private PatternMatcher tblPattern;
    private String origTbl;
    private boolean isAnyTbl;

    protected TablePrivEntry() {
    }

    private TablePrivEntry(PatternMatcher hostPattern, String origHost, PatternMatcher dbPattern, String origDb,
            PatternMatcher userPattern, String user, PatternMatcher tblPattern, String origTbl,
            boolean isDomain, PrivBitSet privSet) {
        super(hostPattern, origHost, dbPattern, origDb, userPattern, user, isDomain, privSet);
        this.tblPattern = tblPattern;
        this.origTbl = origTbl;
        if (origTbl.equals(ANY_TBL)) {
            isAnyTbl = true;
        }
    }

    public static TablePrivEntry create(String host, String db, String user, String tbl, boolean isDomain,
            PrivBitSet privs) throws AnalysisException {
        PatternMatcher hostPattern = PatternMatcher.createMysqlPattern(host, CaseSensibility.HOST.getCaseSensibility());
        PatternMatcher dbPattern = PatternMatcher.createMysqlPattern(db.equals(ANY_DB) ? "%" : db,
                                                                     CaseSensibility.DATABASE.getCaseSensibility());
        PatternMatcher userPattern = PatternMatcher.createMysqlPattern(user, CaseSensibility.USER.getCaseSensibility());

        PatternMatcher tblPattern = PatternMatcher.createMysqlPattern(tbl.equals(ANY_TBL) ? "%" : tbl,
                                                                      CaseSensibility.TABLE.getCaseSensibility());

        if (privs.containsNodePriv() || privs.containsResourcePriv()) {
            throw new AnalysisException("Table privilege can not contains global or resource privileges: " + privs);
        }

        return new TablePrivEntry(hostPattern, host, dbPattern, db, userPattern, user, tblPattern, tbl, isDomain, privs);
    }

    public PatternMatcher getTblPattern() {
        return tblPattern;
    }

    public String getOrigTbl() {
        return origTbl;
    }

    public boolean isAnyTbl() {
        return isAnyTbl;
    }

    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof TablePrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        TablePrivEntry otherEntry = (TablePrivEntry) other;
        int res = origHost.compareTo(otherEntry.origHost);
        if (res != 0) {
            return -res;
        }

        res = origDb.compareTo(otherEntry.origDb);
        if (res != 0) {
            return -res;
        }

        res = origUser.compareTo(otherEntry.origUser);
        if (res != 0) {
            return -res;
        }

        return -origTbl.compareTo(otherEntry.origTbl);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof TablePrivEntry)) {
            return false;
        }

        TablePrivEntry otherEntry = (TablePrivEntry) other;
        if (origHost.equals(otherEntry.origHost) && origUser.equals(otherEntry.origUser)
                && origDb.equals(otherEntry.origDb) && origTbl.equals(otherEntry.origTbl)
                && isDomain == otherEntry.isDomain) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("db priv. host: ").append(origHost).append(", db: ").append(origDb);
        sb.append(", user: ").append(origUser).append(", tbl: ").append(origTbl);
        sb.append(", priv: ").append(privSet).append(", set by resolver: ").append(isSetByDomainResolver);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = TablePrivEntry.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }
        super.write(out);

        Text.writeString(out, origTbl);

        isClassNameWrote = false;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        origTbl = Text.readString(in);
        try {
            tblPattern = PatternMatcher.createMysqlPattern(origTbl, CaseSensibility.TABLE.getCaseSensibility());
        } catch (AnalysisException e) {
            throw new IOException(e);
        }
        isAnyTbl = origTbl.equals(ANY_TBL);
    }

}
