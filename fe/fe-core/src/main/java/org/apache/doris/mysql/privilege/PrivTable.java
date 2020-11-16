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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public abstract class PrivTable implements Writable {
    private static final Logger LOG = LogManager.getLogger(PrivTable.class);

    protected List<PrivEntry> entries = Lists.newArrayList();

    // see PrivEntry for more detail
    protected boolean isClassNameWrote = false;

    /*
     * Add an entry to priv table.
     * If entry already exists and errOnExist is false, we try to reset or merge the new priv entry with existing one.
     * NOTICE, this method does not set password for the newly added entry if this is a user priv table, the caller
     * need to set password later.
     */
    public PrivEntry addEntry(PrivEntry newEntry, boolean errOnExist, boolean errOnNonExist) throws DdlException {
        PrivEntry existingEntry = getExistingEntry(newEntry);
        if (existingEntry == null) {
            if (errOnNonExist) {
                throw new DdlException("User " + newEntry.getUserIdent() + " does not exist");
            }
            entries.add(newEntry);
            Collections.sort(entries);
            LOG.info("add priv entry: {}", newEntry);
            return newEntry;
        } else {
            if (errOnExist) {
                throw new DdlException("User already exist");
            } else {
                checkOperationAllowed(existingEntry, newEntry, "ADD ENTRY");
                // if existing entry is set by domain resolver, just replace it with the new entry.
                // if existing entry is not set by domain resolver, merge the 2 entries.
                if (existingEntry.isSetByDomainResolver()) {
                    existingEntry.setPrivSet(newEntry.getPrivSet());
                    existingEntry.setSetByDomainResolver(newEntry.isSetByDomainResolver());
                    LOG.debug("reset priv entry: {}", existingEntry);
                } else if (!newEntry.isSetByDomainResolver()) {
                    mergePriv(existingEntry, newEntry);
                    existingEntry.setSetByDomainResolver(false);
                    LOG.debug("merge priv entry: {}", existingEntry);
                }
            }
            return existingEntry;
        }
    }

    public List<PrivEntry> getEntries() {
        return entries;
    }

    public void dropEntry(PrivEntry entry) {
        Iterator<PrivEntry> iter = entries.iterator();
        while (iter.hasNext()) {
            PrivEntry privEntry = iter.next();
            if (privEntry.keyMatch(entry)) {
                iter.remove();
                LOG.info("drop priv entry: {}", privEntry);
                break;
            }
        }
    }

    public void clearEntriesSetByResolver() {
        Iterator<PrivEntry> iter = entries.iterator();
        while (iter.hasNext()) {
            PrivEntry privEntry = iter.next();
            if (privEntry.isSetByDomainResolver()) {
                iter.remove();
                LOG.info("drop priv entry set by resolver: {}", privEntry);
            }
        }
    }

    // drop all entries which user name are matched, and is not set by resolver
    public void dropUser(UserIdentity userIdentity) {
        Iterator<PrivEntry> iter = entries.iterator();
        while (iter.hasNext()) {
            PrivEntry privEntry = iter.next();
            if (privEntry.match(userIdentity, true /* exact match */) && !privEntry.isSetByDomainResolver()) {
                iter.remove();
                LOG.info("drop entry: {}", privEntry);
            }
        }
    }

    public void revoke(PrivEntry entry, boolean errOnNonExist, boolean deleteEntryWhenEmpty) throws DdlException {
        PrivEntry existingEntry = getExistingEntry(entry);
        if (existingEntry == null) {
            if (errOnNonExist) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NONEXISTING_GRANT, entry.getOrigUser(),
                        entry.getOrigHost());
            }
            return;
        }

        checkOperationAllowed(existingEntry, entry, "REVOKE");

        // check if privs to be revoked exist in priv entry.
        PrivBitSet tmp = existingEntry.getPrivSet().copy();
        tmp.and(entry.getPrivSet());
        if (tmp.isEmpty()) {
            if (errOnNonExist) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NONEXISTING_GRANT, entry.getOrigUser(),
                        entry.getOrigHost());
            }
            // there is no such priv, nothing need to be done
            return;
        }

        // revoke privs from existing priv entry
        LOG.debug("before revoke: {}, privs to be revoked: {}",
                  existingEntry.getPrivSet(), entry.getPrivSet());
        tmp = existingEntry.getPrivSet().copy();
        tmp.xor(entry.getPrivSet());
        existingEntry.getPrivSet().and(tmp);
        LOG.debug("after revoke: {}", existingEntry);

        if (existingEntry.getPrivSet().isEmpty() && deleteEntryWhenEmpty) {
            // no priv exists in this entry, remove it
            dropEntry(existingEntry);
        }
    }

    /*
     * the priv entry is classified by 'set by domain resolver'
     * or 'NOT set by domain resolver'(other specified operations).
     * if the existing entry is set by resolver, it can be reset by resolver or set by specified ops.
     * in other word, if the existing entry is NOT set by resolver, it can not be set by resolver.
     */
    protected void checkOperationAllowed(PrivEntry existingEntry, PrivEntry newEntry, String op) throws DdlException {
        if (!existingEntry.isSetByDomainResolver() && newEntry.isSetByDomainResolver()) {
            throw new DdlException("the existing entry is NOT set by resolver: " + existingEntry + ","
                    + " can not be set by resolver " + newEntry + ", op: " + op);
        }
    }

    // Get existing entry which is the keys match the given entry
    protected PrivEntry getExistingEntry(PrivEntry entry) {
        for (PrivEntry existingEntry : entries) {
            if (existingEntry.keyMatch(entry)) {
                return existingEntry;
            }
        }
        return null;
    }

    private void mergePriv(PrivEntry first, PrivEntry second) {
        first.getPrivSet().or(second.getPrivSet());
        first.setSetByDomainResolver(first.isSetByDomainResolver() || second.isSetByDomainResolver());
    }

    public boolean doesUsernameExist(String qualifiedUsername) {
        for (PrivEntry entry : entries) {
            if (entry.getOrigUser().equals(qualifiedUsername)) {
                return true;
            }
        }
        return false;
    }

    // for test only
    public void clear() {
        entries.clear();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public static PrivTable read(DataInput in) throws IOException {
        String className = Text.readString(in);
        if (className.startsWith("com.baidu.palo")) {
            // we need to be compatible with former class name
            className = className.replaceFirst("com.baidu.palo", "org.apache.doris");
        }
        PrivTable privTable = null;
        try {
            Class<? extends PrivTable> derivedClass = (Class<? extends PrivTable>) Class.forName(className);
            privTable = derivedClass.newInstance();
            Class[] paramTypes = { DataInput.class };
            Method readMethod = derivedClass.getMethod("readFields", paramTypes);
            Object[] params = { in };
            readMethod.invoke(privTable, params);

            return privTable;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                | SecurityException | IllegalArgumentException | InvocationTargetException e) {
            throw new IOException("failed read PrivTable", e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\n");
        for (PrivEntry privEntry : entries) {
            sb.append(privEntry).append("\n");
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = PrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }
        out.writeInt(entries.size());
        for (PrivEntry privEntry : entries) {
            privEntry.write(out);
        }
        isClassNameWrote = false;
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            PrivEntry entry = PrivEntry.read(in);
            entries.add(entry);
        }
        Collections.sort(entries);
    }

}
