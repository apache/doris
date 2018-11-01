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

import org.apache.doris.common.DdlException;
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

    public void addEntry(PrivEntry newEntry, boolean errOnExist, boolean errOnNonExist) throws DdlException {
        PrivEntry existingEntry = getExistingEntry(newEntry);
        if (existingEntry == null) {
            if (errOnNonExist) {
                throw new DdlException("User " + newEntry.getUserIdent() + " does not exist");
            }
            entries.add(newEntry);
            Collections.sort(entries);
            LOG.info("add priv entry: {}", newEntry);
        } else {
            if (errOnExist) {
                throw new DdlException("User already exist");
            } else {
                if (!checkOperationAllowed(existingEntry, newEntry, "ADD ENTRY")) {
                    return;
                } else {
                    if (existingEntry.isSetByDomainResolver() && newEntry.isSetByDomainResolver()) {
                        existingEntry.setPrivSet(newEntry.getPrivSet());
                        LOG.debug("reset priv entry: {}", existingEntry);
                    } else if (existingEntry.isSetByDomainResolver() && !newEntry.isSetByDomainResolver()
                            || !existingEntry.isSetByDomainResolver() && !newEntry.isSetByDomainResolver()) {
                        mergePriv(existingEntry, newEntry);
                        existingEntry.setSetByDomainResolver(false);
                        LOG.info("merge priv entry: {}", existingEntry);
                    }
                    return;
                }
            }
        }

        return;
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

    // drop all entries which user name are matched
    public void dropUser(String qualifiedUser) {
        Iterator<PrivEntry> iter = entries.iterator();
        while (iter.hasNext()) {
            PrivEntry privEntry = iter.next();
            if (privEntry.getOrigUser().equals(qualifiedUser)) {
                iter.remove();
                LOG.info("drop entry: {}", privEntry);
            }
        }
    }

    public boolean revoke(PrivEntry entry, boolean errOnNonExist, boolean deleteEntryWhenEmpty) {
        PrivEntry existingEntry = getExistingEntry(entry);
        if (existingEntry == null && errOnNonExist) {
            return false;
        }

        if (!checkOperationAllowed(existingEntry, entry, "REVOKE")) {
            return true;
        }

        // check if privs to be revoked exist in priv entry.
        PrivBitSet tmp = existingEntry.getPrivSet().copy();
        tmp.and(entry.getPrivSet());
        if (tmp.isEmpty()) {
            return !errOnNonExist;
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

        return true;
    }

    /*
     * the priv entry is classified by 'set by domain resolver'
     * or 'NOT set by domain resolver'(other specified operations).
     * if the existing entry is set by resolver, it can be reset by resolver or set by specified ops.
     * if the existing entry is NOT set by resolver, it can not be set by resolver.
     */
    protected boolean checkOperationAllowed(PrivEntry existingEntry, PrivEntry newEntry, String op) {
        if (!existingEntry.isSetByDomainResolver() && newEntry.isSetByDomainResolver()) {
            LOG.debug("the existing entry is NOT set by resolver: {}, can not be set by resolver {}, op: {}",
                      existingEntry, newEntry);
            return false;
        } else if (existingEntry.isSetByDomainResolver() && !newEntry.isSetByDomainResolver()) {
            LOG.debug("the existing entry is currently set by resolver: {}, be set by ops now: {}, op: {}",
                      existingEntry, newEntry);
            return true;
        }
        return true;
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

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            PrivEntry entry = PrivEntry.read(in);
            entries.add(entry);
        }
        Collections.sort(entries);
    }

}
