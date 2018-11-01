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

package org.apache.doris.backup;

import org.apache.doris.common.io.Writable;

import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FileSaverI implements Writable {
    protected String name;

    protected DirSaver parent;

    public FileSaverI() {

    }

    public FileSaverI(String name) {
        this.name = name;
        parent = null;
    }

    public String getName() {
        return name;
    }

    public void setName(String newName) {
        name = newName;
    }

    public void setParent(DirSaver parent) {
        this.parent = parent;
    }

    public DirSaver getParent() {
        return parent;
    }

    public FileSaverI getTopParent() {
        if (parent == null) {
            return this;
        } else {
            return parent.getTopParent();
        }
    }

    public void removeParent() {
        parent = null;
    }

    public FileSaverI getChild(String childName) {
        return null;
    }

    public boolean hasChild(String childName) {
        return false;
    }

    public String getFullPath() {
        StringBuilder sb = new StringBuilder();
        FileSaverI current = this;
        if (this instanceof FileSaver) {
            sb.insert(0, name);
        } else {
            sb.insert(0, name + PathBuilder.PATH_DELIMITER);
        }
        if (current.getParent() != null) {
            DirSaver parent = current.getParent();
            sb.insert(0, parent.getFullPath());
        } else {
            sb.insert(0, PathBuilder.PATH_DELIMITER);
        }
        return sb.toString();
    }

    public void print(String prefix) {
        print(prefix, 0);
    }

    private void print(String prefix, int level) {
        String pre = prefix;
        for (int i = 0; i < level; i++) {
            pre += prefix;
        }
        System.out.println(pre + name);
        if (this instanceof DirSaver) {
            DirSaver dirSaver = (DirSaver) this;
            for (FileSaverI fileSaverI : dirSaver.getChildren()) {
                fileSaverI.print(prefix, level + 1);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FileSaverI)) {
            return false;
        }
        FileSaverI fileSaverI = (FileSaverI) obj;
        if (!name.equals(fileSaverI.name)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new NotImplementedException();
    }
}

