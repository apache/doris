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

import org.apache.doris.common.io.Text;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class DirSaver extends FileSaverI {
    private Map<String, FileSaverI> children;

    public DirSaver() {
        children = Maps.newHashMap();
    }

    public DirSaver(String name) {
        super(name);
        children = Maps.newHashMap();
    }

    public static DirSaver createWithPath(String path) {
        DirSaver current = null;
        String[] nodes = path.split(PathBuilder.PATH_DELIMITER);
        for (int i = 0; i < nodes.length; i++) {
            if (Strings.isNullOrEmpty(nodes[i])) {
                continue;
            }
            DirSaver ds = new DirSaver(nodes[i]);
            if (current != null) {
                current.addChild(ds);
            }
            current = ds;
        }
        return current;
    }

    public void addChild(FileSaverI child) {
        if (!children.containsKey(child.name)) {
            children.put(child.name, child);
            child.setParent(this);
        }
    }

    public FileSaverI delChild(String childName) {
        FileSaverI child = children.remove(childName);
        if (child != null) {
            child.removeParent();
        }
        return child;
    }

    public FileSaverI getChild(String childName) {
        if (children.containsKey(childName)) {
            return children.get(childName);
        }
        return null;
    }

    public Collection<FileSaverI> getChildren() {
        return this.children.values();
    }

    public Collection<String> getChildrenName() {
        return this.children.keySet();
    }

    public FileSaver addFile(String relativePath) {
        DirSaver current = this;
        String[] nodes = relativePath.split(PathBuilder.PATH_DELIMITER);
        for (int i = 0; i < nodes.length; i++) {
            if (Strings.isNullOrEmpty(nodes[i])) {
                continue;
            }

            if (i == nodes.length - 1) {
                if (current.hasChild(nodes[i])) {
                    if (current.getChild(nodes[i]) instanceof DirSaver) {
                        return null;
                    } else {
                        return (FileSaver) current.getChild(nodes[i]);
                    }
                } else {
                    FileSaver fs = new FileSaver(nodes[i]);
                    current.addChild(fs);
                    return (FileSaver) current.getChild(nodes[i]);
                }
            } else {
                if (current.hasChild(nodes[i])) {
                    if (current.getChild(nodes[i]) instanceof FileSaver) {
                        return null;
                    } else {
                        current = (DirSaver) current.getChild(nodes[i]);
                    }
                } else {
                    DirSaver ds = new DirSaver(nodes[i]);
                    current.addChild(ds);
                    current = (DirSaver) current.getChild(nodes[i]);
                }
            }
        }
        return null;
    }

    public DirSaver addDir(String relativePath) {
        DirSaver current = this;
        String[] nodes = relativePath.split(PathBuilder.PATH_DELIMITER);
        for (int i = 0; i < nodes.length; i++) {
            if (Strings.isNullOrEmpty(nodes[i])) {
                continue;
            }

            DirSaver ds = new DirSaver(nodes[i]);
            if (current != null) {
                current.addChild(ds);
            }
            current = (DirSaver) current.getChild(nodes[i]);
        }
        return current;
    }

    public boolean contains(String path) {
        String[] nodes = path.split(PathBuilder.PATH_DELIMITER);
        FileSaverI current = this;
        for (int i = 0; i < nodes.length; i++) {
            if (!current.hasChild(nodes[i])) {
                return false;
            }
            current = current.getChild(nodes[i]);
        }
        return true;
    }

    public static DirSaver read(DataInput in) throws IOException {
        DirSaver dirSaver = new DirSaver();
        dirSaver.readFields(in);
        return dirSaver;
    }

    @Override
    public boolean hasChild(String childName) {
        return children.containsKey(childName);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(0);
        Text.writeString(out, name);

        // copy children
        Map<String, FileSaverI> copiedChildren = Maps.newHashMap(children);

        Queue<FileSaverI> queue = new LinkedList<FileSaverI>();
        for (String childName : copiedChildren.keySet()) {
            queue.add(delChild(childName));
        }

        while (!queue.isEmpty()) {
            // -1: EOF; 0: dir; 1: file
            FileSaverI node = queue.poll();
            if (node instanceof DirSaver) {
                out.writeShort(0);
                DirSaver dir = (DirSaver) node;
                for (FileSaverI fileSaverI : dir.getChildren()) {
                    queue.add(fileSaverI);
                }
            } else {
                out.writeShort(1);
            }
            Text.writeString(out, node.getFullPath());
        }
        out.writeShort(-1);

        // restore children
        for (FileSaverI child : copiedChildren.values()) {
            addChild(child);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readShort(); // skip start 0
        this.name = Text.readString(in); // read dir name

        short flag = -1;
        while ((flag = in.readShort()) != -1) {
            if (flag == 0) {
                String name = Text.readString(in);
                addDir(name);
            } else {
                String name = Text.readString(in);
                addFile(name);
            }
        }
    }
}
