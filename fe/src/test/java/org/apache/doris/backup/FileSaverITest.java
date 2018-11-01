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

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileSaverITest {

    @Test
    public void test() {
        DirSaver parent1 = new DirSaver("parent1");
        DirSaver parent2 = new DirSaver("parent2");
        parent1.addChild(parent2);
        parent1.addChild(new FileSaver("child1"));
        parent1.addChild(new DirSaver("parent2"));

        FileSaver child2 = new FileSaver("child2");
        parent2.addChild(child2);
        parent2.addChild(new FileSaver("child3"));

        parent1.print("\t");
        parent2.print("\t");

        System.out.println(child2.getFullPath());

        DirSaver root = DirSaver.createWithPath("/");
        if (root != null) {
            Assert.fail();
        }

        root = DirSaver.createWithPath("abc//ghi////ghi/");
        System.out.println(root.getFullPath());
    }

    @Test
    public void test_manifest_read_write() throws IOException {
        DirSaver dir = DirSaver.createWithPath("/a/b/c/d");
        Assert.assertEquals("/a/b/c/d/", dir.getFullPath());

        dir = (DirSaver) dir.getTopParent();
        Assert.assertEquals("/a/", dir.getFullPath());

        dir.addDir("e/f/g").addFile("file1");
        dir.print("#");
        
        Assert.assertTrue(dir.getChild("b").getChild("c").getChild("d") instanceof DirSaver);
        Assert.assertTrue(dir.getChild("e").getChild("f").getChild("g").hasChild("file1"));

        // write
        File file = new File("./manifest");
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        dir.write(out);
        out.flush();
        out.close();

        // read
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        DirSaver readDir = new DirSaver();
        readDir.readFields(in);
        readDir.print("-");

        Assert.assertTrue(readDir.getChild("b").getChild("c").getChild("d") instanceof DirSaver);
        Assert.assertTrue(readDir.getChild("e").getChild("f").getChild("g").hasChild("file1"));

        in.close();
        file.delete();
    }

    @Test
    public void test_manifest_read_write2() throws IOException {
        DirSaver dir = DirSaver.createWithPath("a");
        Assert.assertEquals("/a/", dir.getFullPath());
        Assert.assertFalse(dir.hasChild("file1"));

        // write
        File file = new File("./manifest");
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        dir.write(out);
        out.flush();
        out.close();

        // read
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        DirSaver readDir = new DirSaver();
        readDir.readFields(in);
        readDir.print("-");

        Assert.assertEquals("/a/", readDir.getFullPath());
        Assert.assertFalse(readDir.hasChild("file1"));

        in.close();
        file.delete();
    }

}
