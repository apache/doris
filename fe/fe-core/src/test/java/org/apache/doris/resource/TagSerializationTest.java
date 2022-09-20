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

package org.apache.doris.resource;


import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/*
 * Author: Chenmingyu
 * Date: Dec 9, 2019
 */

public class TagSerializationTest {

    private static final String fileName = "./TagSerializationTest";

    @After
    public void tearDown() throws IOException {
        Files.deleteIfExists(Paths.get(fileName));
    }

    @Test
    public void testSerializeTag() throws IOException, AnalysisException {
        // 1. Write objects to file
        final Path path = Files.createFile(Paths.get(fileName));
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        Tag tag = Tag.create(Tag.TYPE_LOCATION, "rack1");
        tag.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        Tag readTag = Tag.read(in);
        Assert.assertEquals(tag, readTag);
    }

    @Test
    public void testSerializeTagSet() throws IOException, AnalysisException {
        // 1. Write objects to file
        final Path path = Files.createFile(Paths.get(fileName));
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        TagSet tagSet = TagSet.create(Tag.create(Tag.TYPE_LOCATION, "rack1"), Tag.create(Tag.TYPE_LOCATION, "rack2"),
                Tag.create(Tag.TYPE_ROLE, "backend"));
        tagSet.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        TagSet readTag = TagSet.read(in);
        Assert.assertEquals(tagSet, readTag);
    }

    @Test
    public void testSerializeTagManager() throws IOException, AnalysisException {
        // 1. Write objects to file
        final Path path = Files.createFile(Paths.get(fileName));
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        TagManager tagManager = new TagManager();
        tagManager.addResourceTag(1L,
                Tag.create(Tag.TYPE_LOCATION, "rack1"));
        tagManager.addResourceTags(2L,
                TagSet.create(Tag.create(Tag.TYPE_LOCATION, "rack1"),  Tag.create(Tag.TYPE_LOCATION, "rack2")));
        tagManager.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        TagManager readTagManager = TagManager.read(in);
        Assert.assertEquals(Sets.newHashSet(1L, 2L),
                readTagManager.getResourceIdsByTag(Tag.create(Tag.TYPE_LOCATION, "rack1")));
        Assert.assertEquals(Sets.newHashSet(2L),
                readTagManager.getResourceIdsByTags(TagSet.create(Tag.create(Tag.TYPE_LOCATION, "rack2"))));

        in.close();
    }
}
