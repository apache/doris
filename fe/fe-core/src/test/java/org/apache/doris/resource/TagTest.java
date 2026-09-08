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

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TagTest {

    @Test
    public void testTagName1() throws AnalysisException {
        Assertions.assertThrows(AnalysisException.class, () -> {
            Tag.create("location", "_tag1");
        });
    }

    @Test
    public void testTagName2() throws AnalysisException {
        Assertions.assertThrows(AnalysisException.class, () -> {
            Tag.create("location", "asdlajwdjdawhkjldjawlkdjawldjlkwasdasdsadasdd");
        });
    }

    @Test
    public void testTagName3() throws AnalysisException {
        Tag.create("unknown", "test1");
        Tag.create("unknown", "Test1");
        Tag.create("unknown", "tTest1");
    }

    @Test
    public void testTagName4() throws AnalysisException {
        Tag tag = Tag.create("location", "zone1");
        Assertions.assertEquals("{\"location\" : \"zone1\"}", tag.toString());
    }

    @Test
    public void testTagSet1() throws AnalysisException {
        Map<String, String> map = Maps.newHashMap();
        map.put("location", "zone1, zone2");
        map.put("unknown", "tag1, tag2");
        TagSet.create(map);
    }

    @Test
    public void testTagSet2() throws AnalysisException {
        Assertions.assertThrows(AnalysisException.class, () -> {
            Map<String, String> map = Maps.newHashMap();
            map.put("location", "zone1, zone2");
            map.put("type", "tag1, _tag2");
            TagSet.create(map);
        });
    }

    @Test
    public void testTagSet3() throws AnalysisException {
        Map<String, String> map = Maps.newHashMap();
        map.put("location", "zone1, zone2");
        map.put("type", "backend");
        map.put("function", "store,computation");
        TagSet tagSet = TagSet.create(map);
        Assertions.assertTrue(tagSet.containsTag(Tag.create("location", "zone1")));
        Assertions.assertTrue(tagSet.containsTag(Tag.create("location", "zone2")));
        Assertions.assertTrue(tagSet.containsTag(Tag.create("type", "backend")));
        Assertions.assertTrue(tagSet.containsTag(Tag.create("function", "store")));
        Assertions.assertTrue(tagSet.containsTag(Tag.create("function", "computation")));
        Assertions.assertFalse(tagSet.containsTag(Tag.create("function", "load")));

        // test union
        Map<String, String> map2 = Maps.newHashMap();
        map2.put("function", "load");
        TagSet tagSet2 = TagSet.create(map2);
        tagSet.union(tagSet2);
        Assertions.assertTrue(tagSet.containsTag(Tag.create("function", "store")));
        Assertions.assertTrue(tagSet.containsTag(Tag.create("function", "computation")));
        Assertions.assertTrue(tagSet.containsTag(Tag.create("function", "load")));

        // test substitute merge
        tagSet.substituteMerge(tagSet2);
        Assertions.assertFalse(tagSet.containsTag(Tag.create("function", "store")));
        Assertions.assertFalse(tagSet.containsTag(Tag.create("function", "computation")));
        Assertions.assertTrue(tagSet.containsTag(Tag.create("function", "load")));
    }
}
