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

package org.apache.doris.common;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class MarkDownParserTest {

    @Test
    public void testNormal() throws UserException {
        List<String> lines = Lists.newArrayList();
        lines.add("## SHOW TABLES");
        lines.add("### name");
        lines.add("SHOW TABLES");
        lines.add("### description");
        lines.add("SYNTAX:");
        lines.add("\tSHOW TABLES [FROM] database");
        lines.add("### example");
        lines.add("show tables;");
        lines.add("### keywords");
        lines.add("SHOW, TABLES");
        lines.add("### url");
        lines.add("http://www.baidu.com");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
        Assert.assertNotNull(map.get("show taBLES"));
        Assert.assertEquals("SHOW TABLES\n", map.get("SHOW TABLES").get("name"));
        Assert.assertEquals("SYNTAX:\n\tSHOW TABLES [FROM] database\n", map.get("SHOW TABLES").get("description"));
        Assert.assertEquals("show tables;\n", map.get("SHOW TABLES").get("example"));
        Assert.assertEquals("SHOW, TABLES\n", map.get("SHOW TABLES").get("keywords"));
        Assert.assertEquals("http://www.baidu.com\n", map.get("SHOW TABLES").get("url"));
        for (Map.Entry<String, Map<String, String>> doc : map.entrySet()) {
            Assert.assertEquals("SHOW TABLES\n", doc.getValue().get("NAme"));
        }
    }

    @Test
    public void testMultiDoc() throws UserException {
        List<String> lines = Lists.newArrayList();
        lines.add(" name");
        lines.add("## SHOW TABLES");
        lines.add("### name");
        lines.add("SHOW TABLES");
        lines.add("### description");
        lines.add("SYNTAX:\n\tSHOW TABLES [FROM] database");
        lines.add("### example");
        lines.add("show tables;");
        lines.add("### keywords");
        lines.add("SHOW, TABLES");
        lines.add("### url");
        lines.add("http://www.baidu.com");
        lines.add("## SHOW DATABASES");
        lines.add("### description");
        lines.add("### keywords");
        lines.add("## DATABASES");
        lines.add("### description");
        lines.add("### keywords");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
        Assert.assertNotNull(map.get("SHOW TABLES"));
        Assert.assertEquals("SHOW TABLES\n", map.get("SHOW TABLES").get("name"));
        Assert.assertEquals("SYNTAX:\n\tSHOW TABLES [FROM] database\n", map.get("SHOW TABLES").get("description"));
        Assert.assertEquals("show tables;\n", map.get("SHOW TABLES").get("example"));
        Assert.assertEquals("SHOW, TABLES\n", map.get("SHOW TABLES").get("keywords"));
        Assert.assertEquals("http://www.baidu.com\n", map.get("SHOW TABLES").get("url"));
        Assert.assertNotNull(map.get("SHOW DATABASES"));
        Assert.assertNotNull(map.get("DATABASES"));
        Assert.assertNull(map.get("DATABASES abc"));
    }

    @Test
    public void testNoDoc() throws UserException {
        List<String> lines = Lists.newArrayList();
        lines.add(" SHOW TABLES");
        lines.add(" name");
        lines.add("SHOW TABLES");
        lines.add(" description");
        lines.add("SYNTAX:\n\tSHOW TABLES [FROM] database");
        lines.add(" example");
        lines.add("show tables;");
        lines.add(" keyword");
        lines.add("SHOW, TABLES");
        lines.add(" url");
        lines.add("http://www.baidu.com");
        lines.add(" SHOW DATABASES");
        lines.add(" DATABASES");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
        Assert.assertNull(map.get("SHOW TABLES"));
        Assert.assertNull(map.get("SHOW DATABASES"));
        Assert.assertNull(map.get("DATABASES"));
        Assert.assertNull(map.get("DATABASES abc"));
    }

    @Test(expected = UserException.class)
    public void testNoFirst() throws UserException {
        List<String> lines = Lists.newArrayList();
        lines.add("## SHOW TABLES");
        MarkDownParser parser = new MarkDownParser(lines);
        parser.parse();
        Assert.fail("No exception throws.");
    }

    @Test
    public void testMultiHeadLevel() throws UserException {
        List<String> lines = Lists.newArrayList();
        lines.add("## SHOW TABLES");
        lines.add("### name");
        lines.add(" SHOW TABLES");
        lines.add("### description");
        lines.add("####Syntax");
        lines.add("SYNTAX:\n\tSHOW TABLES [FROM] database");
        lines.add("####Parameter");
        lines.add(">table_name");
        lines.add("### example");
        lines.add("show tables;");
        lines.add("#### Exam1");
        lines.add("exam1");
        lines.add("### keywords");
        lines.add("SHOW, TABLES");
        lines.add("### url");
        lines.add("http://www.baidu.com");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
        Assert.assertNotNull(map.get("SHOW TABLES"));
        Assert.assertEquals(" SHOW TABLES\n", map.get("SHOW TABLES").get("name"));
        Assert.assertEquals("####Syntax\nSYNTAX:\n\tSHOW TABLES [FROM] database\n####Parameter\n>table_name\n", map.get("SHOW TABLES").get("description"));
        Assert.assertEquals("show tables;\n#### Exam1\nexam1\n", map.get("SHOW TABLES").get("example"));
        Assert.assertEquals("SHOW, TABLES\n", map.get("SHOW TABLES").get("keywords"));
        Assert.assertEquals("http://www.baidu.com\n", map.get("SHOW TABLES").get("url"));
    }

    // the level of "description" is wrong
    @Test(expected = DdlException.class)
    public void testEmptyTitle() throws UserException {
        List<String> lines = Lists.newArrayList();
        lines.add("#");
        lines.add("## ");
        lines.add("SHOW TABLES");
        lines.add("## description");
        lines.add("SYNTAX:\n\tSHOW TABLES [FROM] database");
        lines.add("### example");
        lines.add("show tables;");
        lines.add("### keywords");
        lines.add("SHOW, TABLES");
        lines.add("### url");
        lines.add("http://www.baidu.com");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
    }

    // no valid topic
    @Test
    public void testOneName() throws UserException {
        List<String> lines = Lists.newArrayList();
        lines.add("# TABLES");
        lines.add("# TABLE");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
        Assert.assertTrue(map.isEmpty());
    }
}
