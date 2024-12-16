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

package org.apache.doris.qe;

import org.apache.doris.common.UserException;
import org.apache.doris.qe.help.HelpCategory;
import org.apache.doris.qe.help.HelpObjectLoader;
import org.apache.doris.qe.help.HelpTopic;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

public class HelpObjectLoaderTest {

    @Test
    public void testTopicNormal() throws IOException, UserException {
        URL resource = getClass().getClassLoader().getResource("data/helpTopicNormal.md");
        HelpObjectLoader<HelpTopic> loader = HelpObjectLoader.createTopicLoader();
        List<HelpTopic> helpTopics = loader.loadAll(resource.getFile());
        Assert.assertNotNull(helpTopics);
        Assert.assertEquals(2, helpTopics.size());

        for (HelpTopic topic : helpTopics) {
            if (topic.getName().equals("SHOW TABLES")) {
                Assert.assertTrue(Arrays.equals(Lists.newArrayList("SHOW", "TABLES").toArray(),
                        topic.getKeywords().toArray()));
                Assert.assertEquals("Administration\n", topic.getCategory());
                Assert.assertEquals("", topic.getUrl());
                Assert.assertEquals("show table in this\n"
                        + "SYNTAX: SHOW TABLES\n", topic.getDescription());
                Assert.assertEquals("show tables\n", topic.getExample());
            } else {
                // SHOW DATABASES
                Assert.assertEquals("SHOW DATABASES", topic.getName());
                Assert.assertTrue(Arrays.equals(Lists.newArrayList("SHOW", "DATABASES").toArray(),
                        topic.getKeywords().toArray()));
                Assert.assertEquals("", topic.getCategory());
                Assert.assertEquals("", topic.getUrl());
                Assert.assertEquals("show table in this\n"
                        + "    SYNTAX: SHOW DATABASES\n", topic.getDescription());
                Assert.assertEquals("", topic.getExample());
            }
        }
    }

    @Test
    public void testCategoryNormal() throws IOException, UserException {
        URL resource = getClass().getClassLoader().getResource("data/helpCategoryNormal.md");

        HelpObjectLoader<HelpCategory> loader = HelpObjectLoader.createCategoryLoader();
        List<HelpCategory> helpTopics = loader.loadAll(resource.getFile());
        Assert.assertNotNull(helpTopics);
        Assert.assertEquals(2, helpTopics.size());

        for (HelpCategory topic : helpTopics) {
            if (topic.getName().equals("Polygon properties")) {
                Assert.assertEquals("", topic.getUrl());
                Assert.assertEquals("Geographic Features\n", topic.getParent());
            } else if (topic.getName().equals("Geographic")) {
                Assert.assertEquals("", topic.getUrl());
                Assert.assertEquals("", topic.getParent());
            }
        }
    }
}
