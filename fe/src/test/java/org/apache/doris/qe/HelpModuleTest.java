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

import mockit.Expectations;
import mockit.Injectable;
import org.apache.doris.common.UserException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HelpModuleTest {
    private List<HelpCategory> categories;
    private List<HelpTopic> topics;

    // Category
    //  Admin
    //      - Show
    //      - Select
    // Topic
    //      - SHOW TABLES
    //      - SELECT TIME
    @Before
    public void setUp() {
        categories = Lists.newArrayList();
        topics = Lists.newArrayList();

        HelpCategory category = new HelpCategory();
        Map<String, String> map = Maps.newHashMap();
        map.put("parent", "Admin");
        Map.Entry<String, Map<String, String>> entry = Maps.immutableEntry("Show", map);
        category.loadFrom(entry);
        categories.add(category);

        category = new HelpCategory();
        map = Maps.newHashMap();
        map.put("parent", "Admin");
        entry = Maps.immutableEntry("Select", map);
        category.loadFrom(entry);
        categories.add(category);

        category = new HelpCategory();
        map = Maps.newHashMap();
        entry = Maps.immutableEntry("Admin", map);
        category.loadFrom(entry);
        categories.add(category);

        // Topic
        HelpTopic topic = new HelpTopic();
        map = Maps.newHashMap();
        map.put("keyword", "SHOW, TABLES");
        map.put("category", "Show");
        entry = Maps.immutableEntry("SHOW TABLES", map);
        topic.loadFrom(entry);
        topics.add(topic);

        topic = new HelpTopic();
        map = Maps.newHashMap();
        map.put("keyword", "SELECT");
        map.put("category", "Select");
        entry = Maps.immutableEntry("SELECT TIME", map);
        topic.loadFrom(entry);
        topics.add(topic);

        // emtpy
        topic = new HelpTopic();
        map = Maps.newHashMap();
        entry = Maps.immutableEntry("empty", map);
        topic.loadFrom(entry);
        topics.add(topic);
    }

    @Test
    public void testNormal() throws IOException, UserException {
        // Mock
        // HelpObjectLoader categoryLoader = EasyMock.createMock(HelpObjectLoader.class);
        // EasyMock.expect(categoryLoader.loadAll(EasyMock.isA(String.class))).andReturn(categories).anyTimes();
        // EasyMock.replay(categoryLoader);
        // HelpObjectLoader topicLoader = EasyMock.createMock(HelpObjectLoader.class);
        // EasyMock.expect(topicLoader.loadAll(EasyMock.isA(String.class))).andReturn(topics).anyTimes();
        // EasyMock.replay(topicLoader);
        // PowerMock.mockStatic(HelpObjectLoader.class);
        // EasyMock.expect(HelpObjectLoader.createCategoryLoader()).andReturn(categoryLoader).anyTimes();
        // EasyMock.expect(HelpObjectLoader.createTopicLoader()).andReturn(topicLoader).anyTimes();
        // PowerMock.replay(HelpObjectLoader.class);

        HelpModule module = new HelpModule();
        URL help = getClass().getClassLoader().getResource("data/help");
        module.setUp(help.getPath());

        HelpTopic topic = module.getTopic("SELECT TIME");
        Assert.assertNotNull(topic);

        topic = module.getTopic("select time");
        Assert.assertNotNull(topic);

        // Must ordered by alpha.
        List<String> categories = module.listCategoryByCategory("Admin");
        Assert.assertEquals(2, categories.size());
        Assert.assertTrue(Arrays.equals(categories.toArray(), Lists.newArrayList("Select", "Show").toArray()));
        // topics
        List<String> topics = module.listTopicByKeyword("SHOW");
        Assert.assertEquals(1, topics.size());
        Assert.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SHOW TABLES").toArray()));

        topics = module.listTopicByKeyword("SELECT");
        Assert.assertEquals(1, topics.size());
        Assert.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SELECT TIME").toArray()));

        topics = module.listTopicByCategory("selEct");
        Assert.assertEquals(1, topics.size());
        Assert.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SELECT TIME").toArray()));

        topics = module.listTopicByCategory("show");
        Assert.assertEquals(1, topics.size());
        Assert.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SHOW TABLES").toArray()));

        Assert.assertTrue(Arrays.equals(module.listCategoryByName("ADMIN").toArray(),
                Lists.newArrayList("Admin").toArray()));
    }

    @Test
    public void testLoadFromZip() throws IOException, UserException {
        HelpModule module = new HelpModule();
        URL help = getClass().getClassLoader().getResource("test-help-resource.zip");
        module.setUpByZip(help.getPath());

        HelpTopic topic = module.getTopic("SELECT TIME");
        Assert.assertNotNull(topic);

        topic = module.getTopic("select time");
        Assert.assertNotNull(topic);

        // Must ordered by alpha.
        List<String> categories = module.listCategoryByCategory("Admin");
        Assert.assertEquals(2, categories.size());
        Assert.assertTrue(Arrays.equals(categories.toArray(), Lists.newArrayList("Select", "Show").toArray()));
        // topics
        List<String> topics = module.listTopicByKeyword("SHOW");
        Assert.assertEquals(1, topics.size());
        Assert.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SHOW TABLES").toArray()));

        topics = module.listTopicByKeyword("SELECT");
        Assert.assertEquals(1, topics.size());
        Assert.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SELECT TIME").toArray()));

        topics = module.listTopicByCategory("selEct");
        Assert.assertEquals(1, topics.size());
        Assert.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SELECT TIME").toArray()));

        topics = module.listTopicByCategory("show");
        Assert.assertEquals(1, topics.size());
        Assert.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SHOW TABLES").toArray()));

        Assert.assertTrue(Arrays.equals(module.listCategoryByName("ADMIN").toArray(),
                Lists.newArrayList("Admin").toArray()));
    }
}
