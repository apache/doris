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
import org.apache.doris.qe.help.HelpModule;
import org.apache.doris.qe.help.HelpTopic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

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
    @BeforeEach
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

        System.out.println(HelpModuleTest.class.getResource("/"));
        System.out.println(HelpModuleTest.class.getClassLoader().getResource(""));
    }

    @Disabled
    public void testNormal() throws IOException, UserException {
        HelpModule module = new HelpModule();
        URL help = getClass().getClassLoader().getResource("data/help");
        module.setUp(help.getPath());

        HelpTopic topic = module.getTopic("SELECT TIME");
        Assertions.assertNotNull(topic);

        topic = module.getTopic("select time");
        Assertions.assertNotNull(topic);

        // Must ordered by alpha.
        List<String> categories = module.listCategoryByCategory("Admin");
        Assertions.assertEquals(2, categories.size());
        Assertions.assertTrue(Arrays.equals(categories.toArray(), Lists.newArrayList("Select", "Show").toArray()));
        // topics
        List<String> topics = module.listTopicByKeyword("SHOW");
        Assertions.assertEquals(1, topics.size());
        Assertions.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SHOW TABLES").toArray()));

        topics = module.listTopicByKeyword("SELECT");
        Assertions.assertEquals(1, topics.size());
        Assertions.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SELECT TIME").toArray()));

        topics = module.listTopicByCategory("selEct");
        Assertions.assertEquals(1, topics.size());
        Assertions.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SELECT TIME").toArray()));

        topics = module.listTopicByCategory("show");
        Assertions.assertEquals(1, topics.size());
        Assertions.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SHOW TABLES").toArray()));

        Assertions.assertTrue(Arrays.equals(module.listCategoryByName("ADMIN").toArray(),
                Lists.newArrayList("Admin").toArray()));
    }

    @Disabled
    public void testLoadFromZip() throws IOException, UserException {
        HelpModule module = new HelpModule();
        URL help = getClass().getClassLoader().getResource("test-help-resource.zip");
        module.setUpByZip(help.getPath());

        HelpTopic topic = module.getTopic("SELECT TIME");
        Assertions.assertNotNull(topic);

        topic = module.getTopic("select time");
        Assertions.assertNotNull(topic);

        // Must ordered by alpha.
        List<String> categories = module.listCategoryByCategory("Admin");
        Assertions.assertEquals(2, categories.size());
        Assertions.assertTrue(Arrays.equals(categories.toArray(), Lists.newArrayList("Select", "Show").toArray()));
        // topics
        List<String> topics = module.listTopicByKeyword("SHOW");
        Assertions.assertEquals(1, topics.size());
        Assertions.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SHOW TABLES").toArray()));

        topics = module.listTopicByKeyword("SELECT");
        Assertions.assertEquals(1, topics.size());
        Assertions.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SELECT TIME").toArray()));

        topics = module.listTopicByCategory("selEct");
        Assertions.assertEquals(1, topics.size());
        Assertions.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SELECT TIME").toArray()));

        topics = module.listTopicByCategory("show");
        Assertions.assertEquals(1, topics.size());
        Assertions.assertTrue(Arrays.equals(topics.toArray(), Lists.newArrayList("SHOW TABLES").toArray()));

        Assertions.assertTrue(Arrays.equals(module.listCategoryByName("ADMIN").toArray(),
                Lists.newArrayList("Admin").toArray()));
    }

    // Need first call docs/build_help_resource.sh to build real help resource.
    // And copy docs/build/help-resource.zip to fe/fe-core/src/test/resources/real-help-resource.zip
    @Disabled
    public void testRealHelpZip() {
        try {
            HelpModule.getInstance().setUpModule("real-help-resource.zip");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }
}
