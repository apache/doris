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

package org.apache.doris.qe.help;

import org.apache.doris.common.MarkDownParser;
import org.apache.doris.common.UserException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

// Loader of help object
// Now, we only have one type of format for help topic,
// so use class instead of interface.
public abstract class HelpObjectLoader<HelpTypeT extends HelpObjectIface> {
    private static final Logger LOG = LogManager.getLogger(HelpObjectLoader.class);

    public List<HelpTypeT> loadAll(List<String> lines) throws UserException {
        if (lines == null) {
            LOG.error("Help object loader input lines is empty.");
            return null;
        }
        List<HelpTypeT> topics = Lists.newArrayList();
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> docs = parser.parse();

        for (Map.Entry<String, Map<String, String>> doc : docs.entrySet()) {
            HelpTypeT topic = newInstance();
            topic.loadFrom(doc);
            if (!Strings.isNullOrEmpty(topic.getName())) {
                topics.add(topic);
            }
        }
        return topics;
    }

    // Load all Topics
    public List<HelpTypeT> loadAll(String path) throws IOException, UserException {
        if (Strings.isNullOrEmpty(path)) {
            LOG.error("Help object loader input file is empty.");
            return null;
        }
        List<String> lines = Files.readAllLines(Paths.get(path), Charset.forName("UTF-8"));
        return this.loadAll(lines);
    }

    public abstract HelpTypeT newInstance();

    public static HelpObjectLoader<HelpTopic> createTopicLoader() {
        return new HelpObjectLoader<HelpTopic>() {
            @Override
            public HelpTopic newInstance() {
                return new HelpTopic();
            }
        };
    }

    public static HelpObjectLoader<HelpCategory> createCategoryLoader() {
        return new HelpObjectLoader<HelpCategory>() {
            @Override
            public HelpCategory newInstance() {
                return new HelpCategory();
            }
        };
    }
}
