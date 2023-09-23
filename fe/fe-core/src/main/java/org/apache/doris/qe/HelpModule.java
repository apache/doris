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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

// Help module, used to get information of help
public class HelpModule {
    private static final Logger LOG = LogManager.getLogger(HelpModule.class);
    private static volatile HelpModule instance = null;

    private static final ImmutableList<String> EMPTY_LIST = ImmutableList.of();
    // Map from name to topic
    private ImmutableMap<String, HelpTopic> topicByName = ImmutableMap.of();

    // Map keyword to topics that have this keyword
    private ImmutableListMultimap<String, String> topicByKeyword = ImmutableListMultimap.of();

    // Map category to topics that belong to this category.
    private ImmutableListMultimap<String, String> topicByCategory = ImmutableListMultimap.of();

    // Map parent category to children categories.
    private ImmutableListMultimap<String, String> categoryByParent = ImmutableListMultimap.of();

    // Category set, case insensitive.
    private ImmutableMap<String, String> categoryByName = ImmutableMap.of();

    // Temporary used. Used to build immutable map.
    private ImmutableSortedMap.Builder<String, String> categoryByNameBuilder;
    private ImmutableListMultimap.Builder<String, String> categoryByParentBuilder;
    private ImmutableListMultimap.Builder<String, String> topicByCatBuilder;
    private ImmutableListMultimap.Builder<String, String> topicByKeyBuilder;
    private ImmutableMap.Builder<String, HelpTopic> topicBuilder;

    public static final String HELP_ZIP_FILE_NAME = "help-resource.zip";
    private static final long HELP_ZIP_CHECK_INTERVAL_MS = 10 * 60 * 1000L;

    private static Charset CHARSET_UTF_8;

    static {
        try {
            CHARSET_UTF_8 = Charset.forName("UTF-8");
        } catch (Exception e) {
            CHARSET_UTF_8 = Charset.defaultCharset();
            LOG.error("charset named UTF-8 in not found. use: {}", CHARSET_UTF_8.displayName());
        }
    }

    private static long lastModifyTime = 0L;
    private static long lastCheckTime = 0L;
    private boolean isloaded = false;
    private static String zipFilePath;
    private static ReentrantLock lock = new ReentrantLock();

    // Files in zip is not recursive, so we only need to traverse it
    public void setUpByZip(String path) throws IOException, UserException {
        initBuild();
        ZipFile zf = new ZipFile(path);
        Enumeration<? extends ZipEntry> entries = zf.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            if (entry.isDirectory()) {
                setUpDirInZip(entry.getName());
            } else {
                long size = entry.getSize();
                String line;
                List<String> lines = Lists.newArrayList();
                if (size > 0) {
                    try (BufferedReader reader =
                             new BufferedReader(new InputStreamReader(zf.getInputStream(entry), CHARSET_UTF_8))) {
                        while ((line = reader.readLine()) != null) {
                            lines.add(line);
                        }
                    }

                    // note that we only need basename
                    String parentPathStr = null;
                    Path pathObj = Paths.get(entry.getName());
                    if (pathObj.getParent() != null) {
                        parentPathStr = pathObj.getParent().getFileName().toString();
                    }
                    HelpObjectLoader<HelpTopic> topicLoader = HelpObjectLoader.createTopicLoader();
                    try {
                        List<HelpTopic> topics = topicLoader.loadAll(lines);
                        updateTopic(parentPathStr, topics);
                    } catch (UserException e) {
                        LOG.warn("failed to load help topic: {}", entry.getName(), e);
                        throw e;
                    }
                }
            }
        }
        zf.close();
        build();
        isloaded = true;
    }

    // process dirs in zip file
    private void setUpDirInZip(String pathInZip) {
        Path pathObj = Paths.get(pathInZip);
        // Note: we only need 'basename' here, which is the farthest element from the root in the
        // directory hierarchy.
        String pathStr = pathObj.getFileName().toString();
        String parentPathStr = null;
        if (pathObj.getParent() != null) {
            parentPathStr = pathObj.getParent().getFileName().toString();
        }
        updateCategory(parentPathStr, pathStr);
    }

    // for test only
    public void setUp(String path) throws UserException, IOException {
        File root = new File(path);
        if (!root.isDirectory()) {
            throw new UserException("Need help directory.");
        }
        initBuild();
        for (File file : root.listFiles()) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            setUpDir("", file);
        }
        build();
    }

    // for test only
    private void setUpDir(String parent, File dir) throws IOException, UserException {
        updateCategory(parent, dir.getName());
        for (File file : dir.listFiles()) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            if (file.isDirectory()) {
                setUpDir(dir.getName(), file);
            } else {
                // Load this File
                HelpObjectLoader<HelpTopic> topicLoader = HelpObjectLoader.createTopicLoader();
                List<HelpTopic> topics = topicLoader.loadAll(file.getPath());
                updateTopic(dir.getName(), topics);
            }
        }
    }

    private void initBuild() {
        categoryByNameBuilder = ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);
        categoryByParentBuilder = ImmutableListMultimap.builder();
        categoryByParentBuilder.orderKeysBy(String.CASE_INSENSITIVE_ORDER).orderValuesBy(String.CASE_INSENSITIVE_ORDER);
        topicByCatBuilder = ImmutableListMultimap.builder();
        topicByCatBuilder.orderKeysBy(String.CASE_INSENSITIVE_ORDER).orderValuesBy(String.CASE_INSENSITIVE_ORDER);
        topicByKeyBuilder = ImmutableListMultimap.builder();
        topicByKeyBuilder.orderKeysBy(String.CASE_INSENSITIVE_ORDER).orderValuesBy(String.CASE_INSENSITIVE_ORDER);
        topicBuilder = ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);
    }

    private void updateCategory(String parent, String category) {
        if (!Strings.isNullOrEmpty(parent)) {
            categoryByParentBuilder.put(parent.toLowerCase(), category);
        }
        categoryByNameBuilder.put(category, category);
    }

    private void updateTopic(String category, List<HelpTopic> topics) {
        for (HelpTopic topic : topics) {
            if (Strings.isNullOrEmpty(topic.getName())) {
                continue;
            }
            topicBuilder.put(topic.getName(), topic);
            if (!Strings.isNullOrEmpty(category)) {
                topicByCatBuilder.put(category.toLowerCase(), topic.getName());
            }
            for (String keyword : topic.getKeywords()) {
                if (!Strings.isNullOrEmpty(keyword)) {
                    topicByKeyBuilder.put(keyword.toLowerCase(), topic.getName());
                }
            }
        }
    }

    private void build() {
        categoryByName = categoryByNameBuilder.build();
        categoryByParent = categoryByParentBuilder.build();
        topicByName = topicBuilder.build();
        topicByCategory = topicByCatBuilder.build();
        topicByKeyword = topicByKeyBuilder.build();

        categoryByNameBuilder = null;
        categoryByParentBuilder = null;
        topicBuilder = null;
        topicByCatBuilder = null;
        topicByKeyBuilder = null;
    }

    // Get help information by help name.
    public HelpTopic getTopic(String name) {
        return topicByName.get(name);
    }

    public List<String> listTopicByKeyword(String keyword) {
        if (Strings.isNullOrEmpty(keyword)) {
            return EMPTY_LIST;
        }
        return topicByKeyword.get(keyword.toLowerCase());
    }

    public List<String> listTopicByCategory(String category) {
        if (Strings.isNullOrEmpty(category)) {
            return EMPTY_LIST;
        }
        return topicByCategory.get(category.toLowerCase());
    }

    public List<String> listCategoryByCategory(String category) {
        if (Strings.isNullOrEmpty(category)) {
            return EMPTY_LIST;
        }
        return categoryByParent.get(category.toLowerCase());
    }

    public List<String> listCategoryByName(String name) {
        if (categoryByName.get(name) != null) {
            return Lists.newArrayList(categoryByName.get(name));
        }
        return EMPTY_LIST;
    }

    public void setUpModule(String targetHelpZip) throws IOException, UserException {
        if (Strings.isNullOrEmpty(targetHelpZip)) {
            throw new IOException("Help zip file is null");
        }
        URL helpResource = instance.getClass().getClassLoader().getResource(targetHelpZip);
        if (helpResource == null) {
            throw new IOException("Can not find help zip file: " + targetHelpZip);
        }
        zipFilePath = helpResource.getPath();
        setUpByZip(zipFilePath);

        long now = System.currentTimeMillis();
        lastCheckTime = now;
        lastModifyTime = now;
    }

    public boolean needReloadZipFile(String zipPath) throws UserException {
        if (!isloaded) {
            return false;
        }

        long now = System.currentTimeMillis();
        if ((now - lastCheckTime) < HELP_ZIP_CHECK_INTERVAL_MS) {
            return false;
        }
        lastCheckTime = now;

        // check zip file's last modify time
        File file = new File(zipPath);
        if (!file.exists()) {
            throw new UserException("zipfile of help module is not exist" + zipPath);
        }
        long lastModify = file.lastModified();
        if (lastModifyTime >= lastModify) {
            return false;
        } else {
            lastModifyTime = lastModify;
            return true;
        }
    }

    // Every query will begin at this method, so we add check logic here to check
    // whether need reload ZipFile
    public static HelpModule getInstance() {
        if (instance == null) {
            synchronized (HelpModule.class) {
                if (instance == null) {
                    instance = new HelpModule();
                }
            }
        }

        try {
            // If one thread is reloading zip-file, the other thread use old instance.
            if (instance.needReloadZipFile(zipFilePath)) {
                if (lock.tryLock()) {
                    LOG.info("reload help zip file: " + zipFilePath);
                    try {
                        HelpModule newInstance = new HelpModule();
                        newInstance.setUpByZip(zipFilePath);
                        instance = newInstance;
                    } catch (UserException | IOException e) {
                        LOG.warn("Failed to reload help zip file: " + zipFilePath, e);
                    } finally {
                        lock.unlock();
                    }
                }
            }
        } catch (UserException e) {
            LOG.warn("Failed to reload help zip file: " + zipFilePath, e);
        }

        return instance;
    }
}
