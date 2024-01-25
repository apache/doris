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

import org.apache.doris.qe.HelpTopic;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A simple MarkDownParser to parser the help topic
 * eg: sql-reference, sql-functions.
 * <p>
 * Each topic must have following structure:
 * ## Topic Name
 * ### Description  // required
 * ### Example      // optional
 * ### Keywords     // required
 * other fields are optional
 * <p>
 * <p>
 * It is allowed to have multi topic in one file.
 */
public class MarkDownParser {
    private enum ParseState {
        START,
        PARSED_H1,
        PARSED_H2
    }

    private static final byte SINGLE_POUND_SIGN = '#';

    private Map<String, Map<String, String>> documents;
    private List<String> lines;
    private int nextToRead;
    private ParseState state;

    private int headLevel;
    private String head;
    // Temp map used to store parsed keyValues;
    private Map<String, String> keyValues;

    public MarkDownParser(List<String> lines) {
        this.lines = lines;
        documents = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        nextToRead = 0;
        state = ParseState.START;
    }

    public Map<String, Map<String, String>> parse() throws UserException {
        while (nextToRead < lines.size()) {
            Map.Entry<String, String> keyValue = parseOneItem();
            if (keyValue == null) {
                // Parse over!
                continue;
            }
            switch (state) {
                case START:
                    if (headLevel == 2) {
                        head = keyValue.getKey();
                        keyValues = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
                        state = ParseState.PARSED_H1;
                    } else {
                        // State error
                        throw new UserException("Head first read is not h1.");
                    }
                    break;
                case PARSED_H1:
                    if (headLevel == 2) {
                        // Empty document, step over, do nothing
                        documents.put(head, keyValues);
                        head = keyValue.getKey();
                        keyValues = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
                    } else if (headLevel == 3) {
                        keyValues.put(keyValue.getKey(), keyValue.getValue());
                        state = ParseState.PARSED_H2;
                    } else {
                        throw new UserException("Unknown head level.");
                    }
                    break;
                case PARSED_H2:
                    if (headLevel == 2) {
                        // One document read over.
                        documents.put(head, keyValues);
                        head = keyValue.getKey();
                        keyValues = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
                    } else if (headLevel == 3) {
                        keyValues.put(keyValue.getKey(), keyValue.getValue());
                    } else {
                        //Ignore headlevel greater than 2 instead of throwing a exception
                        //State error
                        //throw new UserException("Unknown head level when parsing head level(2)");
                    }
                    break;
                default:
                    // State error
                    throw new UserException("Unknown parse state.");
            }
        }

        if (head != null) {
            documents.put(head, keyValues);
        }

        checkStructure();
        return documents;
    }

    private void checkStructure() throws DdlException {
        for (Map.Entry<String, Map<String, String>> entry : documents.entrySet()) {
            Set<String> keys = entry.getValue().keySet();
            if (!(keys.contains(HelpTopic.DESCRIPTION)
                    && keys.contains(HelpTopic.KEYWORDS))) {
                throw new DdlException("Invalid help topic structure. title: " + entry.getKey() + ", keys: " + keys);
            }
        }
    }

    private Map.Entry<String, String> parseOneItem() {
        // 1. Find the first heading line (start with ##)
        while (nextToRead < lines.size() && !lines.get(nextToRead).startsWith("##")) {
            nextToRead++;
        }
        if (nextToRead >= lines.size()) {
            return null;
        }
        // 2. Get the level of this key
        String key = lines.get(nextToRead++);
        headLevel = 0;
        while (headLevel < key.length() && key.charAt(headLevel) == SINGLE_POUND_SIGN) {
            headLevel++;
        }
        // 3. Save all lines within this level until we met next ## or ###
        StringBuilder sb = new StringBuilder();
        while (nextToRead < lines.size()) {
            if (!lines.get(nextToRead).startsWith("##")) {
                // content
                sb.append(lines.get(nextToRead)).append('\n');
                nextToRead++;
            } else if (lines.get(nextToRead).startsWith("####")) {
                // Ignore head level greater than 3, treat them as normal content
                sb.append(lines.get(nextToRead)).append('\n');
                nextToRead++;
            } else {
                // break if we meet next heading
                break;
            }
        }
        // Note that multiple line breaks at content's end will be merged to be one,
        // and other whitespace characters will be deleted.
        // Also, the header in md file is like "## STREAM-LOAD", we need to convert it to "STREAM LOAD",
        // so that we can execute "help stream load" to show the help doc.
        return Maps.immutableEntry(key.substring(headLevel).trim().replaceAll("-", " "),
                processWhitespace(sb));
    }

    private String processWhitespace(StringBuilder sb) {
        int index = sb.length() - 1;
        while (index >= 0 && Character.isWhitespace(sb.charAt(index))) {
            index--;
        }

        if (index < sb.length() - 1) {
            sb.setLength(index + 1);
            sb.append('\n');
        }
        return sb.toString();
    }
}
