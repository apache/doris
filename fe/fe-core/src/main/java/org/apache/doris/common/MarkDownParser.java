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

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

// Process markdown file.
//
// Example:
// file content is below:
// 
// # head1
// ## head2_1
// some lines
// ## head2_2
// some other lines
//
// will be parsed to map like {"head1" : {"head2_1":"some lines", "head2_2" : "some other lines"}}
// Note: It is allowed to have more than one topic in a file
public class MarkDownParser {
    private static enum ParseState {
        START,
        PARSED_H1,
        PARSED_H2
    }

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
                    if (headLevel == 1) {
                        head = keyValue.getKey();
                        keyValues = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
                        state = ParseState.PARSED_H1;
                    } else {
                        // State error
                        throw new UserException("Head first read is not h1.");
                    }
                    break;
                case PARSED_H1:
                    if (headLevel == 1) {
                        // Empty document, step over, do nothing
                        documents.put(head, keyValues);
                        head = keyValue.getKey();
                        keyValues = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
                    } else if (headLevel == 2) {
                        keyValues.put(keyValue.getKey(), keyValue.getValue());
                        state = ParseState.PARSED_H2;
                    } else {
                        throw new UserException("Unknown head level.");
                    }
                    break;
                case PARSED_H2:
                    if (headLevel == 1) {
                        // One document read over.
                        documents.put(head, keyValues);
                        head = keyValue.getKey();
                        keyValues = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
                    } else if (headLevel == 2) {
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

        return documents;
    }

    private Map.Entry<String, String> parseOneItem() {
        while (nextToRead < lines.size() && !lines.get(nextToRead).startsWith("#")) {
            nextToRead++;
        }
        if (nextToRead >= lines.size()) {
            return null;
        }
        String key = lines.get(nextToRead++);
        headLevel = 0;
        while (headLevel < key.length() && key.charAt(headLevel) == '#') {
            headLevel++;
        }
        StringBuilder sb = new StringBuilder();
        while (nextToRead < lines.size()) {
            if (!lines.get(nextToRead).startsWith("#")) {
                sb.append(lines.get(nextToRead)).append('\n');
                nextToRead++;
            }
            // Ignore headlevel greater than 2
            else if (lines.get(nextToRead).startsWith("###")) {
                sb.append(lines.get(nextToRead).replaceAll("#","")).append('\n');
                nextToRead++;
            } 
            else {
                break;
            }
        }
        // Note that multiple line breaks at content's end will be merged to be one,
        // and other whitespace characters will be deleted.
        return Maps.immutableEntry(key.substring(headLevel).trim(),
                sb.toString().replaceAll("\\s+$", "\n"));
    }
}

