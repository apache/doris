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

package org.apache.doris.common.path;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

// Organized path to be a trie, which is used in Palo to route actions in web interface.
// Path can be a local file path, or path part in a URL.
// 
// NOTE: Wildcard is supported. If dir names in path have brace in both side, the dir node will 
// be regarded as a wildcard, which means it can match any string. A map contains the keys to 
// matched strings will be built.
// e.g. "/api/{database}/{table}", can match "/api/db_name/tb_name", and the map is 
// {database => db_name, table => tb_name}
public class PathTrie<T> {
    
    private static final char PATH_SEPARATOR = '/';
    private static final char LEFT_BRACE = '{';
    private static final char RIGHT_BRACE = '}';
    
    private static final String ASTERISK_WILDCARD = "*";

    // Some path may have been encoded, so they need a Decoder, Invoker should supply 
    // implementation for different path type.
    // e.g. URL path in a http-request from browser may be encoded as per RFC 3986, Section 2,
    public static interface Decoder {
        String decode(String value);
    }

    public static final Decoder NO_DECODER = new Decoder() {
        @Override
        public String decode(String value) {
            return value;
        }
    };

    private final Decoder decoder;
    private final TrieNode<T> root;
    private final char separator;
    private T rootValue;

    public PathTrie() {
        this(PATH_SEPARATOR, ASTERISK_WILDCARD, NO_DECODER);
    }

    public PathTrie(Decoder decoder) {
        this(PATH_SEPARATOR, ASTERISK_WILDCARD, decoder);
    }

    public PathTrie(char separator, String wildcard, Decoder decoder) {
        this.decoder = decoder;
        this.separator = separator;
        root = new TrieNode<>(new String(new char[]{separator}), null, null, wildcard);
    }

    public void insert(String path, T value) {
        String[] strings = path.split(String.valueOf(separator));
        if (strings.length == 0) {
            rootValue = value;
            return;
        }
        int index = 0;
        // supports initial delimiter.
        if (strings.length > 0 && strings[0].isEmpty()) {
            index = 1;
        }
        root.insert(strings, index, value);
    }

    public T retrieve(String path) {
        return retrieve(path, null);
    }

    public T retrieve(String path, Map<String, String> params) {
        if (path.length() == 0) {
            return rootValue;
        }
        String[] strings = path.split(String.valueOf(separator));
        if (strings.length == 0) {
            return rootValue;
        }
        int index = 0;
        // supports initial delimiter.
        if (strings.length > 0 && strings[0].isEmpty()) {
            index = 1;
        }
        return root.retrieve(strings, index, params);
    }
    
    public class TrieNode<T> {
        private transient String key;
        private transient T value;
        private boolean isWildcard;
        private final String wildcard;
        
        private transient String namedWildcard;
        
        private ImmutableMap<String, TrieNode<T>> children;
        
        private final TrieNode<T> parent;
        
        public TrieNode(String key, T value, TrieNode<T> parent, String wildcard) {
            this.key = key;
            this.wildcard = wildcard;
            this.isWildcard = (key.equals(wildcard));
            this.parent = parent;
            this.value = value;
            this.children = ImmutableMap.of();
            if (isNamedWildcard(key)) {
                namedWildcard = key.substring(key.indexOf(LEFT_BRACE) + 1, key.indexOf(RIGHT_BRACE));
            } else {
                namedWildcard = null;
            }
        }
        
        public void updateKeyWithNamedWildcard(String key) {
            this.key = key;
            namedWildcard = key.substring(key.indexOf(LEFT_BRACE) + 1, key.indexOf(RIGHT_BRACE));
        }
        
        public boolean isWildcard() {
            return isWildcard;
        }
        
        public synchronized void addChild(TrieNode<T> child) {
            Map<String, TrieNode<T>> temp = Maps.newHashMap(children);
            temp.put(child.key, child);
            children = ImmutableMap.copyOf(temp);
        }
        
        public TrieNode<T> getChild(String key) {
            return children.get(key);
        }
        
        // construct the trie tree by inserting recursively.
        public synchronized void insert(String[] path, int index, T value) {
            if (index >= path.length) {
                return;
            }
            
            String token = path[index];
            String key = token;
            if (isNamedWildcard(token)) {
                key = wildcard;
            }
            TrieNode<T> node = children.get(key);
            if (node == null) {
                if (index == (path.length - 1)) {
                    node = new TrieNode<>(token, value, this, wildcard);
                } else {
                    node = new TrieNode<>(token, null, this, wildcard);
                }
                Map<String, TrieNode<T>> temp = Maps.newHashMap(children);
                temp.put(key, node);
                children = ImmutableMap.copyOf(temp);
            } else {
                if (isNamedWildcard(token)) {
                    node.updateKeyWithNamedWildcard(token);
                }
                
                // In case the target(last) node already exist but without a value
                // than the value should be updated.
                if (index == (path.length - 1)) {
                    assert (node.value == null || node.value == value);
                    if (node.value == null) {
                        node.value = value;
                    }
                }
            }
            
            node.insert(path, index + 1, value);
        }
        
        private boolean isNamedWildcard(String key) {
            return key.indexOf(LEFT_BRACE) != -1 && key.indexOf(RIGHT_BRACE) != -1;
        }
        
        private boolean isNamedWildcard() {
            return namedWildcard != null;
        }
        
        private String namedWildcard() {
            return namedWildcard;
        }
        
        // Retrieve the trie tree recursively and build the map.
        public T retrieve(String[] path, int index, Map<String, String> params) {
            if (index >= path.length) {
                return null;
            }
            
            String token = path[index];
            TrieNode<T> node = children.get(token);
            boolean usedWildcard;
            if (node == null) {
                node = children.get(wildcard);
                if (node == null) {
                    return null;
                }
                usedWildcard = true;
            } else {
                // If we are at the end of the path, the current node does not have a value but there
                // is a child wildcard node, use the child wildcard node
                if (index + 1 == path.length && node.value == null && children.get(wildcard) != null) {
                    node = children.get(wildcard);
                    usedWildcard = true;
                } else {
                    usedWildcard = token.equals(wildcard);
                }
            }
            
            put(params, node, token);
            
            if (index == (path.length - 1)) {
                return node.value;
            }
            
            T res = node.retrieve(path, index + 1, params);
            if (res == null && !usedWildcard) {
                node = children.get(wildcard);
                if (node != null) {
                    put(params, node, token);
                    res = node.retrieve(path, index + 1, params);
                }
            }
            
            return res;
        }
        
        private void put(Map<String, String> params, TrieNode<T> node, String value) {
            if (params != null && node.isNamedWildcard()) {
                params.put(node.namedWildcard(), decoder.decode(value));
            }
        }
    }
}
