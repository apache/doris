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

package org.apache.doris.datasource.metacache;

import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Pair;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class MetaCache<T> {
    private LoadingCache<String, List<Pair<String, String>>> namesCache;
    //Pair<String, String> : <Remote name, Local name>
    private Map<Long, String> idToName = Maps.newConcurrentMap();
    private LoadingCache<String, Optional<T>> metaObjCache;

    private String name;

    public MetaCache(String name,
            ExecutorService executor,
            OptionalLong expireAfterWriteSec,
            OptionalLong refreshAfterWriteSec,
            long maxSize,
            CacheLoader<String, List<Pair<String, String>>> namesCacheLoader,
            CacheLoader<String, Optional<T>> metaObjCacheLoader,
            RemovalListener<String, Optional<T>> removalListener) {
        this.name = name;

        // ATTN:
        // The refreshAfterWriteSec is only used for metaObjCache, not for namesCache.
        // Because namesCache need to be refreshed at interval so that user can get the latest meta list.
        // But metaObjCache does not need to be refreshed at interval, because the object is actually not
        // from remote datasource, it is just a local generated object to represent the meta info.
        // So it only need to be expired after specified duration.
        CacheFactory namesCacheFactory = new CacheFactory(
                expireAfterWriteSec,
                refreshAfterWriteSec,
                1, // names cache has one and only one entry
                true,
                null);
        CacheFactory objCacheFactory = new CacheFactory(
                expireAfterWriteSec,
                OptionalLong.empty(),
                maxSize,
                true,
                null);
        namesCache = namesCacheFactory.buildCache(namesCacheLoader, null, executor);
        metaObjCache = objCacheFactory.buildCache(metaObjCacheLoader, removalListener, executor);
    }

    public List<String> listNames() {
        return Objects.requireNonNull(namesCache.get("")).stream().map(Pair::value).collect(Collectors.toList());
    }

    public String getRemoteName(String localName) {
        return Objects.requireNonNull(namesCache.getIfPresent("")).stream()
                .filter(pair -> pair.value().equals(localName))
                .map(Pair::key)
                .findFirst()
                .orElse(null);
    }

    public Optional<T> getMetaObj(String name, long id) {
        Optional<T> val = metaObjCache.getIfPresent(name);
        if (val == null) {
            synchronized (metaObjCache) {
                val = metaObjCache.get(name);
                idToName.put(id, name);
            }
        }
        return val;
    }

    public Optional<T> getMetaObjById(long id) {
        String name = idToName.get(id);
        return name == null ? Optional.empty() : getMetaObj(name, id);
    }

    public void updateCache(String remoteName, String localName, T obj, long id) {
        metaObjCache.put(localName, Optional.of(obj));
        namesCache.asMap().compute("", (k, v) -> {
            if (v == null) {
                return Lists.newArrayList(Pair.of(remoteName, localName));
            } else {
                v.add(Pair.of(remoteName, localName));
                return v;
            }
        });
        idToName.put(id, localName);
    }

    public void invalidate(String localName, long id) {
        namesCache.asMap().compute("", (k, v) -> {
            if (v == null) {
                return Lists.newArrayList();
            } else {
                v.removeIf(pair -> pair.value().equals(localName));
                return v;
            }
        });
        metaObjCache.invalidate(localName);
        idToName.remove(id);
    }

    public void invalidateAll() {
        namesCache.invalidateAll();
        metaObjCache.invalidateAll();
        idToName.clear();
    }
}
