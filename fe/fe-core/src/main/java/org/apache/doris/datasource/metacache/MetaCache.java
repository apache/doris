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

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

public class MetaCache<T> {
    private LoadingCache<String, List<String>> namesCache;
    private Map<Long, String> idToName = Maps.newConcurrentMap();
    private LoadingCache<String, Optional<T>> metaObjCache;

    private String name;

    public MetaCache(String name,
            ExecutorService executor,
            OptionalLong expireAfterWriteSec,
            OptionalLong refreshAfterWriteSec,
            long maxSize,
            CacheLoader<String, List<String>> namesCacheLoader,
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
                maxSize,
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
        return namesCache.get("");
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

    public void updateCache(String objName, T obj) {
        metaObjCache.put(objName, Optional.of(obj));
        namesCache.asMap().compute("", (k, v) -> {
            if (v == null) {
                return Lists.newArrayList(objName);
            } else {
                v.add(objName);
                return v;
            }
        });
    }

    public void invalidate(String objName) {
        namesCache.asMap().compute("", (k, v) -> {
            if (v == null) {
                return Lists.newArrayList();
            } else {
                v.remove(objName);
                return v;
            }
        });
        metaObjCache.invalidate(objName);
    }

    public void invalidateAll() {
        namesCache.invalidateAll();
        metaObjCache.invalidateAll();
    }

}
