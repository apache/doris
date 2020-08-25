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

package org.apache.doris.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.doris.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * A base cache on each FE node, local only
 */
public class SimpleLocalCache implements Cache {
    private static final Logger LOG = LogManager.getLogger(SimpleLocalCache.class);
    /**
     * Minimum cost in "weight" per entry;
     */
    private static final int FIXED_COST = 8;
    private static final int MAX_DEFAULT_BYTES = 1024 * 1024 * 1024;
    private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
    private static final LZ4FastDecompressor LZ4_DECOMPRESSOR = LZ4_FACTORY.fastDecompressor();
    private static final LZ4Compressor LZ4_COMPRESSOR = LZ4_FACTORY.fastCompressor();

    private final com.github.benmanes.caffeine.cache.Cache<NamedKey, byte[]> cache;

    public static Cache create() {
        return create(CacheExecutorFactory.COMMON_FJP.createExecutor());
    }

    // Used in testing
    public static Cache create(final Executor executor) {
        LOG.info("Instance cache with expiration " + Config.result_cache_expire_after_in_milliseconds
                + " milliseconds, max size " + Config.result_cache_size_in_bytes + " bytes");
        Caffeine<Object, Object> builder = Caffeine.newBuilder().recordStats();
        if (Config.result_cache_expire_after_in_milliseconds >= 0) {
            builder.expireAfterWrite(Config.result_cache_expire_after_in_milliseconds, TimeUnit.MILLISECONDS);
        }
        if (Config.result_cache_size_in_bytes >= 0) {
            builder.maximumWeight(Config.result_cache_size_in_bytes);
        } else {
            builder.maximumWeight(Math.min(MAX_DEFAULT_BYTES, Runtime.getRuntime().maxMemory() / 10));
        }
        builder.weigher((NamedKey key, byte[] value) -> value.length
                + key.key.length
                + key.namespace.length() * Character.BYTES
                + FIXED_COST)
                .executor(executor);
        return new SimpleLocalCache(builder.build());
    }

    private SimpleLocalCache(final com.github.benmanes.caffeine.cache.Cache<NamedKey, byte[]> cache) {
        this.cache = cache;
    }

    @Override
    public byte[] get(NamedKey key) {
        return decompress(cache.getIfPresent(key));
    }

    @Override
    public void put(NamedKey key, byte[] value) {
        final byte[] compresssize = compress(value);
        if (compresssize.length > Config.result_cache_size_per_query_in_bytes) {
            LOG.info(" result size more than result_cache_size_per_query_in_bytes: "
                    + Config.result_cache_size_per_query_in_bytes + " so not storage in cache");
            return;
        }
        cache.put(key, compress(value));
    }

    @Override
    public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys) {
        // The assumption here is that every value is accessed at least once. Materializing here ensures deserialize is only
        // called *once* per value.
        return ImmutableMap.copyOf(Maps.transformValues(cache.getAllPresent(keys), this::decompress));
    }

    // This is completely racy with put. Any values missed should be evicted later anyways. So no worries.
    public void close(String namespace) {
        // Evict on close
        cache.asMap().keySet().removeIf(key -> key.namespace.equals(namespace));
    }

    @Override
    public void close() {
        cache.cleanUp();
    }

    @Override
    public CacheStats getStats() {
        final com.github.benmanes.caffeine.cache.stats.CacheStats stats = cache.stats();
        final long size = cache
                .policy().eviction()
                .map(eviction -> eviction.isWeighted() ? eviction.weightedSize() : OptionalLong.empty())
                .orElse(OptionalLong.empty()).orElse(-1);
        return new CacheStats(
                stats.hitCount(),
                stats.missCount(),
                cache.estimatedSize(),
                size,
                stats.evictionCount(),
                0,
                stats.loadFailureCount()
        );
    }

    @Override
    public boolean isLocal() {
        return true;
    }


    @VisibleForTesting
    com.github.benmanes.caffeine.cache.Cache<NamedKey, byte[]> getCache() {
        return cache;
    }

    private byte[] decompress(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        final int decompressedLen = ByteBuffer.wrap(bytes).getInt();
        final byte[] out = new byte[decompressedLen];
        LZ4_DECOMPRESSOR.decompress(bytes, Integer.BYTES, out, 0, out.length);
        return out;
    }

    private byte[] compress(byte[] value) {
        final int len = LZ4_COMPRESSOR.maxCompressedLength(value.length);
        final byte[] out = new byte[len];
        final int compressedSize = LZ4_COMPRESSOR.compress(value, 0, value.length, out, 0);
        return ByteBuffer.allocate(compressedSize + Integer.BYTES)
                .putInt(value.length)
                .put(out, 0, compressedSize)
                .array();
    }
}