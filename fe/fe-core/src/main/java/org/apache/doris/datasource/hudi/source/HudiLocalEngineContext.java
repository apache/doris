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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieAtomicLongAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.function.FunctionWrapper;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableConsumer;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFlatMapFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This file is copied from org.apache.hudi.common.engine.HoodieLocalEngineContext.
 * Because we need set ugi in thread pool
 * A java based engine context, use this implementation on the query engine integrations if needed.
 */
public final class HudiLocalEngineContext extends HoodieEngineContext {

    public HudiLocalEngineContext(Configuration conf) {
        this(conf, new LocalTaskContextSupplier());
    }

    public HudiLocalEngineContext(Configuration conf, TaskContextSupplier taskContextSupplier) {
        super(new SerializableConfiguration(conf), taskContextSupplier);
    }

    @Override
    public HoodieAccumulator newAccumulator() {
        return HoodieAtomicLongAccumulator.create();
    }

    @Override
    public <T> HoodieData<T> emptyHoodieData() {
        return HoodieListData.eager(Collections.emptyList());
    }

    @Override
    public <T> HoodieData<T> parallelize(List<T> data, int parallelism) {
        return HoodieListData.eager(data);
    }

    @Override
    public <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism) {
        return data.stream().parallel().map(v1 -> {
            try {
                return HiveMetaStoreClientHelper.ugiDoAs(getHadoopConf().get(), () -> func.apply(v1));
            } catch (Exception e) {
                throw new HoodieException("Error occurs when executing map", e);
            }
        }).collect(Collectors.toList());
    }

    @Override
    public <I, K, V> List<V> mapToPairAndReduceByKey(
            List<I> data,
            SerializablePairFunction<I, K, V> mapToPairFunc,
            SerializableBiFunction<V, V, V> reduceFunc, int parallelism) {
        return data.stream().parallel().map(FunctionWrapper.throwingMapToPairWrapper(mapToPairFunc))
            .collect(Collectors.groupingBy(p -> p.getKey())).values().stream()
            .map(list ->
                    list.stream()
                        .map(e -> e.getValue())
                        .reduce(FunctionWrapper.throwingReduceWrapper(reduceFunc)).get())
            .collect(Collectors.toList());
    }

    @Override
    public <I, K, V> Stream<ImmutablePair<K, V>> mapPartitionsToPairAndReduceByKey(
            Stream<I> data, SerializablePairFlatMapFunction<Iterator<I>, K, V> flatMapToPairFunc,
            SerializableBiFunction<V, V, V> reduceFunc, int parallelism) {
        return FunctionWrapper.throwingFlatMapToPairWrapper(flatMapToPairFunc).apply(data.parallel().iterator())
            .collect(Collectors.groupingBy(Pair::getKey)).entrySet().stream()
            .map(entry -> new ImmutablePair<>(entry.getKey(), entry.getValue().stream().map(
                Pair::getValue).reduce(FunctionWrapper.throwingReduceWrapper(reduceFunc)).orElse(null)))
            .filter(Objects::nonNull);
    }

    @Override
    public <I, K, V> List<V> reduceByKey(
            List<Pair<K, V>> data, SerializableBiFunction<V, V, V> reduceFunc, int parallelism) {
        return data.stream().parallel()
            .collect(Collectors.groupingBy(p -> p.getKey())).values().stream()
            .map(list ->
                list.stream()
                    .map(e -> e.getValue())
                    .reduce(FunctionWrapper.throwingReduceWrapper(reduceFunc)).orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    @Override
    public <I, O> List<O> flatMap(List<I> data, SerializableFunction<I, Stream<O>> func, int parallelism) {
        return
            data.stream().parallel().flatMap(FunctionWrapper.throwingFlatMapWrapper(func)).collect(Collectors.toList());
    }

    @Override
    public <I> void foreach(List<I> data, SerializableConsumer<I> consumer, int parallelism) {
        data.stream().forEach(FunctionWrapper.throwingForeachWrapper(consumer));
    }

    @Override
    public <I, K, V> Map<K, V> mapToPair(List<I> data, SerializablePairFunction<I, K, V> func, Integer parallelism) {
        return data.stream().map(FunctionWrapper.throwingMapToPairWrapper(func)).collect(
            Collectors.toMap(Pair::getLeft, Pair::getRight, (oldVal, newVal) -> newVal)
        );
    }

    @Override
    public void setProperty(EngineProperty key, String value) {
        // no operation for now
    }

    @Override
    public Option<String> getProperty(EngineProperty key) {
        return Option.empty();
    }

    @Override
    public void setJobStatus(String activeModule, String activityDescription) {
        // no operation for now
    }

    @Override
    public void putCachedDataIds(HoodieDataCacheKey cacheKey, int... ids) {
        // no operation for now
    }

    @Override
    public List<Integer> getCachedDataIds(HoodieDataCacheKey cacheKey) {
        return Collections.emptyList();
    }

    @Override
    public List<Integer> removeCachedDataIds(HoodieDataCacheKey cacheKey) {
        return Collections.emptyList();
    }

    @Override
    public void cancelJob(String jobId) {
        // no operation for now
    }

    @Override
    public void cancelAllJobs() {
        // no operation for now
    }
}
