/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.segment;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;

/**
 * {@code Cache} wrapper exposing the number of read accesses and the
 * number of misses ot the underlying cache via the {@link StatisticsProvider}.
 * michid test
 * michid doc
 */
public class CacheAccessTracker<K, V> implements Cache<K,V> {
    private final Cache<K, V> delegate;
    private final CounterStats accessCount;
    private final CounterStats missCount;

    public CacheAccessTracker(
            @Nonnull String name,
            @Nonnull Cache<K, V> delegate,
            @Nonnull StatisticsProvider statisticsProvider) {
        this.delegate = delegate;
        this.accessCount = statisticsProvider.getCounterStats(
                name + ".access-count", StatsOptions.DEFAULT);
        this.missCount = statisticsProvider.getCounterStats(
                name + ".miss-count", StatsOptions.DEFAULT);
    }

    @Override
    public void put(@Nonnull K key, @Nonnull V value) {
        delegate.put(key, value);
    }

    @Override
    public void put(@Nonnull K key, @Nonnull V value, byte cost) {
        delegate.put(key, value, cost);
    }

    @CheckForNull
    @Override
    public V get(@Nonnull K key) {
        V v = delegate.get(key);
        accessCount.inc();
        if (v == null) {
            missCount.inc();
        }
        return v;
    }
}
