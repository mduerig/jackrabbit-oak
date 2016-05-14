/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Sets.newHashSet;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// michid OAK-4277: Finalise de-duplication caches
// implement monitoring for this cache
// add unit tests
public class CacheManager<T> {
    private static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);

    private final ConcurrentMap<Integer, Supplier<T>> generations = newConcurrentMap();
    private final Supplier<T> cacheFactory;

    public CacheManager(Supplier<T> cacheFactory) {
        this.cacheFactory = cacheFactory;
    }

    public T generation(final int generation) {
        // Preemptive check to limit the number of wasted (Memoizing)Supplier instances
        if (!generations.containsKey(generation)) {
            generations.putIfAbsent(generation, memoize(cacheFactory));
        }
        return generations.get(generation).get();
    }

    // michid remove
    public void clearUpTo(int maxGen) {
        Iterator<Integer> it = generations.keySet().iterator();
        while (it.hasNext()) {
            Integer gen =  it.next();
            if (gen <= maxGen) {
                it.remove();
            }
        }
    }

    public static class RecordCache<T> {
        private final Map<T, RecordId> records;

        public static final <T> Supplier<RecordCache<T>> factory(final int size) {
            return new Supplier<RecordCache<T>>() {
                @Override
                public RecordCache<T> get() {
                    return new RecordCache<>(size);
                }
            };
        }

        public static final <T> Supplier<RecordCache<T>> empty() {
            return new Supplier<RecordCache<T>>() {
                @Override
                public RecordCache<T> get() {
                    return new RecordCache<T>(0) {
                        @Override public synchronized void put(T key, RecordId value) { }
                        @Override public synchronized RecordId get(T key) { return null; }
                    };
                }
            };
        }

        public RecordCache(final int size) {
            records = new LinkedHashMap<T, RecordId>(size * 4 / 3, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<T, RecordId> eldest) {
                    return size() >= size;
                }
            };
        }

        public synchronized void put(T key, RecordId value) {
            records.put(key, value);
        }

        public synchronized RecordId get(T key) {
            return records.get(key);
        }
    }

    public static class NodeCache {
        private final int capacity;
        private final List<Map<String, RecordId>> nodes;

        private int size;

        private final Set<Integer> muteDepths = newHashSet();

        public static final Supplier<NodeCache> factory(final int capacity, final int maxDepth) {
            return new Supplier<NodeCache>() {
                @Override
                public NodeCache get() {
                    return new NodeCache(capacity, maxDepth);
                }
            };
        }

        public static final Supplier<NodeCache> empty() {
            return new Supplier<NodeCache>() {
                @Override
                public NodeCache get() {
                    return new NodeCache(0, 0){
                        @Override public synchronized void put(String key, RecordId value, int depth) { }
                        @Override public synchronized RecordId get(String key) { return null; }
                    };
                }
            };
        }

        public NodeCache(int capacity, int maxDepth) {
            checkArgument(capacity > 0);
            checkArgument(maxDepth > 0);
            this.capacity = capacity;
            this.nodes = newArrayList();
            for (int k = 0; k < maxDepth; k++) {
                nodes.add(new HashMap<String, RecordId>());
            }
        }

        public synchronized void put(String key, RecordId value, int depth) {
            // michid OAK-4277: Finalise de-duplication caches
            // Validate and optimise the eviction strategy.
            // Nodes with many children should probably get a boost to
            // protecting them from preemptive eviction. Also it might be
            // necessary to implement pinning (e.g. for checkpoints).
            while (size >= capacity) {
                int d = nodes.size() - 1;
                int removed = nodes.remove(d).size();
                size -= removed;
                if (removed > 0) {
                    // FIXME OAK-4165: Too verbose logging during revision gc
                    LOG.info("Evicted cache at depth {} as size {} reached capacity {}. " +
                        "New size is {}", d, size + removed, capacity, size);
                }
            }

            if (depth < nodes.size()) {
                if (nodes.get(depth).put(key, value) == null) {
                    size++;
                }
            } else {
                if (muteDepths.add(depth)) {
                    LOG.info("Not caching {} -> {} as depth {} reaches or exceeds the maximum of {}",
                        key, value, depth, nodes.size());
                }
            }
        }

        public synchronized RecordId get(String key) {
            for (Map<String, RecordId> map : nodes) {
                if (!map.isEmpty()) {
                    RecordId recordId = map.get(key);
                    if (recordId != null) {
                        return recordId;
                    }
                }
            }
            return null;
        }

    }
}
