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
import com.google.common.base.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FIXME OAK-4277: Finalise de-duplication caches
// implement monitoring for this cache
// add unit tests
public class RecordCache<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RecordCache.class);

    private final ConcurrentMap<Integer, Supplier<T>> generations = newConcurrentMap();
    private final Supplier<T> factory;

    public RecordCache(Supplier<T> factory) {
        this.factory = factory;
    }

    public T generation(final int generation) {
        // Preemptive check to limit the number of wasted Supplier instances
        if (!generations.containsKey(generation)) {
            generations.putIfAbsent(generation, memoize(new Supplier<T>() {
                @Override
                public T get() {
                    return factory.get();
                }
            }));
        }
        return generations.get(generation).get();
    }

    public void put(T cache, int generation) {
        generations.put(generation, Suppliers.ofInstance(cache));
    }

    public void clearUpTo(int maxGen) {
        Iterator<Integer> it = generations.keySet().iterator();
        while (it.hasNext()) {
            Integer gen =  it.next();
            if (gen <= maxGen) {
                it.remove();
            }
        }
    }

    public void clear(int generation) {
        generations.remove(generation);
    }

    public void clear() {
        generations.clear();
    }

    public static class LRUCache<T> {
        private final Map<T, RecordId> map;

        public static final <T> Supplier<LRUCache<T>> factory(final int size) {
            return new Supplier<LRUCache<T>>() {
                @Override
                public LRUCache<T> get() {
                    return new LRUCache<>(size);
                }
            };
        }

        public static final <T> Supplier<LRUCache<T>> empty() {
            return new Supplier<LRUCache<T>>() {
                @Override
                public LRUCache<T> get() {
                    return new LRUCache<T>(0) {
                        @Override public synchronized void put(T key, RecordId value) { }
                        @Override public synchronized RecordId get(T key) { return null; }
                        @Override public synchronized void clear() { }
                    };
                }
            };
        }

        public LRUCache(final int size) {
            map = new LinkedHashMap<T, RecordId>(size * 4 / 3, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<T, RecordId> eldest) {
                    return size() >= size;
                }
            };
        }

        public synchronized void put(T key, RecordId value) {
            map.put(key, value);
        }

        public synchronized RecordId get(T key) {
            return map.get(key);
        }

        public synchronized void clear() {
            map.clear();
        }
    }

    public static class DeduplicationCache<T> {
        private final int capacity;
        private final List<Map<T, RecordId>> maps;

        private int size;

        private final Set<Integer> muteDepths = newHashSet();

        public static final <T> Supplier<DeduplicationCache<T>> factory(final int capacity, final int maxDepth) {
            return new Supplier<DeduplicationCache<T>>() {
                @Override
                public DeduplicationCache<T> get() {
                    return new DeduplicationCache<>(capacity, maxDepth);
                }
            };
        }

        public static final <T> Supplier<DeduplicationCache<T>> empty() {
            return new Supplier<DeduplicationCache<T>>() {
                @Override
                public DeduplicationCache<T> get() {
                    return new DeduplicationCache<T>(0, 0){
                        @Override public synchronized void put(T key, RecordId value, int cost) { }
                        @Override public synchronized RecordId get(T key) { return null; }
                        @Override public synchronized void clear() { }
                    };
                }
            };
        }

        public DeduplicationCache(int capacity, int maxDepth) {
            checkArgument(capacity > 0);
            checkArgument(maxDepth > 0);
            this.capacity = capacity;
            this.maps = newArrayList();
            for (int k = 0; k < maxDepth; k++) {
                maps.add(new HashMap<T, RecordId>());
            }
        }

        public synchronized void put(T key, RecordId value, int cost) {
            // FIXME OAK-4277: Finalise de-duplication caches
            // Validate and optimise the eviction strategy.
            // Nodes with many children should probably get a boost to
            // protecting them from preemptive eviction. Also it might be
            // necessary to implement pinning (e.g. for checkpoints).
            while (size >= capacity) {
                int d = maps.size() - 1;
                int removed = maps.remove(d).size();
                size -= removed;
                if (removed > 0) {
                    // FIXME OAK-4165: Too verbose logging during revision gc
                    LOG.info("Evicted cache at depth {} as size {} reached capacity {}. " +
                        "New size is {}", d, size + removed, capacity, size);
                }
            }

            if (cost < maps.size()) {
                if (maps.get(cost).put(key, value) == null) {
                    size++;
                }
            } else {
                if (muteDepths.add(cost)) {
                    LOG.info("Not caching {} -> {} as depth {} reaches or exceeds the maximum of {}",
                        key, value, cost, maps.size());
                }
            }
        }

        public synchronized RecordId get(T key) {
            for (Map<T, RecordId> map : maps) {
                if (!map.isEmpty()) {
                    RecordId recordId = map.get(key);
                    if (recordId != null) {
                        return recordId;
                    }
                }
            }
            return null;
        }

        public synchronized void clear() {
            maps.clear();
        }

    }
}
