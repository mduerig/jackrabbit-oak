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
import static com.google.common.collect.Lists.newArrayList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import com.google.common.cache.CacheStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// michid doc: update
/**
 * Partial mapping of string keys to values of type {@link RecordId}. This is
 * typically used for de-duplicating nodes that have already been persisted and thus
 * already have a {@code RecordId}.
 */
public abstract class NodeCache {
    private long hitCount;
    private long missCount;
    private long loadCount;
    private long evictionCount;

    /**
     * Add a mapping from {@code key} to {@code value} where {@code value} is the
     * id of a node at the given {@code depth}. Any existing mapping is replaced.
     */
    public abstract void put(@Nonnull String key, @Nonnull RecordId value, int cost);

    /**
     * @return  The mapping for {@code key}, or {@code null} if none.
     */
    @CheckForNull
    public abstract RecordId get(@Nonnull String key);

    /**
     * @return number of mappings
     */
    public abstract long size();

    public void logSizes() {}

    /**
     * @return  access statistics for this cache
     */
    @Nonnull
    public CacheStats getStats() {
        return new CacheStats(hitCount, missCount, loadCount, 0, 0, evictionCount);
    }

    /**
     * Factory method for creating {@code NodeCache} instances with a given
     * {@code capacity} and maximal depth. The returned instances are all
     * thread safe.
     *
     * Mappings with a depth exceeding {@code maxDepth} will not be added.
     *
     * If the number of mappings exceed {@code capacity} the maximal depth
     * is decreased and all mappings exceeding that new value are removed.
     *
     * @param capacity   maximal number of mappings
     * @param maxDepth   maximal depth
     * @return  A new {@code RecordCache} instance of the given {@code size}.
     */

    @Nonnull
    public static NodeCache newNodeCache(int capacity, int maxDepth) {
        if (capacity <= 0) {
            return new NodeCache.Empty();
        } else {
            return new NodeCache.Default(capacity, maxDepth);
        }
    }

    /**
     * @return  A factory returning {@code Node} instances of the given
     *          {@code capacity} and {@code maxDepth} when invoked.
     * @see #newNodeCache(int, int)
     */
    @Nonnull
    public static Supplier<NodeCache> factory(int capacity, int maxDepth) {
        if (capacity <= 0) {
            return NodeCache.Empty.supplier();
        } else {
            return NodeCache.Default.supplier(capacity, maxDepth);
        }
    }

    private static class Empty extends NodeCache {
        static final Supplier<NodeCache> supplier() {
            return  new Supplier<NodeCache>() {
                @Override
                public NodeCache get() {
                    return new NodeCache.Empty();
                }
            };
        }

        @Override
        public synchronized void put(@Nonnull String key, @Nonnull RecordId value, int cost) { }

        @Override
        public synchronized RecordId get(@Nonnull String key) {
            super.missCount++;
            return null;
        }

        @Override
        public long size() {
            return 0;
        }
    }

    private static class Default extends NodeCache {
        private static final Logger LOG = LoggerFactory.getLogger(Default.class);

        private final int capacity;
        private final List<Map<String, RecordId>> caches;
        private final int[] costs;

        private int size;

        static final Supplier<NodeCache> supplier(final int capacity, final int size) {
            return new Supplier<NodeCache>() {
                @Override
                public NodeCache get() {
                    return new NodeCache.Default(capacity, size);
                }
            };
        }

        Default(int capacity, int levels) {
            checkArgument(capacity > 0);
            checkArgument(levels > 0);
            this.capacity = capacity;
            this.caches = newArrayList();
            // michid make costs configurable
            costs = new int[]{0,1,2,3,4,5,6,7,8,9,20,50,100,500,1000,10000,Integer.MAX_VALUE};
            for (int cost : costs) {
                caches.add(new HashMap<String, RecordId>());
            }
        }

        // michid replace with more sophisticated monitoring
        @Override
        public synchronized void logSizes() {
            for (int k = 0; k < caches.size(); k++) {
                LOG.info("NodeCache size @{}={}", costs[k], caches.get(k).size());
            }
            LOG.info("NodeCache size total={}", size());
        }

        @Override
        public synchronized void put(@Nonnull String key, @Nonnull RecordId value, int cost) {
            for (int k = 0; size >= capacity; k++) {
                Iterator<Entry<String, RecordId>> entries = caches.get(k).entrySet().iterator();
                if (entries.hasNext()) {
                    entries.next();
                    entries.remove();
                    size--;
                    super.evictionCount++;
                }
            }

            for (int k = 0; k < costs.length; k++) {
                if (cost <= costs[k]) {
                    caches.get(k).put(key, value);
                    super.loadCount++;
                    size++;
                    return;
                }
            }
        }

        @Override
        public synchronized RecordId get(@Nonnull String key) {
            for (Map<String, RecordId> cache : caches) {
                if (!cache.isEmpty()) {
                    RecordId recordId = cache.get(key);
                    if (recordId != null) {
                        super.hitCount++;
                        return recordId;
                    }
                }
            }
            super.missCount++;
            return null;
        }

        @Override
        public synchronized long size() {
            long size = 0;
            for (Map<String, RecordId> cache : caches) {
                size += cache.size();
            }
            return size;
        }
    }

}
