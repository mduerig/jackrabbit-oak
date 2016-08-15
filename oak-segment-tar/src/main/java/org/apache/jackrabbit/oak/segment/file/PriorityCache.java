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

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.bitCount;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.util.Arrays.fill;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.cache.CacheStats;

// michid: doc
// michid optimise stable id from string to (long, long, short)
public class PriorityCache<K, V> {
    private final int rehash;
    private final Entry<?,?>[] entries;
    private final int[] costs = new int[256];
    private final int[] evictions = new int[256];

    private long hitCount;
    private long missCount;
    private long loadCount;
    private long evictionCount;
    private long size;

    private static class Entry<K, V> {
        static Entry<Void, Void> NULL = new Entry<>(null, null, -1, Byte.MIN_VALUE);

        final K key;
        final V value;
        final int generation;
        byte cost;

        public Entry(K key, V value, int generation, byte cost) {
            this.key = key;
            this.value = value;
            this.generation = generation;
            this.cost = cost;
        }

        @Override
        public String toString() {
            return this == NULL
                ? "NULL"
                : "Entry{" + key + "->" + value + " @" + generation + ", $" + cost + "}";
        }
    }

    // michid doc size must be power of 2
    // michid doc rehash >= 0,  rehash + lb size < 32
    public PriorityCache(int size, int rehash) {
        checkArgument(bitCount(size) == 1);
        checkArgument(rehash >= 0);
        checkArgument(rehash + numberOfTrailingZeros(size) < 32);
        this.rehash = rehash;
        entries = new Entry<?,?>[size];
        fill(entries, Entry.NULL);
    }

    private int project(int hashCode, int iteration) {
        return (hashCode >> iteration) & (entries.length - 1);
    }

    public long size() {
        return size;
    }

    public synchronized boolean put(@Nonnull K key, @Nonnull V value, int generation, byte initialCost) {
        int hashCode = key.hashCode();
        byte cheapest = initialCost;
        int index = -1;
        boolean eviction = false;
        for (int k = 0; k <= rehash; k++) {
            int i = project(hashCode, k);
            Entry<?, ?> entry = entries[i];
            if (entry == Entry.NULL) {
                // Empty slot -> use this index
                index = i;
                break;
            } else if (entry.generation <= generation && key.equals(entry.key)) {
                // Key exists and generation is greater or equal -> use this index and boost the cost
                index = i;
                initialCost = entry.cost;
                if (initialCost < Byte.MAX_VALUE) {
                    initialCost++;
                }
                break;
            } else if (entry.generation < generation) {
                // Old generation -> use this index
                index = i;
                break;
            } else if (entry.cost < cheapest) {
                // Candidate slot, keep on searching for even cheaper slots
                cheapest = entry.cost;
                index = i;
                eviction = true;
            }
        }

        if (index >= 0) {
            Entry<?, ?> old = entries[index];
            entries[index] = new Entry<K, V>(key, value, generation, initialCost);
            loadCount++;
            costs[initialCost - Byte.MIN_VALUE]++;
            if (old != Entry.NULL) {
                costs[old.cost - Byte.MIN_VALUE]--;
                if (eviction) {
                    evictions[old.cost - Byte.MIN_VALUE]++;
                    evictionCount++;
                }
            } else {
                size++;
            }
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    @CheckForNull
    public synchronized V get(@Nonnull K key, int generation) {
        int hashCode = key.hashCode();
        for (int k = 0; k <= rehash; k++) {
            int i = project(hashCode, k);
            Entry<?, ?> entry = entries[i];
            if (generation == entry.generation && key.equals(entry.key)) {
                if (entry.cost < Byte.MAX_VALUE) {
                    costs[entry.cost - Byte.MIN_VALUE]--;
                    entry.cost++;
                    costs[entry.cost - Byte.MIN_VALUE]++;
                }
                hitCount++;
                return (V) entry.value;
            }
        }
        missCount++;
        return null;
    }

    @Override
    public synchronized String toString() {
        return "PriorityCache" +
            "{ costs=" + toString(costs) +
            ", evictions=" + toString(evictions) + " }";
    }

    private static String toString(int[] ints) {
        StringBuilder b = new StringBuilder("[");
        String sep = "";
        for (int i = 0; i < ints.length; i++) {
            if (ints[i] > 0) {
                b.append(sep).append(i).append("->").append(ints[i]);
                sep = ",";
            }
        }
        return b.append(']').toString();
    }

    /**
     * @return  access statistics for this cache
     */
    @Nonnull
    public CacheStats getStats() {
        return new CacheStats(hitCount, missCount, loadCount, 0, 0, evictionCount);
    }

}
