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

import static com.google.common.collect.Queues.newConcurrentLinkedQueue;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import com.google.common.cache.Weigher;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.segment.CacheWeights.SegmentCacheWeigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache for {@link Segment} instances by their {@link SegmentId}.
 * <p>
 * Conceptually this cache serves as a 2nd level cache for segments. The 1st level cache is
 * implemented by memoising the segment in its id (see {@link SegmentId#segment}. Every time
 * an segment is evicted from this cache the memoised segment is discarded (see
 * {@link SegmentId#unloaded()}).
 * <p>
 * As a consequence this cache is actually only queried for segments it does not contain,
 * which are then loaded through the loader passed to {@link #getSegment(SegmentId, Callable)}.
 * This behaviour is eventually reflected in the cache statistics (see {@link #getCacheStats()}),
 * which always reports a {@link CacheStats#getHitRate()} () miss rate} of 1.
 */
public class SegmentCache {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentCache.class);

    // michid remove? Configurable?, default?
    private static final boolean INCLUDE_BULK = Boolean.getBoolean("segment-cache.include.bulk");

    public static final int DEFAULT_SEGMENT_CACHE_MB = 256;

    @Nonnull
    private final Weigher<SegmentId, Segment> weigher = new SegmentCacheWeigher();

    /**
     * Cache of recently accessed segments
     */
    @Nonnull
    private final ConcurrentLinkedQueue<Segment> segments = newConcurrentLinkedQueue();

    @Nonnull
    private final AtomicLong currentWeight = new AtomicLong();

    @Nonnull
    private final AtomicInteger elementCount = new AtomicInteger();

    @Nonnull
    private final AtomicLong loadSuccessCount = new AtomicLong();

    @Nonnull
    private final AtomicInteger loadExceptionCount = new AtomicInteger();

    @Nonnull
    private final AtomicLong loadTime = new AtomicLong();

    @Nonnull
    private final AtomicLong evictionCount = new AtomicLong();

    @Nonnull
    private final AtomicLong missCount = new AtomicLong();

    @Nonnull
    static final AtomicLong hitCount = new AtomicLong();  // michid properly inject

    @Nonnull
    private final Stats stats = new Stats("Segment Cache");

    private final long maximumWeight;

    /**
     * Create a new segment cache of the given size.
     * @param cacheSizeMB  size of the cache in megabytes.
     */
    public SegmentCache(long cacheSizeMB) {
        this.maximumWeight = cacheSizeMB * 1024 * 1024;
    }

    private void put(@Nonnull SegmentId id, @Nonnull Segment segment) {
        if (!INCLUDE_BULK && id.isBulkSegmentId()) {
            return;
        }

        long size = weigher.weigh(id, segment);
        id.loaded(segment);
        segments.add(segment);
        currentWeight.addAndGet(size);
        elementCount.incrementAndGet();
        LOG.debug("Put segment {} into segment cache ({} bytes)", id, size);

        int failedEvictions = 0;
        while (currentWeight.get() > maximumWeight) {
            Segment head = segments.poll();
            if (head == null) {
                return;
            }
            SegmentId headId = head.getSegmentId();
            if (head.accessed()) {
                failedEvictions++;
                segments.add(head);
                LOG.debug("Segment {} was recently used, keeping in segment cache", headId);
            } else {
                headId.unloaded();
                long lastSize = weigher.weigh(headId, head);
                currentWeight.addAndGet(-lastSize);
                elementCount.decrementAndGet();
                evictionCount.incrementAndGet();
                LOG.debug("Evicted segment {} from segment cache ({} bytes) after {} eviction attempts",
                        headId, lastSize, failedEvictions);
            }
        }
    }

    /**
     * Retrieve an segment from the cache or load it and cache it if not yet in the cache.
     * @param id        the id of the segment
     * @param loader    the loader to load the segment if not yet in the cache
     * @return          the segment identified by {@code id}
     * @throws ExecutionException  when {@code loader} failed to load an segment
     */
    @Nonnull
    public Segment getSegment(@Nonnull final SegmentId id, @Nonnull final Callable<Segment> loader)
    throws ExecutionException {
        try {
            missCount.incrementAndGet();
            long t0 = System.nanoTime();
            Segment segment = loader.call();
            loadTime.addAndGet(System.nanoTime() - t0);
            loadSuccessCount.incrementAndGet();
            put(id, segment);
            return segment;
        } catch (Exception e) {
            loadExceptionCount.incrementAndGet();
            throw new ExecutionException(e);
        }
    }

    /**
     * Put a segment into the cache
     * @param segment  the segment to cache
     */
    public void putSegment(@Nonnull Segment segment) {
        SegmentId segmentId = segment.getSegmentId();
        put(segmentId, segment);
    }

    /**
     * Clear all segment from the cache
     */
    public synchronized void clear() {
        for (Iterator<Segment> iterator = segments.iterator(); iterator.hasNext(); ) {
            Segment segment = iterator.next();
            SegmentId id = segment.getSegmentId();
            id.unloaded();
            iterator.remove();
            currentWeight.addAndGet(weigher.weigh(id, segment));
            elementCount.decrementAndGet();
        }
    }

    /**
     * See the class comment regarding some peculiarities of this cache's statistics
     * @return  statistics for this cache.
     */
    @Nonnull
    public AbstractCacheStats getCacheStats() {
        return stats;
    }

    private class Stats extends AbstractCacheStats {
        protected Stats(@Nonnull String name) {
            super(name);
        }

        @Override
        protected com.google.common.cache.CacheStats getCurrentStats() {
            return new com.google.common.cache.CacheStats(
                    hitCount.get(),
                    missCount.get(),
                    loadSuccessCount.get(),
                    loadExceptionCount.get(),
                    loadTime.get(),
                    evictionCount.get());
        }

        @Override
        public long getElementCount() {
            return elementCount.get();
        }

        @Override
        public long getMaxTotalWeight() {
            return maximumWeight;
        }

        @Override
        public long estimateCurrentWeight() {
            return currentWeight.get();
        }
    }
}