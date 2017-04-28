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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.junit.Ignore;
import org.junit.Test;

public class SegmentCacheTest {
    @Test
    public void putTest() {
        SegmentId id = new SegmentId(mock(SegmentStore.class), -1, -1);
        Segment segment = mock(Segment.class);
        when(segment.getSegmentId()).thenReturn(id);
        SegmentCache cache = new SegmentCache();

        cache.putSegment(segment);
        assertEquals(segment, id.getSegment());
    }

    @Test
    public void invalidateTests() {
        Segment segment1 = mock(Segment.class);
        Segment segment2 = mock(Segment.class);
        SegmentStore store = mock(SegmentStore.class);
        SegmentId id = new SegmentId(store, -1, -1);
        when(segment1.getSegmentId()).thenReturn(id);
        SegmentCache cache = new SegmentCache();

        cache.putSegment(segment1);
        assertEquals(segment1, id.getSegment());

        // Clearing the cache should cause an eviction call back for id
        cache.clear();

        // Check that this was the case by loading a different segment
        when(store.readSegment(id)).thenReturn(segment2);
        assertEquals(segment2, id.getSegment());
    }

    @Test
    @Ignore // michid fix hit injection of hit counter
    public void statsTest() throws Exception {
        SegmentId id = new SegmentId(mock(SegmentStore.class), -1, -1);
        Segment segment = mock(Segment.class);
        doReturn(id).when(segment).getSegmentId();

        SegmentCache cache = new SegmentCache();
        AbstractCacheStats stats = cache.getCacheStats();

        // empty cache
        assertEquals(0, stats.getElementCount());
        assertEquals(0, stats.getLoadCount());
        assertEquals(0, stats.getHitCount());
        assertEquals(0, stats.getMissCount());
        assertEquals(0, stats.getRequestCount());

        // load
        cache.getSegment(id, () -> segment);
        assertEquals(1, stats.getElementCount());
        assertEquals(1, stats.getLoadCount());
        assertEquals(0, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(1, stats.getRequestCount());

        // cache hit
        id.getSegment();
        assertEquals(1, stats.getElementCount());
        assertEquals(1, stats.getLoadCount());
        assertEquals(1, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(2, stats.getRequestCount());

        cache.clear();
        assertEquals(0, stats.getElementCount());
        assertEquals(1, stats.getLoadCount());
        assertEquals(1, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(2, stats.getRequestCount());

        stats.resetStats();
        assertEquals(0, stats.getElementCount());
        assertEquals(0, stats.getLoadCount());
        assertEquals(0, stats.getHitCount());
        assertEquals(0, stats.getMissCount());
        assertEquals(0, stats.getRequestCount());
    }

}