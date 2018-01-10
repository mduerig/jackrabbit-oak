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
 */

package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType.FULL;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType.TAIL;
import static org.apache.jackrabbit.oak.segment.file.Reclaimers.newExactReclaimer;
import static org.apache.jackrabbit.oak.segment.file.Reclaimers.newOldReclaimer;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.junit.Test;

public class ReclaimersTest {

    private static void testOldReclaimerForTail(boolean isCompacted) {
        Predicate<GCGeneration> reclaimer = newOldReclaimer(TAIL, newGCGeneration(3, 3, isCompacted), 2);

        // Don't reclaim young segments
        assertFalse(reclaimer.apply(newGCGeneration(3, 3, false)));
        assertFalse(reclaimer.apply(newGCGeneration(3, 3, true)));
        assertFalse(reclaimer.apply(newGCGeneration(2, 3, false)));
        assertFalse(reclaimer.apply(newGCGeneration(2, 3, true)));

        // Reclaim old and uncompacted segments
        assertTrue(reclaimer.apply(newGCGeneration(1, 3, false)));
        assertTrue(reclaimer.apply(newGCGeneration(0, 3, false)));

        // Don't reclaim old compacted segments from the same full generation
        assertFalse(reclaimer.apply(newGCGeneration(1, 3, true)));
        assertFalse(reclaimer.apply(newGCGeneration(0, 3, true)));

        // Reclaim old compacted segments from prior full generations
        assertTrue(reclaimer.apply(newGCGeneration(1, 2, true)));
        assertTrue(reclaimer.apply(newGCGeneration(1, 2, false)));
        assertTrue(reclaimer.apply(newGCGeneration(0, 2, true)));
        assertTrue(reclaimer.apply(newGCGeneration(0, 2, false)));
    }

    private static void testOldReclaimerSequenceForTail(boolean isCompacted) {
        Predicate<GCGeneration> reclaimer = newOldReclaimer(TAIL, newGCGeneration(9, 2, isCompacted), 2);

        assertFalse(reclaimer.apply(newGCGeneration(9, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(9, 2, true)));
        assertFalse(reclaimer.apply(newGCGeneration(8, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(8, 2, true)));
        assertTrue(reclaimer.apply(newGCGeneration(7, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(7, 2, true)));
        assertTrue(reclaimer.apply(newGCGeneration(6, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(6, 2, true)));

        assertTrue(reclaimer.apply(newGCGeneration(5, 1, false)));
        assertTrue(reclaimer.apply(newGCGeneration(5, 1, true)));
        assertTrue(reclaimer.apply(newGCGeneration(4, 1, false)));
        assertTrue(reclaimer.apply(newGCGeneration(4, 1, true)));
        assertTrue(reclaimer.apply(newGCGeneration(3, 1, false)));
        assertTrue(reclaimer.apply(newGCGeneration(3, 1, true)));

        assertTrue(reclaimer.apply(newGCGeneration(2, 0, false)));
        assertTrue(reclaimer.apply(newGCGeneration(2, 0, true)));
        assertTrue(reclaimer.apply(newGCGeneration(1, 0, false)));
        assertTrue(reclaimer.apply(newGCGeneration(1, 0, true)));
        assertTrue(reclaimer.apply(newGCGeneration(0, 0, false)));
    }

    private static void testOldReclaimerForFull(boolean isCompacted) {
        Predicate<GCGeneration> reclaimer = newOldReclaimer(FULL, newGCGeneration(6, 3, isCompacted), 2);

        // Don't reclaim segments from young full generation
        assertFalse(reclaimer.apply(newGCGeneration(6, 3, false)));
        assertFalse(reclaimer.apply(newGCGeneration(6, 3, true)));
        assertFalse(reclaimer.apply(newGCGeneration(6, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(6, 2, true)));

        // Reclaim segments from old full generation
        assertTrue(reclaimer.apply(newGCGeneration(6, 1, false)));
        assertTrue(reclaimer.apply(newGCGeneration(6, 1, true)));
        assertTrue(reclaimer.apply(newGCGeneration(6, 0, false)));
        assertTrue(reclaimer.apply(newGCGeneration(6, 0, true)));

        // Reclaim segments from old generation if not compacted
        assertTrue(reclaimer.apply(newGCGeneration(4, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(4, 2, true)));
        assertTrue(reclaimer.apply(newGCGeneration(3, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(3, 2, true)));
    }

    private static void testOldReclaimerSequenceForFull(boolean isCompacted) {
        Predicate<GCGeneration> reclaimer = newOldReclaimer(FULL, newGCGeneration(9, 9, isCompacted), 2);

        assertFalse(reclaimer.apply(newGCGeneration(9, 9, false)));
        assertFalse(reclaimer.apply(newGCGeneration(9, 9, true)));
        assertFalse(reclaimer.apply(newGCGeneration(8, 8, false)));
        assertFalse(reclaimer.apply(newGCGeneration(8, 8, true)));
        assertTrue(reclaimer.apply(newGCGeneration(7, 7, false)));
        assertTrue(reclaimer.apply(newGCGeneration(7, 7, true)));
        assertTrue(reclaimer.apply(newGCGeneration(6, 6, false)));
        assertTrue(reclaimer.apply(newGCGeneration(6, 6, true)));

        assertTrue(reclaimer.apply(newGCGeneration(5, 1, false)));
        assertTrue(reclaimer.apply(newGCGeneration(5, 1, true)));
        assertTrue(reclaimer.apply(newGCGeneration(4, 1, false)));
        assertTrue(reclaimer.apply(newGCGeneration(4, 1, true)));
        assertTrue(reclaimer.apply(newGCGeneration(3, 1, false)));
        assertTrue(reclaimer.apply(newGCGeneration(3, 1, true)));

        assertTrue(reclaimer.apply(newGCGeneration(2, 0, false)));
        assertTrue(reclaimer.apply(newGCGeneration(2, 0, true)));
        assertTrue(reclaimer.apply(newGCGeneration(1, 0, false)));
        assertTrue(reclaimer.apply(newGCGeneration(1, 0, true)));
        assertTrue(reclaimer.apply(newGCGeneration(0, 0, false)));
    }

    @Test
    public void testOldReclaimerCompactedHead() {
        testOldReclaimerForTail(true);
        testOldReclaimerForFull(true);
    }

    @Test
    public void testOldReclaimerUncompactedHead() {
        testOldReclaimerForTail(false);
        testOldReclaimerForFull(false);
    }

    @Test
    public void testOldReclaimerSequenceCompactedHead() throws Exception {
        testOldReclaimerSequenceForTail(true);
        testOldReclaimerSequenceForFull(true);
    }

    @Test
    public void testOldReclaimerSequenceUncompactedHead() throws Exception {
        testOldReclaimerSequenceForTail(false);
        testOldReclaimerSequenceForFull(false);
    }

    @Test
    public void testExactReclaimer() {
        Predicate<GCGeneration> reclaimer = newExactReclaimer(newGCGeneration(3, 3, false));
        assertTrue(reclaimer.apply(newGCGeneration(3, 3, false)));
        assertFalse(reclaimer.apply(newGCGeneration(3, 3, true)));
        assertFalse(reclaimer.apply(newGCGeneration(3, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(2, 3, false)));
    }
}
