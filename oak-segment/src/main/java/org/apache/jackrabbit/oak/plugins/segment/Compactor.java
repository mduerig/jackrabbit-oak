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
package org.apache.jackrabbit.oak.plugins.segment;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool for compacting segments.
 */
// michid add progress logging
public class Compactor {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(Compactor.class);

    private final SegmentWriter writer;

    public Compactor(SegmentTracker tracker) {
        this.writer = createSegmentWriter(tracker);
    }

    @Nonnull
    private static SegmentWriter createSegmentWriter(SegmentTracker tracker) {
        return new SegmentWriter(tracker.getStore(), tracker.getSegmentVersion(),
            new SegmentBufferWriter(tracker.getStore(), tracker.getSegmentVersion(), "c", tracker.getGcGen() + 1));
    }

    /**
     * Compact the differences between a {@code before} and a {@code after}
     * on top of an {@code onto} state.
     * @param before  the before state
     * @param after   the after state
     * @param onto    the onto state
     * @return  the compacted state
     */
    // michid inline as this is really slim now ;-)
    public SegmentNodeState compact(NodeState before, NodeState after, NodeState onto) throws IOException {
        SegmentNodeState nodeState = writer.writeNode(after);
        writer.flush();
        return nodeState;
    }

}
