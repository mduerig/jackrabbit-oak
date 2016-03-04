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

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
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
    public SegmentNodeState compact(NodeState before, NodeState after, NodeState onto) throws IOException {
        SegmentNodeBuilder builder = new SegmentNodeBuilder(writer.writeNode(onto), writer);
        new CompactDiff(builder).diff(before, after);
        SegmentNodeState compacted = builder.getNodeState();
        writer.flush();
        return compacted;
    }

    private class CompactDiff extends ApplyDiff {
        private IOException exception;

        CompactDiff(NodeBuilder builder) {
            super(builder);
        }

        boolean diff(NodeState before, NodeState after) throws IOException {
            boolean success = after.compareAgainstBaseState(before, this);
            if (exception != null) {
                throw new IOException(exception);
            }
            return success;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            try {
                return super.propertyAdded(compact(after));
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            try {
                return super.propertyChanged(before, compact(after));
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            try {
                NodeBuilder child = EMPTY_NODE.builder();
                boolean success =  new CompactDiff(child).diff(EMPTY_NODE, after);
                if (success) {
                    builder.setChildNode(name, writer.writeNode(child.getNodeState()));
                }
                return success;
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean childNodeChanged(
                String name, NodeState before, NodeState after) {
            try {
                NodeBuilder child = builder.getChildNode(name);
                boolean success = new CompactDiff(child).diff(before, after);
                if (success) {
                    writer.writeNode(child.getNodeState()).getRecordId();
                }
                return success;
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        private PropertyState compact(PropertyState property) throws IOException {
            RecordId id = writer.writeProperty(property);
            PropertyTemplate template = new PropertyTemplate(property);  // michid this hack might not work, but it will go away anyway
            return new SegmentPropertyState(id, template);
        }

    }

}
