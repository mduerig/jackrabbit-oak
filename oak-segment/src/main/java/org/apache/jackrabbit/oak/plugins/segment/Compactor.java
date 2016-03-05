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

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool for compacting segments.
 */
public class Compactor {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(Compactor.class);

    private final SegmentTracker tracker;

    private final SegmentWriter writer;

    private final ProgressTracker progress = new ProgressTracker();

    /**
     * Allows the cancellation of the compaction process. If this {@code
     * Supplier} returns {@code true}, this compactor will cancel compaction and
     * return a partial {@code SegmentNodeState} containing the changes
     * compacted before the cancellation.
     */
    private final Supplier<Boolean> cancel;

    public Compactor(SegmentTracker tracker) {
        this(tracker, Suppliers.ofInstance(false));
    }

    Compactor(SegmentTracker tracker, Supplier<Boolean> cancel) {
        this.tracker = tracker;
        this.writer = tracker.getWriter();
        this.cancel = cancel;
    }

    // michid remove clone binaries and compaction strategy
    public Compactor(SegmentTracker tracker, CompactionStrategy compactionStrategy, Supplier<Boolean> cancel) {
        this.tracker = tracker;
        this.writer = createSegmentWriter(tracker);
        this.cancel = cancel;
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
        progress.start();
        SegmentNodeBuilder builder = new SegmentNodeBuilder(writer.writeNode(onto), writer);
        new CompactDiff(builder).diff(before, after);
        SegmentNodeState compacted = builder.getNodeState();
        writer.flush();
        progress.stop();
        return compacted;
    }

    private class CompactDiff extends ApplyDiff {
        private IOException exception;

        /**
         * Current processed path, or null if the trace log is not enabled at
         * the beginning of the compaction call. The null check will also be
         * used to verify if a trace log will be needed or not
         */
        private final String path;

        CompactDiff(NodeBuilder builder) {
            super(builder);
            if (log.isTraceEnabled()) {
                this.path = "/";
            } else {
                this.path = null;
            }
        }

        private CompactDiff(NodeBuilder builder, String path, String childName) {
            super(builder);
            if (path != null) {
                this.path = concat(path, childName);
            } else {
                this.path = null;
            }
        }

        boolean diff(NodeState before, NodeState after) throws IOException {
            boolean success = after.compareAgainstBaseState(before, new CancelableDiff(this, cancel));
            if (exception != null) {
                throw new IOException(exception);
            }
            return success;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (path != null) {
                log.trace("propertyAdded {}/{}", path, after.getName());
            }
            progress.onProperty();
            try {
                return super.propertyAdded(compact(after));
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (path != null) {
                log.trace("propertyChanged {}/{}", path, after.getName());
            }
            progress.onProperty();
            try {
                return super.propertyChanged(before, compact(after));
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (path != null) {
                log.trace("childNodeAdded {}/{}", path, name);
            }

            progress.onNode();
            try {
                NodeBuilder child = EMPTY_NODE.builder();
                boolean success =  new CompactDiff(child, path, name).diff(EMPTY_NODE, after);
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
            if (path != null) {
                log.trace("childNodeChanged {}/{}", path, name);
            }

            progress.onNode();
            try {
                NodeBuilder child = builder.getChildNode(name);
                boolean success = new CompactDiff(child, path, name).diff(before, after);
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
            PropertyTemplate template = new PropertyTemplate(property);  // michid this hack might not work, check, improve
            return new SegmentPropertyState(id, template);
        }

    }

    private static class ProgressTracker {
        private final long logAt = Long.getLong("compaction-progress-log",
                150000);

        private long start = 0;

        private long nodes = 0;
        private long properties = 0;
        private long binaries = 0;

        void start() {
            nodes = 0;
            properties = 0;
            binaries = 0;
            start = System.currentTimeMillis();
        }

        void onNode() {
            if (++nodes % logAt == 0) {
                logProgress(start, false);
                start = System.currentTimeMillis();
            }
        }

        void onProperty() {
            properties++;
        }

        void onBinary() {
            binaries++;
        }

        void stop() {
            logProgress(start, true);
        }

        private void logProgress(long start, boolean done) {
            log.debug(
                    "Compacted {} nodes, {} properties, {} binaries in {} ms.",
                    nodes, properties, binaries, System.currentTimeMillis()
                            - start);
            if (done) {
                log.info(
                        "Finished compaction: {} nodes, {} properties, {} binaries.",
                        nodes, properties, binaries);
            }
        }
    }

    private static class OfflineCompactionPredicate implements
            Predicate<NodeState> {

        /**
         * over 64K in size, node will be included in the compaction map
         */
        private static final long offlineThreshold = 65536;

        @Override
        public boolean apply(NodeState state) {
            if (state.getChildNodeCount(2) > 1) {
                return true;
            }
            long count = 0;
            for (PropertyState ps : state.getProperties()) {
                Type<?> type = ps.getType();
                for (int i = 0; i < ps.count(); i++) {
                    long size = 0;
                    if (type == BINARY || type == BINARIES) {
                        Blob blob = ps.getValue(BINARY, i);
                        if (blob instanceof SegmentBlob) {
                            if (!((SegmentBlob) blob).isExternal()) {
                                size += blob.length();
                            }
                        } else {
                            size += blob.length();
                        }
                    } else {
                        size = ps.size(i);
                    }
                    count += size;
                    if (size >= offlineThreshold || count >= offlineThreshold) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

}
