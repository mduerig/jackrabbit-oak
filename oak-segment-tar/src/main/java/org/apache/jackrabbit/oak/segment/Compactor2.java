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
 *
 */
package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;
import static org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState.binaryPropertyFromBlob;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.io.IOException;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO michid doc
// TODO michid rename
// TODO michid unify with Compactor?!
// TODO michid add progress tracker!?
// TODO michid logging
public class Compactor2 {
    private static final Logger log = LoggerFactory.getLogger(Compactor2.class);

    @Nonnull
    private final SegmentWriter writer;

    @Nonnull
    private final Supplier<Boolean> cancel;

    public Compactor2(
            @Nonnull SegmentWriter writer,
            @Nonnull Supplier<Boolean> cancel) {
        this.writer = checkNotNull(writer);
        this.cancel = checkNotNull(cancel);
    }

    @CheckForNull
    public SegmentNodeState compact(@Nonnull NodeState state) throws IOException {
        return writer.compactNode(checkNotNull(state), null, cancel);
    }

    @CheckForNull
    public SegmentNodeState compact(
            @Nonnull NodeState before,
            @Nonnull NodeState after,
            @Nonnull NodeState onto)
    throws IOException {
        checkNotNull(before);
        checkNotNull(after);
        checkNotNull(onto);
        SegmentNodeState compacted = writer.compactNode(onto, null, cancel);
        if (compacted != null) {
            return new CompactDiff(compacted).diff(before, after);
        } else {
            return null;
        }
    }

    @CheckForNull
    private static byte[] getStableIdBytes(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return ((SegmentNodeState) state).getStableIdBytes();
        } else {
            return null;
        }
    }

    private class CompactDiff implements NodeStateDiff {
        @Nonnull
        private final MemoryNodeBuilder builder;

        @Nonnull
        private final NodeState base;

        @CheckForNull
        private IOException exception;

        CompactDiff(@Nonnull NodeState base) {
            this.builder = new MemoryNodeBuilder(checkNotNull(base));
            this.base = base;
        }

        @CheckForNull
        SegmentNodeState diff(@Nonnull NodeState before, @Nonnull NodeState after) throws IOException {
            boolean success = after.compareAgainstBaseState(before, new CancelableDiff(this, cancel));
            if (exception != null) {
                throw new IOException(exception);
            } else if (success) {
                return writer.compactNode(builder.getNodeState(), getStableIdBytes(after), cancel);
            } else {
                return null;
            }
        }

        @Override
        public boolean propertyAdded(@Nonnull PropertyState after) {
            builder.setProperty(compact(after));
            return true;
        }

        @Override
        public boolean propertyChanged(@Nonnull PropertyState before, @Nonnull PropertyState after) {
            builder.setProperty(compact(after));
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            builder.removeProperty(before.getName());
            return true;
        }

        @Override
        public boolean childNodeAdded(@Nonnull String name, @Nonnull NodeState after) {
            try {
                SegmentNodeState compacted = compact(after);
                if (compacted != null) {
                    builder.setChildNode(name, compacted);
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean childNodeChanged(@Nonnull String name, @Nonnull NodeState before, @Nonnull NodeState after) {
            try {
                SegmentNodeState compacted = writer.deduplicateNode(after);
                if (compacted == null) {
                    compacted = new CompactDiff(base.getChildNode(name)).diff(before, after);
                }
                if (compacted != null) {
                    builder.setChildNode(name, compacted);
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            builder.getChildNode(name).remove();
            return true;
        }
    }

    // TODO michid deduplicate binaries, strings here?
    @Nonnull
    private static PropertyState compact(@Nonnull PropertyState property) {
        String name = property.getName();
        Type<?> type = property.getType();
        if (type == BINARY) {
            return binaryProperty(name, property.getValue(Type.BINARY));
        } else if (type == BINARIES) {
            List<Blob> blobs = newArrayList();
            for (Blob blob : property.getValue(BINARIES)) {
                blobs.add(blob);
            }
            return binaryPropertyFromBlob(name, blobs);
        } else {
            return createProperty(name, property.getValue(type), type);
        }
    }

}
