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

package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState.binaryPropertyFromBlob;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.segment.SegmentTracker.MergeStats;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * FIXME (michid) document
 * FIXME (michid) route through the compaction map of the respective gc cycle for better de-dup!?
 * FIXME (michid) de-dup with ApplyDiff
 */
public class MergeDiff implements NodeStateDiff {
    private final Map<String, List<RecordId>> binaries = newHashMap();
    private final NodeBuilder builder;
    private final SegmentWriter writer;
    private final SegmentNodeState empty;
    private final MergeStats mergeStats;

    public MergeDiff(NodeBuilder builder, SegmentWriter writer, MergeStats mergeStats) throws IOException {
        this(builder, writer, writer.writeNode(EMPTY_NODE), mergeStats);
    }

    private MergeDiff(NodeBuilder builder, SegmentWriter writer, SegmentNodeState empty, MergeStats mergeStats) {
        this.builder = builder;
        this.writer = writer;
        this.empty = empty;
        this.mergeStats = mergeStats;
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        builder.setProperty(copy(after));
        mergeStats.propertyCopyCount.incrementAndGet();
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        builder.setProperty(copy(after));
        mergeStats.propertyCopyCount.incrementAndGet();
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        mergeStats.nodeCopyCount.incrementAndGet();
        return childNodeChanged(name, empty, after);
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        MergeDiff diff = new MergeDiff(builder.child(name), writer, empty, mergeStats);
        after.compareAgainstBaseState(before, diff);
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        builder.removeProperty(before.getName());
        return true;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        builder.child(name).remove();
        return true;
    }

    // FIXME (michid) de-dup with Compactor
    private PropertyState copy(PropertyState after) {
        String name = after.getName();
        Type<?> type = after.getType();
        if (type == BINARY) {
            Blob blob = copy(after.getValue(BINARY));
            return binaryProperty(name, blob);
        } else if (type == BINARIES) {
            List<Blob> blobs = new ArrayList<Blob>();
            for (Blob blob : after.getValue(BINARIES)) {
                blobs.add(copy(blob));
            }
            return binaryPropertyFromBlob(name, blobs);
        } else {
            return createProperty(name, after.getValue(type), type);
        }
    }

    private Blob copy(Blob blob) {
        if (blob instanceof SegmentBlob) {
            SegmentBlob sb = (SegmentBlob) blob;
            try {
                RecordId id = sb.getRecordId();

                // if the blob is inlined or external, just clone it
                if (sb.isExternal() || sb.length() < Segment.MEDIUM_LIMIT) {
                    return sb.clone(writer, true);
                }

                // alternatively look if the exact same binary has been cloned
                String key = ((SegmentBlob) blob).getBlobKey();
                List<RecordId> ids = binaries.get(key);
                if (ids != null) {
                    for (RecordId duplicateId : ids) {
                        SegmentBlob dupBlob = new SegmentBlob(duplicateId);
                        if (dupBlob.equals(sb)) {
                            return dupBlob;
                        }
                    }
                }

                // if not, clone the blob and keep track of the result
                sb = sb.clone(writer, true);
                if (ids == null) {
                    ids = newArrayList();
                    binaries.put(key, ids);
                }
                ids.add(sb.getRecordId());

                return sb;
            } catch (IOException e) {
                // FIXME (michid) LOG.warn("Failed to copy a blob", e);
                // fall through
            }
        }

        // no way to compact this blob, so we'll just keep it as-is
        return blob;
    }
}
