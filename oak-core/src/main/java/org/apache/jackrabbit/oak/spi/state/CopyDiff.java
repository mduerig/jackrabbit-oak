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

package org.apache.jackrabbit.oak.spi.state;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.compareAgainstEmptyState;
import static org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState.binaryPropertyFromBlob;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlob;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyDiff implements NodeStateDiff {
    private static final Logger LOG = LoggerFactory.getLogger(CopyDiff.class);

    private final NodeBuilder builder;
    private final SegmentWriter writer;
    private final NodeState empty;

    public CopyDiff(NodeBuilder builder, NodeState empty) {
        this(builder, toSegmentNodeState(empty).getTracker().getWriter(), empty);
    }

    private static SegmentNodeState toSegmentNodeState(NodeState empty) {
        // michid this should work for any node state
        checkArgument(empty instanceof SegmentNodeState);
        return (SegmentNodeState) empty;
    }

    private CopyDiff(NodeBuilder builder, SegmentWriter writer, NodeState empty) {
        this.builder = builder;
        this.writer = writer;
        this.empty = empty;
    }

    // michid move to common location and share with dup in CompactorDiff
    private PropertyState copy(PropertyState property) {
        String name = property.getName();
        Type<?> type = property.getType();
        if (type == BINARY) {
            Blob blob = copy(property.getValue(BINARY));
            return binaryProperty(name, blob);
        } else if (type == BINARIES) {
            List<Blob> blobs = new ArrayList<Blob>();
            for (Blob blob : property.getValue(BINARIES)) {
                blobs.add(copy(blob));
            }
            return binaryPropertyFromBlob(name, blobs);
        } else {
            return createProperty(name, property.getValue(type), type);
        }
    }

    private final Map<String, List<RecordId>> binaries = newHashMap();

    // michid move to common location and share with dup in CompactorDiff
    private Blob copy(Blob blob) {
        if (blob instanceof SegmentBlob) {
            SegmentBlob sb = (SegmentBlob) blob;
            try {
                RecordId id = sb.getRecordId();

                // if the blob is inlined or external, just clone it
                if (sb.isExternal() || sb.length() < Segment.MEDIUM_LIMIT) {
                    return sb.clone(writer, false);
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
                sb = sb.clone(writer, false);
                if (ids == null) {
                    ids = newArrayList();
                    binaries.put(key, ids);
                }
                ids.add(sb.getRecordId());

                return sb;
            } catch (IOException e) {
                LOG.warn("Failed to copy a blob", e);
                // fall through
            }
        }

        // no way to compact this blob, so we'll just keep it as-is
        return blob;
    }

    private NodeState copy(NodeState nodeState) {
        NodeBuilder builder = empty.builder();
        compareAgainstEmptyState(nodeState, new ApplyDiff(builder));
        return builder.getNodeState();
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        builder.setProperty(copy(after));
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        builder.setProperty(copy(after));
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        builder.removeProperty(before.getName());
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        builder.setChildNode(name, copy(after));
        return true;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        return after.compareAgainstBaseState(before, new CopyDiff(builder.getChildNode(name), writer, empty));
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        builder.getChildNode(name).remove();
        return true;
    }
}
