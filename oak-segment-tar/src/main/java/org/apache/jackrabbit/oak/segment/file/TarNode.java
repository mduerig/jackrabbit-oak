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

package org.apache.jackrabbit.oak.segment.file;

import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.UUID;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.segment.file.tar.TarReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO michid document
 */
public class TarNode extends AbstractNodeState {
    @Nonnull
    private final TarReader tarReader;

    public TarNode(@Nonnull TarReader tarReader) {this.tarReader = tarReader;}

    @Override
    public boolean exists() {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return emptyList(); // TODO michid return tar meta data
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return stream(tarReader.getEntries())
                .anyMatch(segment -> name.equals(nameOf(segment)));
    }

    @Nonnull
    private static String nameOf(@Nonnull SegmentArchiveEntry segment) {
        return new UUID(segment.getMsb(), segment.getLsb()).toString();
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
        return stream(tarReader.getEntries())
                .filter(segment -> name.equals(nameOf(segment)))
                .findFirst()
                .map(TarNode::newSegmentNode)
                .orElse(MISSING_NODE);
    }

    @Nonnull
    private static NodeState newSegmentNode(@Nonnull SegmentArchiveEntry segment) {
        return new SegmentNode(segment);
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return (() -> stream(tarReader.getEntries())
                        .map(TarNode::newChildNodeEntry)
                        .iterator());
    }

    @Nonnull
    private static ChildNodeEntry newChildNodeEntry(@Nonnull SegmentArchiveEntry segment) {
        return new MemoryChildNodeEntry(nameOf(segment), newSegmentNode(segment));
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        throw new UnsupportedOperationException();
    }
}
