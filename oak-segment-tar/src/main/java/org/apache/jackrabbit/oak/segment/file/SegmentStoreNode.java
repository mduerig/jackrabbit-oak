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

import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.segment.file.tar.TarReader;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO michid document
 */
public class SegmentStoreNode extends AbstractNodeState {
    private final List<TarReader> tarReaders;

    public SegmentStoreNode(@Nonnull List<TarReader> tarReaders) {
        this.tarReaders = tarReaders;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        // TODO michid return segmentstore meta data: path, size on disk etc.
        return emptyList();
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return tarReaders.stream().anyMatch(
                r -> name.equals(r.getFileName()));
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
        return tarReaders.stream()
                .filter(r -> name.equals(r.getFileName()))
                .findFirst()
                .map(this::newTarNode)
                .orElse(MISSING_NODE);
    }

    @Nonnull
    private NodeState newTarNode(@Nonnull TarReader tarReader) {
        return new TarNode(tarReader);
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return (() -> tarReaders.stream()
                        .map(this::newChildNodeEntry)
                        .iterator());
    }

    @Nonnull
    private ChildNodeEntry newChildNodeEntry(@Nonnull TarReader tarReader) {
        return new MemoryChildNodeEntry(tarReader.getFileName(), newTarNode(tarReader));
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        throw new UnsupportedOperationException();
    }
}
