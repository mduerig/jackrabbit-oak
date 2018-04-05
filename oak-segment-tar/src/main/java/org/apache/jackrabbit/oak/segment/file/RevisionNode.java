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
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.function.Function;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO michid document
 */
public class RevisionNode extends AbstractNodeState {
    @Nonnull
    private final JournalEntry journalEntry;

    @Nonnull
    private final Function<JournalEntry, NodeState> nodeResolver;

    public RevisionNode(@Nonnull JournalEntry journalEntry,
                        @Nonnull Function<JournalEntry, NodeState> nodeResolver) {
        this.journalEntry = journalEntry;
        this.nodeResolver = nodeResolver;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return emptyList(); // TODO michid expose revision meta data
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return "root".equals(name);
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
        if ("root".equals(name)) {
            return newRoot(journalEntry);
        } else {
            return MISSING_NODE;
        }
    }

    @Nonnull
    private NodeState newRoot(@Nonnull JournalEntry journalEntry) {
        return nodeResolver.apply(journalEntry);
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return singletonList(newChildNodeEntry(journalEntry));
    }

    @Nonnull
    private ChildNodeEntry newChildNodeEntry(@Nonnull JournalEntry journalEntry) {
        return new MemoryChildNodeEntry("root", newRoot(journalEntry));
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        throw new UnsupportedOperationException();
    }
}
