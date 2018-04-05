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

import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterators.limit;
import static com.google.common.collect.Iterators.size;
import static java.lang.String.valueOf;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO michid document
 */
public class JournalNode extends AbstractNodeState {

    @Nonnull
    private final Function<JournalEntry, NodeState> nodeResolver;

    @Nonnull
    private final Iterable<JournalEntry> journalEntries;

    public JournalNode(@Nonnull Function<JournalEntry, NodeState> nodeResolver,
                       @Nonnull Supplier<JournalReader> journal) {
        this.nodeResolver = nodeResolver;

        // TODO michid snap-shotting the journal does not work under truncation
        journalEntries = new Iterable<JournalEntry>() {
            int limit = size(journal.get());

            @Nonnull
            @Override
            public Iterator<JournalEntry> iterator() {
                return limit(journal.get(), limit);
            }
        };
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return emptyList(); // TODO michid return journal meta data
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return find(journalEntries, entry -> name.equals(nameOf(entry)), null) != null;
    }

    @Nonnull
    private static String nameOf(@Nullable JournalEntry entry) {
        return valueOf(requireNonNull(entry).getTimestamp());
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
        JournalEntry journalEntry = find(journalEntries, entry -> name.equals(nameOf(entry)), null);
        if (journalEntry == null) {
            return MISSING_NODE;
        } else {
            return newRevisionNode(journalEntry);
        }
    }

    @Nonnull
    private NodeState newRevisionNode(@Nonnull JournalEntry journalEntry) {
        return new RevisionNode(journalEntry, nodeResolver);
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return transform(journalEntries, this::newChildNodeEntry);
    }

    @Nonnull
    private ChildNodeEntry newChildNodeEntry(@Nonnull JournalEntry entry) {
        return new MemoryChildNodeEntry(nameOf(entry), newRevisionNode(entry));
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        throw new UnsupportedOperationException();
    }
}
