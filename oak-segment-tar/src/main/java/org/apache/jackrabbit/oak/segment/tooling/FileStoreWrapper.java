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

package org.apache.jackrabbit.oak.segment.tooling;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Sets.newConcurrentHashSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.FinalizablePhantomReference;
import com.google.common.base.FinalizableReference;
import com.google.common.base.FinalizableReferenceQueue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.AbstractFileStore.FileStoreProbe;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles.TarProbe;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.tooling.filestore.IOMonitor;
import org.apache.jackrabbit.oak.tooling.filestore.JournalEntry;
import org.apache.jackrabbit.oak.tooling.filestore.Node;
import org.apache.jackrabbit.oak.tooling.filestore.RecordId;
import org.apache.jackrabbit.oak.tooling.filestore.Segment;
import org.apache.jackrabbit.oak.tooling.filestore.Store;
import org.apache.jackrabbit.oak.tooling.filestore.Tar;

public class FileStoreWrapper implements Store, Closeable {

    @Nonnull
    private final FileStoreProbe fileStoreProbe;

    @Nonnull
    private final TarProbe tarProbe;

    @Nonnull
    private final Function<IOMonitor, Closeable> ioMonitorRegistry;

    @Nonnull
    private final FinalizableReferenceQueue referenceQueue = new FinalizableReferenceQueue();

    @Nonnull
    private final Set<FinalizableReference> references = newConcurrentHashSet();

    public FileStoreWrapper(
            @Nonnull FileStoreProbe fileStoreProbe,
            @Nonnull TarProbe tarProbe,
            @Nonnull Function<IOMonitor, Closeable> ioMonitorRegistry
    ) {
        this.fileStoreProbe = checkNotNull(fileStoreProbe);
        this.tarProbe = checkNotNull(tarProbe);
        this.ioMonitorRegistry = checkNotNull(ioMonitorRegistry);
    }

    @Nonnull
    @Override
    public Iterable<Tar> tars() {
        return transform(tarProbe.tarFiles(), TarWrapper::new);
    }

    @Nonnull
    @Override
    public Optional<Segment> segment(@Nonnull UUID uuid) {
        return fileStoreProbe
            .readSegment(newSegmentId(uuid))
            .map(SegmentWrapper::new);
    }

    @Nonnull
    @Override
    public Iterable<JournalEntry> journalEntries() {
        return () -> {
            try {
                JournalReader reader = fileStoreProbe.newJournalReader();
                Iterator<JournalEntry> entries = transform(reader, JournalEntryWrapper::new);

                references.add(new FinalizablePhantomReference<Iterator<?>>(entries, referenceQueue) {
                    @Override
                        public void finalizeReferent() {
                        try {
                            references.remove(this);
                            reader.close();
                        } catch (IOException e) {
                            throw new IllegalStateException("Error closing reader", e);
                        }
                    }
                });

                return entries;
            } catch (IOException e) {
                throw new IllegalStateException("Cannot access journal entries", e);
            }
        };
    }

    @Nonnull
    @Override
    public Node node(@Nonnull RecordId recordId) {
        SegmentNodeState node = fileStoreProbe.getNode(newRecordId(recordId));
        try {
            node.getRecordId().getSegment();
            return new NodeWrapper(node);
        } catch (SegmentNotFoundException ignore) {
            return Node.NULL_NODE;
        }
    }

    @Nonnull
    private org.apache.jackrabbit.oak.segment.RecordId newRecordId(@Nonnull RecordId recordId) {
        return new org.apache.jackrabbit.oak.segment.RecordId(
                newSegmentId(recordId.getSegmentId()), recordId.getOffset());
    }

    @Nonnull
    private SegmentId newSegmentId(@Nonnull UUID uuid) {
        return fileStoreProbe.newSegmentId(
                uuid.getMostSignificantBits(),
                uuid.getLeastSignificantBits());
    }

    @Nonnull
    @Override
    public Closeable addIOMonitor(@Nonnull IOMonitor ioMonitor) {
        return ioMonitorRegistry.apply(ioMonitor);
    }

    @Nonnull
    private static <T> Optional<T> safeCast(@Nullable Object value, @Nonnull Class<T> classType) {
        return classType.isInstance(value)
            ? Optional.of(classType.cast(value))
            : Optional.empty();
    }

    @SuppressWarnings("unchecked")
    @CheckForNull
    private static <T> T coerce(@Nullable Object value) {
        return (T) value;
    }

    @Nonnull
    @Override
    public <T> Optional<T> cast(Object value, Class<T> classType) {
        if (classType.equals(NodeState.class)) {
            return safeCast(value, NodeWrapper.class)
                    .map(v -> coerce(v.getState()));
        } else if (classType.equals(PropertyState.class)) {
            return safeCast(value, PropertyWrapper.class)
                    .map(v -> coerce(v.getState()));
        } else if (classType.equals(SegmentId.class)) {
            return safeCast(value, UUID.class)
                    .map(v -> coerce(newSegmentId(v)));
        } else if (classType.equals(org.apache.jackrabbit.oak.segment.RecordId.class)) {
            return safeCast(value, RecordId.class)
                    .map(v -> coerce(newRecordId(v)));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void close() throws IOException {
        referenceQueue.close();
        references.forEach(FinalizableReference::finalizeReferent);
    }
}
