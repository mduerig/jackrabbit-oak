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

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Predicate;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TarFiles implements Closeable {

    public static class CleanupResult {

        private boolean interrupted;

        private long reclaimedSize;

        private List<File> removeableFiles;

        private Set<UUID> reclaimedSegmentIds;

        private CleanupResult() {
            // Prevent external instantiation.
        }

        public long getReclaimedSize() {
            return reclaimedSize;
        }

        public List<File> getRemoveableFiles() {
            return removeableFiles;
        }

        public Set<UUID> getReclaimedSegmentIds() {
            return reclaimedSegmentIds;
        }

        public boolean isInterrupted() {
            return interrupted;
        }

    }

    public static class Builder {

        private File directory;

        private boolean memoryMapping;

        private TarRecovery tarRecovery;

        private IOMonitor ioMonitor;

        private FileStoreStats fileStoreStats;

        private long maxFileSize;

        public Builder withDirectory(File directory) {
            this.directory = checkNotNull(directory);
            return this;
        }

        public Builder withMemoryMapping(boolean memoryMapping) {
            this.memoryMapping = memoryMapping;
            return this;
        }

        public Builder withTarRecovery(TarRecovery tarRecovery) {
            this.tarRecovery = checkNotNull(tarRecovery);
            return this;
        }

        public Builder withIOMonitor(IOMonitor ioMonitor) {
            this.ioMonitor = checkNotNull(ioMonitor);
            return this;
        }

        public Builder withFileStoreStats(FileStoreStats fileStoreStats) {
            this.fileStoreStats = checkNotNull(fileStoreStats);
            return this;
        }

        public Builder withMaxFileSize(long maxFileSize) {
            checkArgument(maxFileSize > 0);
            this.maxFileSize = maxFileSize;
            return this;
        }

        public TarFiles build() throws IOException {
            checkState(directory != null, "Directory not specified");
            checkState(tarRecovery != null, "TAR recovery strategy not specified");
            checkState(ioMonitor != null, "I/O monitor not specified");
            checkState(fileStoreStats != null, "File store statistics not specified");
            checkState(maxFileSize != 0, "Max file size not specified");
            return new TarFiles(this);
        }

    }

    private static final Logger log = LoggerFactory.getLogger(TarFiles.class);

    public static Builder builder() {
        return new Builder();
    }

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final long maxFileSize;

    private final boolean memoryMapping;

    private final IOMonitor ioMonitor;

    private List<TarReader> readers;

    private TarWriter writer;

    private volatile boolean shutdown;

    private boolean closed;

    private TarFiles(Builder builder) throws IOException {
        maxFileSize = builder.maxFileSize;
        memoryMapping = builder.memoryMapping;
        ioMonitor = builder.ioMonitor;
        Map<Integer, Map<Character, File>> map = AbstractFileStore.collectFiles(builder.directory);
        readers = newArrayListWithCapacity(map.size());
        Integer[] indices = map.keySet().toArray(new Integer[map.size()]);
        Arrays.sort(indices);
        for (int i = indices.length - 1; i >= 0; i--) {
            readers.add(TarReader.open(map.get(indices[i]), builder.memoryMapping, builder.tarRecovery, builder.ioMonitor));
        }
        int writeNumber = 0;
        if (indices.length > 0) {
            writeNumber = indices[indices.length - 1] + 1;
        }
        writer = new TarWriter(builder.directory, builder.fileStoreStats, writeNumber, builder.ioMonitor);
    }

    private void checkOpen() {
        checkState(!closed, "This instance has been closed");
    }

    @Override
    public void close() throws IOException {
        shutdown = true;

        List<TarReader> readers;
        TarWriter writer;

        lock.writeLock().lock();
        try {
            checkState(!closed);
            closed = true;
            readers = this.readers;
            this.readers = null;
            writer = this.writer;
            this.writer = null;
        } finally {
            lock.writeLock().unlock();
        }

        Closer closer = Closer.create();
        for (TarReader reader : readers) {
            closer.register(reader);
        }
        closer.register(writer);
        closer.close();
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return "TarFiles{readers=" + readers + ", writer=" + writer + "}";
        } finally {
            lock.readLock().unlock();
        }
    }

    public long size() {
        lock.readLock().lock();
        try {
            checkOpen();
            long size = writer.fileLength();
            for (TarReader reader : readers) {
                size += reader.size();
            }
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int readerCount() {
        lock.readLock().lock();
        try {
            checkOpen();
            return readers.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void flush() throws IOException {
        lock.readLock().lock();
        try {
            checkOpen();
            writer.flush();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean containsSegment(long msb, long lsb) {
        lock.readLock().lock();
        try {
            checkOpen();
            if (writer.containsEntry(msb, lsb)) {
                return true;
            }
            for (TarReader reader : readers) {
                if (reader.containsEntry(msb, lsb)) {
                    return true;
                }
            }
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    public ByteBuffer readSegment(long msb, long lsb) {
        lock.readLock().lock();
        try {
            checkOpen();
            try {
                ByteBuffer buffer = writer.readEntry(msb, lsb);
                if (buffer != null) {
                    return buffer;
                }
                for (TarReader reader : readers) {
                    buffer = reader.readEntry(msb, lsb);
                    if (buffer != null) {
                        return buffer;
                    }
                }
            } catch (IOException e) {
                log.warn("Unable to read from TAR file {}", writer, e);
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void writeSegment(UUID id, byte[] buffer, int offset, int length, int generation, Set<UUID> references, Set<String> binaryReferences) throws IOException {
        lock.writeLock().lock();
        try {
            checkOpen();
            long size = writer.writeEntry(
                    id.getMostSignificantBits(),
                    id.getLeastSignificantBits(),
                    buffer,
                    offset,
                    length,
                    generation
            );

            if (references != null) {
                for (UUID reference : references) {
                    writer.addGraphEdge(id, reference);
                }
            }

            if (binaryReferences != null) {
                for (String reference : binaryReferences) {
                    writer.addBinaryReference(generation, id, reference);
                }
            }

            if (size >= maxFileSize) {
                newWriter();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void newWriter() throws IOException {
        TarWriter newWriter = writer.createNextGeneration();
        if (newWriter == writer) {
            return;
        }
        File writeFile = writer.getFile();
        List<TarReader> list = newArrayListWithCapacity(1 + readers.size());
        list.add(TarReader.open(writeFile, memoryMapping, ioMonitor));
        list.addAll(readers);
        readers = list;
        writer = newWriter;
    }

    public CleanupResult cleanup(Set<UUID> references, Predicate<Integer> reclaimGeneration) throws IOException {
        CleanupResult result = new CleanupResult();
        result.removeableFiles = new ArrayList<>();
        result.reclaimedSegmentIds = new HashSet<>();

        Map<TarReader, TarReader> cleaned = newLinkedHashMap();
        lock.writeLock().lock();
        try {
            checkState(!closed);
            newWriter();
            for (TarReader reader : readers) {
                cleaned.put(reader, reader);
                result.reclaimedSize += reader.size();
            }
        } finally {
            lock.writeLock().unlock();
        }

        lock.readLock().lock();
        try {
            checkState(!closed);
            Set<UUID> reclaim = newHashSet();
            for (TarReader reader : cleaned.keySet()) {
                if (shutdown) {
                    result.interrupted = true;
                    return result;
                }
                reader.mark(references, reclaim, reclaimGeneration);
                log.info("{}: size of bulk references/reclaim set {}/{}", reader, references.size(), reclaim.size());
            }
            for (TarReader reader : cleaned.keySet()) {
                if (shutdown) {
                    result.interrupted = true;
                    return result;
                }
                cleaned.put(reader, reader.sweep(reclaim, result.reclaimedSegmentIds));
            }
        } finally {
            lock.readLock().unlock();
        }

        List<TarReader> oldReaders = newArrayList();
        lock.writeLock().lock();
        try {
            // Replace current list of reader with the cleaned readers taking care not to lose
            // any new reader that might have come in through concurrent calls to newWriter()
            checkOpen();
            List<TarReader> sweptReaders = newArrayList();
            for (TarReader reader : readers) {
                if (cleaned.containsKey(reader)) {
                    TarReader newReader = cleaned.get(reader);
                    if (newReader != null) {
                        sweptReaders.add(newReader);
                        result.reclaimedSize -= newReader.size();
                    }
                    // if these two differ, the former represents the swept version of the latter
                    if (newReader != reader) {
                        oldReaders.add(reader);
                    }
                } else {
                    sweptReaders.add(reader);
                }
            }
            readers = sweptReaders;
        } finally {
            lock.writeLock().unlock();
        }

        for (TarReader oldReader : oldReaders) {
            try {
                oldReader.close();
            } catch (IOException e) {
                log.error("Unable to close swept TAR reader", e);
            }
            result.removeableFiles.add(oldReader.getFile());
        }

        return result;
    }

    public void collectBlobReferences(ReferenceCollector collector, int minGeneration) throws IOException {
        List<TarReader> tarReaders = newArrayList();
        lock.writeLock().lock();
        try {
            checkState(!closed);
            newWriter();
            tarReaders.addAll(this.readers);
        } finally {
            lock.writeLock().unlock();
        }
        for (TarReader tarReader : tarReaders) {
            tarReader.collectBlobReferences(collector, minGeneration);
        }
    }

}
