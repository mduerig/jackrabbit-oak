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

package org.apache.jackrabbit.oak.segment.azure.persistentcache;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * michid document
 */
public class CachingSegmentArchiveWriter implements SegmentArchiveWriter {

    private final DiskCache diskCache;
    private final SegmentArchiveWriter delegate;

    @Override
    public @NotNull void writeSegment(long msb, long lsb, @NotNull byte[] data, int offset,
                                      int size, int generation, int fullGeneration,
                                      boolean isCompacted) throws IOException {
        delegate.writeSegment(msb, lsb, data, offset, size, generation, fullGeneration, isCompacted);
        diskCache.writeSegment(msb, lsb, data, offset, size, generation, fullGeneration, isCompacted);
    }

    @Override
    public @Nullable ByteBuffer readSegment(long msb, long lsb) throws IOException {
        return null; // michid implement readSegment
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return false; // michid implement containsSegment
    }

    @Override
    public void writeGraph(@NotNull byte[] data) throws IOException {
        // michid implement writeGraph
    }

    @Override
    public void writeBinaryReferences(@NotNull byte[] data) throws IOException {
        // michid implement writeBinaryReferences
    }

    @Override
    public long getLength() {
        return 0; // michid implement getLength
    }

    @Override
    public int getEntryCount() {
        return 0; // michid implement getEntryCount
    }

    @Override
    public void close() throws IOException {
        // michid implement close
    }

    @Override
    public boolean isCreated() {
        return false; // michid implement isCreated
    }

    @Override
    public void flush() throws IOException {
        // michid implement flush
    }

    @Override
    public @NotNull String getName() {
        return null; // michid implement getName
    }
}
