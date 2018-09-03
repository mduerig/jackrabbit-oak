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
import java.util.List;

import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * michid document
 */
public class CachingSegmentArchiveReader implements SegmentArchiveReader {

    private final DiskCache diskCache;
    private final SegmentArchiveReader delegate;

    @Override
    public @Nullable ByteBuffer readSegment(long msb, long lsb) throws IOException {
        ByteBuffer buffer = diskCache.readSegment(msb, lsb);
        if (buffer == null) {
            buffer = delegate.readSegment(msb, lsb);
        }
        return buffer;
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        if (diskCache.containsSegment(msb, lsb)) {
            return true;
        } else {
            return delegate.containsSegment(msb, lsb);
        }
    }

    @Override
    public List<SegmentArchiveEntry> listSegments() {
        return null; // michid implement listSegments
    }

    @Override
    public @Nullable ByteBuffer getGraph() throws IOException {
        return null; // michid implement getGraph
    }

    @Override
    public boolean hasGraph() {
        return false; // michid implement hasGraph
    }

    @Override
    public @NotNull ByteBuffer getBinaryReferences() throws IOException {
        return null; // michid implement getBinaryReferences
    }

    @Override
    public long length() {
        return 0; // michid implement length
    }

    @Override
    public @NotNull String getName() {
        return null; // michid implement getName
    }

    @Override
    public void close() throws IOException {
        // michid implement close
    }

    @Override
    public int getEntrySize(int size) {
        return 0; // michid implement getEntrySize
    }
}
