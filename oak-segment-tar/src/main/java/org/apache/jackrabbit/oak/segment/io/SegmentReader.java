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

package org.apache.jackrabbit.oak.segment.io;

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.segment.io.Constants.GENERATION_OFFSET;
import static org.apache.jackrabbit.oak.segment.io.Constants.HEADER_SIZE;
import static org.apache.jackrabbit.oak.segment.io.Constants.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.segment.io.Constants.RECORDS_COUNT_OFFSET;
import static org.apache.jackrabbit.oak.segment.io.Constants.RECORD_REFERENCE_SIZE;
import static org.apache.jackrabbit.oak.segment.io.Constants.SEGMENT_REFERENCES_COUNT_OFFSET;
import static org.apache.jackrabbit.oak.segment.io.Constants.SEGMENT_REFERENCE_SIZE;
import static org.apache.jackrabbit.oak.segment.io.Constants.VERSION_OFFSET;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Enables read-only access to a segment.
 */
public class SegmentReader implements SegmentAccess {

    /**
     * Creates a {@link SegmentReader} for the provided underlying buffer.
     *
     * @param id     The identifier of this segment.
     * @param buffer An instance of {@link ByteBuffer} containing valid segment
     *               data.
     * @return An instance of {@link SegmentReader}.
     */
    public static SegmentReader of(UUID id, ByteBuffer buffer) {
        checkNotNull(id);
        checkNotNull(buffer);

        if (buffer.limit() < HEADER_SIZE) {
            throw new IllegalArgumentException("Segment too small");
        }

        if (buffer.get(0) != '0' || buffer.get(1) != 'a' || buffer.get(2) != 'K') {
            throw new IllegalArgumentException("Invalid magic number");
        }

        return new SegmentReader(id, buffer);
    }

    /**
     * Creates a {@link SegmentWriter} from the latest state of the provided
     * {@link SegmentWriter}.
     *
     * @param writer An instance of {@link SegmentWriter}.
     * @return An instance of {@link SegmentReader}.
     */
    public static SegmentReader of(SegmentWriter writer) {
        return SegmentReader.of(writer.id(), writer.writeTo(ByteBuffer.allocate(writer.size())));
    }

    private final UUID id;

    private final ByteBuffer buffer;

    private SegmentReader(UUID id, ByteBuffer buffer) {
        this.id = id;
        this.buffer = buffer;
    }

    @Override
    public UUID id() {
        return id;
    }

    @Override
    public int version() {
        return buffer.get(VERSION_OFFSET);
    }

    @Override
    public int generation() {
        return buffer.getInt(GENERATION_OFFSET);
    }

    @Override
    public int segmentReferenceCount() {
        return buffer.getInt(SEGMENT_REFERENCES_COUNT_OFFSET);
    }

    @Override
    public UUID segmentReference(int i) {
        checkElementIndex(i, segmentReferenceCount());

        int idx = HEADER_SIZE + i * SEGMENT_REFERENCE_SIZE;

        long msb = buffer.getLong(idx);
        idx += Long.BYTES;

        long lsb = buffer.getLong(idx);

        return new UUID(msb, lsb);
    }

    @Override
    public int recordCount() {
        return buffer.getInt(RECORDS_COUNT_OFFSET);
    }

    @Override
    public Record recordEntry(int i) {
        checkElementIndex(i, recordCount());

        int idx = HEADER_SIZE + segmentReferenceCount() * SEGMENT_REFERENCE_SIZE + i * RECORD_REFERENCE_SIZE;

        int number = buffer.getInt(idx);
        idx += Integer.BYTES;

        int type = buffer.get(idx);
        idx += Byte.BYTES;

        int offset = buffer.getInt(idx);

        return new Record(number, type, offset);
    }

    @Override
    public ByteBuffer recordValue(int number, int size) {
        int base = offsetByNumber(number);
        if (base < 0) {
            return null;
        }
        checkPositionIndexes(base, base + size, MAX_SEGMENT_SIZE);
        int pos = buffer.limit() - MAX_SEGMENT_SIZE + base;
        checkState(pos >= 0);
        ByteBuffer slice = buffer.slice();
        slice.position(pos);
        slice.limit(pos + size);
        return slice.slice();
    }

    private int offsetByNumber(int number) {
        int start = 0, end = recordCount() - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            int midNumber = number(mid);
            if (midNumber < number) {
                start = mid + 1;
            } else if (midNumber > number) {
                end = mid - 1;
            } else {
                return offset(mid);
            }
        }
        return -1;
    }

    private int number(int i) {
        int idx = buffer.position();
        idx += HEADER_SIZE;
        idx += segmentReferenceCount() * SEGMENT_REFERENCE_SIZE;
        idx += i * RECORD_REFERENCE_SIZE;
        return buffer.getInt(idx);
    }

    private int offset(int i) {
        int idx = buffer.position();
        idx += HEADER_SIZE;
        idx += segmentReferenceCount() * SEGMENT_REFERENCE_SIZE;
        idx += i * RECORD_REFERENCE_SIZE;
        idx += Integer.BYTES;
        idx += Byte.BYTES;
        return buffer.getInt(idx);
    }

}
