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

package org.apache.jackrabbit.oak.segment.io.raw;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_DELTA;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_DELTA;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LIMIT;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

public final class RawRecordWriter {

    public interface RecordAdder {

        ByteBuffer addRecord(int number, int type, int requestedSize, Set<UUID> references);

    }

    public interface SegmentReferenceReader {

        int readSegmentReference(UUID segmentId);

    }

    public static RawRecordWriter of(SegmentReferenceReader segmentReferenceReader, RecordAdder recordAdder) {
        return new RawRecordWriter(checkNotNull(segmentReferenceReader), checkNotNull(recordAdder));
    }

    private static int lengthSize(int length) {
        if (length < SMALL_LIMIT) {
            return SMALL_LENGTH_SIZE;
        }
        if (length < MEDIUM_LIMIT) {
            return MEDIUM_LENGTH_SIZE;
        }
        throw new IllegalArgumentException("invalid length: " + length);
    }

    private static ByteBuffer writeLength(ByteBuffer buffer, int length) {
        if (length < SMALL_LIMIT) {
            return buffer.put((byte) length);
        }
        if (length < MEDIUM_LIMIT) {
            return buffer.putShort((short) (((length - MEDIUM_LENGTH_DELTA) & MEDIUM_LENGTH_MASK) | MEDIUM_LENGTH_MARKER));
        }
        throw new IllegalArgumentException("invalid length: " + length);
    }

    private static ByteBuffer writeLength(ByteBuffer buffer, long length) {
        return buffer.putLong((((length - LONG_LENGTH_DELTA) & LONG_LENGTH_MASK) | LONG_LENGTH_MARKER));
    }

    private static ByteBuffer writeRecordId(ByteBuffer buffer, int segmentReference, int recordNumber) {
        return buffer.putShort((short) segmentReference).putInt(recordNumber);
    }

    private final SegmentReferenceReader segmentReferenceReader;

    private final RecordAdder recordAdder;

    private RawRecordWriter(SegmentReferenceReader segmentReferenceReader, RecordAdder recordAdder) {
        this.segmentReferenceReader = segmentReferenceReader;
        this.recordAdder = recordAdder;
    }

    private ByteBuffer addRecord(int number, int type, int requestedSize, Set<UUID> references) {
        return recordAdder.addRecord(number, type, requestedSize, references);
    }

    private int readSegmentReference(UUID segmentId) {
        return segmentReferenceReader.readSegmentReference(segmentId);
    }

    public boolean writeValue(int number, int type, byte[] data) {
        ByteBuffer buffer = addRecord(number, type, data.length + lengthSize(data.length), null);
        if (buffer == null) {
            return false;
        }
        writeLength(buffer, data.length).put(data);
        return true;
    }

    public boolean writeValue(int number, int type, UUID segmentId, int recordNumber, long length) {
        ByteBuffer buffer = addRecord(number, type, LONG_LENGTH_SIZE + RawRecordId.BYTES, singleton(segmentId));
        if (buffer == null) {
            return false;
        }
        writeRecordId(writeLength(buffer, length), readSegmentReference(segmentId), recordNumber);
        return true;
    }

}
