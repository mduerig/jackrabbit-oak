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
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_BLOB_ID_LENGTH;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_BLOB_ID_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_DELTA;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_DELTA;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_BLOB_ID_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_BLOB_ID_LENGTH_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_BLOB_ID_LENGTH_SIZE;
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

    private final SegmentReferenceReader segmentReferenceReader;

    private final RecordAdder recordAdder;

    private RawRecordWriter(SegmentReferenceReader segmentReferenceReader, RecordAdder recordAdder) {
        this.segmentReferenceReader = segmentReferenceReader;
        this.recordAdder = recordAdder;
    }

    private ByteBuffer addRecord(int number, int type, int requestedSize, Set<UUID> references) {
        return recordAdder.addRecord(number, type, requestedSize, references);
    }

    private short segmentReference(UUID segmentId) {
        return (short) segmentReferenceReader.readSegmentReference(segmentId);
    }

    public boolean writeValue(int number, int type, byte[] data, int offset, int length) {
        if (length < SMALL_LIMIT) {
            return writeSmallValue(number, type, data, offset, length);
        }
        if (length < MEDIUM_LIMIT) {
            return writeMediumValue(number, type, data, offset, length);
        }
        throw new IllegalArgumentException("invalid length: " + length);
    }

    private boolean writeSmallValue(int number, int type, byte[] data, int offset, int length) {
        ByteBuffer buffer = addRecord(number, type, length + SMALL_LENGTH_SIZE, null);
        if (buffer == null) {
            return false;
        }
        buffer.put((byte) length).put(data, offset, length);
        return true;
    }

    private boolean writeMediumValue(int number, int type, byte[] data, int offset, int length) {
        ByteBuffer buffer = addRecord(number, type, length + MEDIUM_LENGTH_SIZE, null);
        if (buffer == null) {
            return false;
        }
        buffer.putShort(mediumLength(length)).put(data, offset, length);
        return true;
    }

    private static short mediumLength(int length) {
        return (short) (((length - MEDIUM_LENGTH_DELTA) & MEDIUM_LENGTH_MASK) | MEDIUM_LENGTH_MARKER);
    }

    public boolean writeValue(int number, int type, RawRecordId id, long length) {
        ByteBuffer buffer = addRecord(number, type, LONG_LENGTH_SIZE + RawRecordId.BYTES, singleton(id.getSegmentId()));
        if (buffer == null) {
            return false;
        }
        buffer.putLong(longLength(length))
                .putShort(segmentReference(id.getSegmentId()))
                .putInt(id.getRecordNumber());
        return true;
    }

    private static long longLength(long length) {
        return ((length - LONG_LENGTH_DELTA) & LONG_LENGTH_MASK) | LONG_LENGTH_MARKER;
    }

    public boolean writeBlobId(int number, int type, byte[] blobId) {
        ByteBuffer buffer = addRecord(number, type, SMALL_BLOB_ID_LENGTH_SIZE + blobId.length, null);
        if (buffer == null) {
            return false;
        }
        buffer.putShort(smallBlobIdLength(blobId.length)).put(blobId);
        return true;
    }

    private static short smallBlobIdLength(int length) {
        return (short) ((length & SMALL_BLOB_ID_LENGTH_MASK) | SMALL_BLOB_ID_LENGTH_MARKER);
    }

    public boolean writeBlobId(int number, int type, RawRecordId id) {
        ByteBuffer buffer = addRecord(number, type, LONG_BLOB_ID_LENGTH_SIZE + RawRecordId.BYTES, null);
        if (buffer == null) {
            return false;
        }
        buffer.put(LONG_BLOB_ID_LENGTH)
                .putShort(segmentReference(id.getSegmentId()))
                .putInt(id.getRecordNumber());
        return true;
    }

    public boolean writeBlock(int number, int type, byte[] data, int offset, int length) {
        ByteBuffer buffer = addRecord(number, type, length, null);
        if (buffer == null) {
            return false;
        }
        buffer.put(data, offset, length);
        return true;
    }

}
