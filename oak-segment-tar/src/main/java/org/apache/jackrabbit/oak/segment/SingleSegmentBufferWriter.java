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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.System.arraycopy;
import static org.apache.jackrabbit.oak.segment.Segment.GC_GENERATION_OFFSET;
import static org.apache.jackrabbit.oak.segment.Segment.HEADER_SIZE;
import static org.apache.jackrabbit.oak.segment.Segment.RECORD_SIZE;
import static org.apache.jackrabbit.oak.segment.Segment.SEGMENT_REFERENCE_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SingleSegmentBufferWriter {

    private static final Logger log = LoggerFactory.getLogger(SingleSegmentBufferWriter.class);

    private static final class Statistics {

        int segmentIdCount;

        int recordIdCount;

        int recordCount;

        int size;

        SegmentId id;

        @Override
        public String toString() {
            return "id=" + id +
                    ",size=" + size +
                    ",segmentIdCount=" + segmentIdCount +
                    ",recordIdCount=" + recordIdCount +
                    ",recordCount=" + recordCount;
        }
    }

    /**
     * Align an {@code address} on the given {@code boundary}
     *
     * @param address  address to align
     * @param boundary boundary to align to
     * @return {@code n = address + a} such that {@code n % boundary == 0} and
     * {@code 0 <= a < boundary}.
     */
    private static int align(int address, int boundary) {
        return (address + boundary - 1) & ~(boundary - 1);
    }

    private final MutableRecordNumbers recordNumbers = new MutableRecordNumbers();

    private final MutableSegmentReferences segmentReferences = new MutableSegmentReferences();

    private final SegmentIdProvider segmentIdProvider;

    private final SegmentStore segmentStore;

    private final Statistics statistics;

    private final SegmentId segmentId;

    private final boolean enableGenerationCheck;

    private final int generation;

    private byte[] buffer;

    /**
     * The number of bytes already written (or allocated). Counted from
     * the <em>end</em> of the buffer.
     */
    private int length;

    /**
     * Current write position within the buffer. Grows up when raw data
     * is written, but shifted downwards by the prepare methods.
     */
    private int position;

    /**
     * Mark this buffer as dirty. A dirty buffer needs to be flushed to disk
     * regularly to avoid data loss.
     */
    private boolean dirty;

    SingleSegmentBufferWriter(SegmentStore segmentStore, int generation, SegmentIdProvider segmentIdProvider, SegmentId segmentId, boolean enableGenerationCheck) {
        this.segmentStore = segmentStore;
        this.segmentId = segmentId;
        this.segmentIdProvider = segmentIdProvider;
        this.enableGenerationCheck = enableGenerationCheck;
        this.generation = generation;

        statistics = new Statistics();
        statistics.id = segmentId;

        buffer = new byte[Segment.MAX_SEGMENT_SIZE];
        buffer[0] = '0';
        buffer[1] = 'a';
        buffer[2] = 'K';
        buffer[3] = SegmentVersion.asByte(LATEST_VERSION);
        buffer[4] = 0; // reserved
        buffer[5] = 0; // refcount
        buffer[GC_GENERATION_OFFSET] = (byte) (generation >> 24);
        buffer[GC_GENERATION_OFFSET + 1] = (byte) (generation >> 16);
        buffer[GC_GENERATION_OFFSET + 2] = (byte) (generation >> 8);
        buffer[GC_GENERATION_OFFSET + 3] = (byte) generation;
        length = 0;
        position = buffer.length;
        dirty = false;
    }

    Segment newSegment(SegmentReader reader, String metaInfo) {
        // TODO this is extremely ugly, but there is no other way to do this at
        // the moment without exposing the internals of the
        // SingleSegmentBufferWriter - the buffer, the record numbers, the
        // segment references, etc.
        return new Segment(segmentId, reader, buffer, recordNumbers, segmentReferences, metaInfo, segmentIdProvider);
    }

    int readSegmentReference(UUID id) {
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();
        SegmentId sid = segmentIdProvider.newSegmentId(msb, lsb);
        return writeSegmentIdReference(sid);
    }

    ByteBuffer addRecord(int number, int type, int size, Set<UUID> references) {
        checkArgument(size > 0);

        int recordSize = align(size, 1 << Segment.RECORD_ALIGN_BITS);

        // First compute the header and segment sizes based on the assumption
        // that *all* identifiers stored in this record point to previously
        // unreferenced segments.

        int recordNumbersCount = recordNumbers.size() + 1;
        int referencedIdCount = segmentReferences.size() + (references != null ? references.size() : 0);
        int headerSize = HEADER_SIZE + referencedIdCount * SEGMENT_REFERENCE_SIZE + recordNumbersCount * RECORD_SIZE;
        int segmentSize = align(headerSize + recordSize + length, 16);

        // If the size estimate looks too big, recompute it with a more
        // accurate refCount value. We skip doing this when possible to
        // avoid the somewhat expensive list and set traversals.

        if (segmentSize > buffer.length) {

            // Collect the newly referenced segment ids

            Set<SegmentId> segmentIds = null;
            if (references != null) {
                for (UUID reference : references) {
                    long msb = reference.getMostSignificantBits();
                    long lsb = reference.getLeastSignificantBits();
                    SegmentId segmentId = segmentIdProvider.newSegmentId(msb, lsb);
                    if (segmentReferences.contains(segmentId)) {
                        continue;
                    }
                    if (segmentIds == null) {
                        segmentIds = newHashSet();
                    }
                    segmentIds.add(segmentId);
                }
            }

            // Adjust the estimation of the new referenced segment ID count.

            referencedIdCount = segmentReferences.size() + (segmentIds != null ? segmentIds.size() : 0);
            headerSize = HEADER_SIZE + referencedIdCount * SEGMENT_REFERENCE_SIZE + recordNumbersCount * RECORD_SIZE;
            segmentSize = align(headerSize + recordSize + length, 16);
        }

        if (segmentSize > buffer.length) {
            return null;
        }

        statistics.recordCount++;

        length += recordSize;
        position = buffer.length - length;
        checkState(position >= 0);
        recordNumbers.addRecord(number, type, position);
        ByteBuffer buffer = ByteBuffer.wrap(this.buffer, position, length);
        dirty = true;
        return buffer;
    }

    boolean flush() throws IOException {
        if (!dirty) {
            return false;
        }

        int referencedSegmentIdCount = segmentReferences.size();
        BinaryUtils.writeInt(buffer, Segment.REFERENCED_SEGMENT_ID_COUNT_OFFSET, referencedSegmentIdCount);

        statistics.segmentIdCount = referencedSegmentIdCount;

        int recordNumberCount = recordNumbers.size();
        BinaryUtils.writeInt(buffer, Segment.RECORD_NUMBER_COUNT_OFFSET, recordNumberCount);

        int totalLength = align(HEADER_SIZE + referencedSegmentIdCount * SEGMENT_REFERENCE_SIZE + recordNumberCount * RECORD_SIZE + length, 16);

        if (totalLength > buffer.length) {
            throw new IllegalStateException("too much data for a segment");
        }

        statistics.size = length = totalLength;

        int pos = HEADER_SIZE;
        if (pos + length <= buffer.length) {
            // the whole segment fits to the space *after* the referenced
            // segment identifiers we've already written, so we can safely
            // copy those bits ahead even if concurrent code is still
            // reading from that part of the buffer
            arraycopy(buffer, 0, buffer, buffer.length - length, pos);
            pos += buffer.length - length;
        } else {
            // this might leave some empty space between the header and
            // the record data, but this case only occurs when the
            // segment is >252kB in size and the maximum overhead is <<4kB,
            // which is acceptable
            length = buffer.length;
        }

        for (SegmentId sid : segmentReferences) {
            pos = BinaryUtils.writeLong(buffer, pos, sid.getMostSignificantBits());
            pos = BinaryUtils.writeLong(buffer, pos, sid.getLeastSignificantBits());
        }

        for (Entry entry : recordNumbers) {
            pos = BinaryUtils.writeInt(buffer, pos, entry.getRecordNumber());
            pos = BinaryUtils.writeByte(buffer, pos, (byte) entry.getType().ordinal());
            pos = BinaryUtils.writeInt(buffer, pos, entry.getOffset());
        }

        log.debug("Writing data segment: {} ", statistics);
        segmentStore.writeSegment(segmentId, buffer, buffer.length - length, length);
        return true;
    }

    void writeByte(byte value) {
        position = BinaryUtils.writeByte(buffer, position, value);
    }

    void writeShort(short value) {
        position = BinaryUtils.writeShort(buffer, position, value);
    }

    public void writeInt(int value) {
        position = BinaryUtils.writeInt(buffer, position, value);
    }

    public void writeLong(long value) {
        position = BinaryUtils.writeLong(buffer, position, value);
    }

    public void writeBytes(byte[] data, int offset, int length) {
        arraycopy(data, offset, buffer, position, length);
        position += length;
    }

    void writeRecordId(RecordId recordId) {
        writeRecordId(recordId, true);
    }

    void writeRecordId(RecordId recordId, boolean reference) {
        checkNotNull(recordId);
        checkState(segmentReferences.size() + 1 < 0xffff, "Segment cannot have more than 0xffff references");
        checkGCGeneration(recordId.getSegmentId());

        writeShort(toShort(writeSegmentIdReference(recordId.getSegmentId())));
        writeInt(recordId.getRecordNumber());

        statistics.recordIdCount++;
    }

    private static short toShort(int value) {
        return (short) value;
    }

    private int writeSegmentIdReference(SegmentId id) {
        if (id.equals(segmentId)) {
            return 0;
        }
        return segmentReferences.addOrReference(id);
    }

    private void checkGCGeneration(SegmentId id) {
        if (enableGenerationCheck) {
            try {
                if (isDataSegmentId(id.getLeastSignificantBits())) {
                    if (id.getGcGeneration() < generation) {
                        log.warn("Detected reference from {} to segment {} from a previous gc generation.", info(segmentId), info(id), new Exception());
                    }
                }
            } catch (SegmentNotFoundException snfe) {
                log.warn("Detected reference from {} to non existing segment {}", info(segmentId), id, snfe);
            }
        }
    }

    private static String info(SegmentId segmentId) {
        String info = segmentId.toString();
        if (isDataSegmentId(segmentId.getLeastSignificantBits())) {
            info += " " + segmentId.getSegment().getSegmentInfo();
        }
        return info;
    }

}
