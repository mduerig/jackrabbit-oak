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

import static org.apache.jackrabbit.oak.segment.io.BinaryUtils.align;
import static org.apache.jackrabbit.oak.segment.io.Constants.GENERATION_OFFSET;
import static org.apache.jackrabbit.oak.segment.io.Constants.HEADER_SIZE;
import static org.apache.jackrabbit.oak.segment.io.Constants.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.segment.io.Constants.RECORDS_COUNT_OFFSET;
import static org.apache.jackrabbit.oak.segment.io.Constants.RECORD_REFERENCE_SIZE;
import static org.apache.jackrabbit.oak.segment.io.Constants.SEGMENT_REFERENCES_COUNT_OFFSET;
import static org.apache.jackrabbit.oak.segment.io.Constants.SEGMENT_REFERENCE_SIZE;
import static org.apache.jackrabbit.oak.segment.io.Constants.VERSION_OFFSET;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Builds a segment incrementally in memory and serializes it.
 */
public class SegmentWriter implements SegmentAccess {

    /**
     * Create a new {@link SegmentWriter} for the given version and generation.
     *
     * @param id         Identifier of this segment.
     * @param version    Version of the segment.
     * @param generation Generation of the segment.
     * @return An instance of {@link SegmentWriter}.
     */
    public static SegmentWriter of(UUID id, int version, int generation) {
        return new SegmentWriter(id, version, generation);
    }

    private final UUID id;

    private final int version;

    private final int generation;

    private final Object lock = new Object();

    private final Set<UUID> references = new HashSet<>();

    private final List<UUID> orderedReferences = new ArrayList<>();

    private final Map<UUID, Integer> referenceIndexes = new HashMap<>();

    private final ByteBuffer values = ByteBuffer.allocate(MAX_SEGMENT_SIZE);

    private final Map<Integer, Record> entries = new TreeMap<>();

    private final List<Record> orderedEntries = new ArrayList<>();

    private int size = HEADER_SIZE;

    private int offset;

    private SegmentWriter(UUID id, int version, int generation) {
        this.id = id;
        this.version = version;
        this.generation = generation;
    }

    @Override
    public UUID id() {
        return id;
    }

    @Override
    public int version() {
        return version;
    }

    @Override
    public int generation() {
        return generation;
    }

    @Override
    public int segmentReferenceCount() {
        synchronized (lock) {
            return orderedReferences.size();
        }
    }

    @Override
    public UUID segmentReference(int i) {
        synchronized (lock) {
            return orderedReferences.get(i);
        }
    }

    /**
     * Return the index of a segment reference in this segment.
     *
     * @param id The ID of the referenced segment.
     * @return A positive integer representing the index of the segment
     * reference if the segment reference was found, {@code -1} otherwise.
     */
    public int segmentReferenceIndex(UUID id) {
        synchronized (lock) {
            return referenceIndexes.getOrDefault(id, -1);
        }
    }

    @Override
    public int recordCount() {
        synchronized (lock) {
            return orderedEntries.size();
        }
    }

    @Override
    public Record recordEntry(int i) {
        synchronized (lock) {
            return orderedEntries.get(i);
        }
    }

    @Override
    public ByteBuffer recordValue(int number, int size) {
        synchronized (this) {
            Record entry = entries.get(number);
            if (entry == null) {
                return null;
            }
            ByteBuffer slice = values.duplicate();
            slice.position(entry.offset());
            slice.limit(entry.offset() + size);
            return slice.slice();
        }
    }

    /**
     * Make space for a record to this segment. The record might reference
     * records from other segments. In this case, those references must be
     * specified to this method.
     *
     * @param number        The record number of this record.
     * @param type          The type of this record.
     * @param requestedSize The space needed this record.
     * @param rs            References to other segments from this record. It
     *                      can be {@code null} if the record doesn't reference
     *                      any other segment.
     * @return an instance of {@link ByteBuffer} if space for the record can be
     * reserved in the segment, {@code null} otherwise. If this method returns
     * {@code null}, the segment has reached its maximum size.
     * @throws IllegalArgumentException if a record with the given number
     *                                  already exists in this segment.
     */
    public ByteBuffer addRecord(int number, int type, int requestedSize, Set<UUID> rs) {
        synchronized (lock) {
            if (entries.containsKey(number)) {
                throw new IllegalArgumentException("record number already exists");
            }

            int recordSize = alignRecordSize(requestedSize);
            int newSize = computeSize(recordSize, rs);

            if (alignSegmentSize(newSize) > MAX_SEGMENT_SIZE) {
                return null;
            }

            size = newSize;
            offset += recordSize;

            int pos = MAX_SEGMENT_SIZE - offset;

            Record entry = new Record(number, type, pos);
            entries.put(number, entry);
            orderedEntries.add(entry);

            if (rs != null) {
                for (UUID r : rs) {
                    if (references.add(r)) {
                        referenceIndexes.put(r, orderedReferences.size());
                        orderedReferences.add(r);
                    }
                }
            }

            values.position(pos);

            ByteBuffer value = values.slice();
            value.limit(requestedSize);
            return value.slice();
        }
    }

    /**
     * The size of this segment, once serialized.
     *
     * @return A positive integer.
     */
    public int size() {
        synchronized (lock) {
            return alignSegmentSize(size);
        }
    }

    /**
     * Serializes this segment to the provided buffer. The buffer must be big
     * enough to contain the serialized segment (see {@link #size()}).
     *
     * @param buffer An instance of {@link ByteBuffer}.
     * @return The same instance of {@link ByteBuffer} passed as input.
     */
    public ByteBuffer writeTo(ByteBuffer buffer) {
        synchronized (lock) {
            ByteBuffer out = buffer.slice();

            if (out.remaining() < size) {
                throw new IllegalArgumentException("buffer too small");
            }

            out.put((byte) '0');
            out.put((byte) 'a');
            out.put((byte) 'K');

            out.put(VERSION_OFFSET, (byte) version);
            out.putInt(GENERATION_OFFSET, generation);
            out.putInt(SEGMENT_REFERENCES_COUNT_OFFSET, references.size());
            out.putInt(RECORDS_COUNT_OFFSET, entries.size());

            out.position(HEADER_SIZE);

            for (UUID reference : references) {
                out.putLong(reference.getMostSignificantBits());
                out.putLong(reference.getLeastSignificantBits());
            }

            for (Record record : entries.values()) {
                out.putInt(record.number());
                out.put((byte) record.type());
                out.putInt(record.offset());
            }

            // We have position the input buffer to the start of the record
            // values for the copy to the output buffer to work properly. We
            // have to explicitly position the output buffer because we have to
            // take into account that between the end of the header and the
            // beginning of the record values there might be padding bytes due
            // to the segment alignment.
            values.position(MAX_SEGMENT_SIZE - offset);
            out.position(out.limit() - offset);
            out.put(values);

            return buffer;
        }
    }

    private int computeSize(int recordSize, Set<UUID> uuids) {
        int result = size;

        // A new record entry must be added in the header.
        result += RECORD_REFERENCE_SIZE;

        // The record value must be added.
        result += recordSize;

        // One segment references must be added for every new segment reference.
        if (uuids != null) {
            for (UUID uuid : uuids) {
                if (!references.contains(uuid)) {
                    result += SEGMENT_REFERENCE_SIZE;
                }
            }
        }

        return result;
    }

    private static int alignRecordSize(int recordSize) {
        return align(recordSize, 4);
    }

    private static int alignSegmentSize(int segmentSize) {
        return align(segmentSize, 16);
    }

}
