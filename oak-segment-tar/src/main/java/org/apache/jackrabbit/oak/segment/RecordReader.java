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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.segment.Segment.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.segment.Segment.RECORD_ID_BYTES;
import static org.apache.jackrabbit.oak.segment.Segment.SMALL_LIMIT;
import static org.apache.jackrabbit.oak.segment.SegmentWriter.BLOCK_SIZE;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

/**
 * Implements logic for reading records from a segment.
 */
class RecordReader {

    private static int asUnsigned(short value) {
        return value & 0xffff;
    }

    private final SegmentId id;

    private final ByteBuffer data;

    private final SegmentReader reader;

    private final RecordNumbers recordNumbers;

    private final SegmentReferences segmentReferences;

    RecordReader(SegmentId id, ByteBuffer data, SegmentReader reader, RecordNumbers recordNumbers, SegmentReferences segmentReferences) {
        this.id = id;
        this.data = data;
        this.reader = reader;
        this.recordNumbers = recordNumbers;
        this.segmentReferences = segmentReferences;
    }

    byte readByte(int recordNumber) {
        return readByte(recordNumber, 0);
    }

    byte readByte(int recordNumber, int offset) {
        return data.get(pos(recordNumber, offset, 1));
    }

    short readShort(int recordNumber) {
        return data.getShort(pos(recordNumber, 2));
    }

    int readInt(int recordNumber) {
        return data.getInt(pos(recordNumber, 4));
    }

    int readInt(int recordNumber, int offset) {
        return data.getInt(pos(recordNumber, offset, 4));
    }

    long readLong(int recordNumber) {
        return data.getLong(pos(recordNumber, 8));
    }

    void readBytes(int recordNumber, int position, byte[] buffer, int offset, int length) {
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);
        ByteBuffer d = readBytes(recordNumber, position, length);
        d.get(buffer, offset, length);
    }

    ByteBuffer readBytes(int recordNumber, int position, int length) {
        int pos = pos(recordNumber, position, length);
        return slice(pos, length);
    }

    RecordId readRecordId(int recordNumber, int rawOffset, int recordIdOffset) {
        return internalReadRecordId(pos(recordNumber, rawOffset, recordIdOffset, RECORD_ID_BYTES));
    }

    RecordId readRecordId(int recordNumber, int rawOffset) {
        return readRecordId(recordNumber, rawOffset, 0);
    }

    RecordId readRecordId(int recordNumber) {
        return readRecordId(recordNumber, 0, 0);
    }

    String readString(int offset) {
        int pos = pos(offset, 1);
        long length = internalReadLength(pos);
        if (length < SMALL_LIMIT) {
            return Charsets.UTF_8.decode(slice(pos + 1, (int) length)).toString();
        } else if (length < MEDIUM_LIMIT) {
            return Charsets.UTF_8.decode(slice(pos + 2, (int) length)).toString();
        } else if (length < Integer.MAX_VALUE) {
            int size = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(internalReadRecordId(pos + 8), size);
            try (SegmentStream stream = new SegmentStream(new RecordId(id, offset), list, length)) {
                return stream.getString();
            }
        } else {
            throw new IllegalStateException("String is too long: " + length);
        }
    }

    Template readTemplate(int recordNumber) {
        int head = readInt(recordNumber);
        boolean hasPrimaryType = (head & (1 << 31)) != 0;
        boolean hasMixinTypes = (head & (1 << 30)) != 0;
        boolean zeroChildNodes = (head & (1 << 29)) != 0;
        boolean manyChildNodes = (head & (1 << 28)) != 0;
        int mixinCount = (head >> 18) & ((1 << 10) - 1);
        int propertyCount = head & ((1 << 18) - 1);

        int offset = 4;

        PropertyState primaryType = null;
        if (hasPrimaryType) {
            RecordId primaryId = readRecordId(recordNumber, offset);
            primaryType = PropertyStates.createProperty(
                    "jcr:primaryType", reader.readString(primaryId), Type.NAME);
            offset += RECORD_ID_BYTES;
        }

        PropertyState mixinTypes = null;
        if (hasMixinTypes) {
            String[] mixins = new String[mixinCount];
            for (int i = 0; i < mixins.length; i++) {
                RecordId mixinId = readRecordId(recordNumber, offset);
                mixins[i] = reader.readString(mixinId);
                offset += RECORD_ID_BYTES;
            }
            mixinTypes = PropertyStates.createProperty(
                    "jcr:mixinTypes", Arrays.asList(mixins), Type.NAMES);
        }

        String childName = Template.ZERO_CHILD_NODES;
        if (manyChildNodes) {
            childName = Template.MANY_CHILD_NODES;
        } else if (!zeroChildNodes) {
            RecordId childNameId = readRecordId(recordNumber, offset);
            childName = reader.readString(childNameId);
            offset += RECORD_ID_BYTES;
        }

        PropertyTemplate[] properties;
        properties = readProps(propertyCount, recordNumber, offset);
        return new Template(reader, primaryType, mixinTypes, properties, childName);
    }

    long readLength(int recordNumber) {
        return internalReadLength(pos(recordNumber, 1));
    }

    private long internalReadLength(int pos) {
        int length = data.get(pos++) & 0xff;
        if ((length & 0x80) == 0) {
            return length;
        } else if ((length & 0x40) == 0) {
            return ((length & 0x3f) << 8
                    | data.get(pos) & 0xff)
                    + SMALL_LIMIT;
        } else {
            return (((long) length & 0x3f) << 56
                    | ((long) (data.get(pos++) & 0xff)) << 48
                    | ((long) (data.get(pos++) & 0xff)) << 40
                    | ((long) (data.get(pos++) & 0xff)) << 32
                    | ((long) (data.get(pos++) & 0xff)) << 24
                    | ((long) (data.get(pos++) & 0xff)) << 16
                    | ((long) (data.get(pos++) & 0xff)) << 8
                    | ((long) (data.get(pos) & 0xff)))
                    + MEDIUM_LIMIT;
        }
    }

    private PropertyTemplate[] readProps(int propertyCount, int recordNumber, int offset) {
        PropertyTemplate[] properties = new PropertyTemplate[propertyCount];
        if (propertyCount > 0) {
            RecordId id = readRecordId(recordNumber, offset);
            ListRecord propertyNames = new ListRecord(id, properties.length);
            offset += RECORD_ID_BYTES;
            for (int i = 0; i < propertyCount; i++) {
                byte type = readByte(recordNumber, offset++);
                properties[i] = new PropertyTemplate(i,
                        reader.readString(propertyNames.getEntry(i)), Type.fromTag(
                        Math.abs(type), type < 0));
            }
        }
        return properties;
    }

    private ByteBuffer slice(int pos, int length) {
        ByteBuffer buffer = data.duplicate();
        buffer.position(pos);
        buffer.limit(pos + length);
        return buffer.slice();
    }

    private int pos(int recordNumber, int length) {
        return pos(recordNumber, 0, 0, length);
    }

    private int pos(int recordNumber, int rawOffset, int length) {
        return pos(recordNumber, rawOffset, 0, length);
    }

    /**
     * Maps the given record number to the respective position within the
     * internal {@link #data} array. The validity of a record with the given
     * length at the given record number is also verified.
     *
     * @param recordNumber   record number
     * @param rawOffset      offset to add to the base position of the record
     * @param recordIdOffset offset to add to to the base position of the
     *                       record, multiplied by the length of a record ID
     * @param length         record length
     * @return position within the data array
     */
    private int pos(int recordNumber, int rawOffset, int recordIdOffset, int length) {
        int offset = recordNumbers.getOffset(recordNumber);

        if (offset == -1) {
            throw new IllegalStateException("invalid record number");
        }

        int base = offset + rawOffset + recordIdOffset * RECORD_ID_BYTES;
        checkPositionIndexes(base, base + length, MAX_SEGMENT_SIZE);
        int pos = data.limit() - MAX_SEGMENT_SIZE + base;
        checkState(pos >= data.position());
        return pos;
    }

    private RecordId internalReadRecordId(int pos) {
        SegmentId segmentId = dereferenceSegmentId(asUnsigned(data.getShort(pos)));
        return new RecordId(segmentId, data.getInt(pos + 2));
    }

    private SegmentId dereferenceSegmentId(int reference) {
        if (reference == 0) {
            return id;
        }

        SegmentId id = segmentReferences.getSegmentId(reference);

        if (id == null) {
            throw new IllegalStateException("Referenced segment not found");
        }

        return id;
    }

}
