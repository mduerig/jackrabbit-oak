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

    private ByteBuffer value(int recordNumber, int length) {
        int offset = recordNumbers.getOffset(recordNumber);

        if (offset == -1) {
            throw new IllegalStateException("invalid record number");
        }

        checkPositionIndexes(offset, offset + length, MAX_SEGMENT_SIZE);
        ByteBuffer slice = data.duplicate();
        slice.position(slice.limit() - (MAX_SEGMENT_SIZE - offset));
        slice.limit(slice.position() + length);
        return slice.slice();
    }

    private ByteBuffer value(int recordNumber, int rawOffset, int recordIdOffset, int length) {
        ByteBuffer value = value(recordNumber, length + rawOffset + recordIdOffset * RECORD_ID_BYTES);
        value.position(rawOffset + recordIdOffset * RECORD_ID_BYTES);
        return value.slice();
    }

    private ByteBuffer value(int recordNumber, int rawOffset, int length) {
        ByteBuffer value = value(recordNumber, length + rawOffset);
        value.position(rawOffset);
        return value.slice();
    }

    byte readByte(int recordNumber) {
        return value(recordNumber, Byte.BYTES).get();
    }

    byte readByte(int recordNumber, int offset) {
        return value(recordNumber, offset, Byte.BYTES).get();
    }

    short readShort(int recordNumber) {
        return value(recordNumber, Short.BYTES).getShort();
    }

    int readInt(int recordNumber) {
        return value(recordNumber, Integer.BYTES).getInt();
    }

    int readInt(int recordNumber, int offset) {
        return value(recordNumber, offset, Integer.BYTES).getInt();
    }

    long readLong(int recordNumber) {
        return value(recordNumber, Long.BYTES).getLong();
    }

    void readBytes(int recordNumber, int position, byte[] buffer, int offset, int length) {
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);
        value(recordNumber, position, length).get(buffer, offset, length);
    }

    ByteBuffer readBytes(int recordNumber, int position, int length) {
        return value(recordNumber, position, length);
    }

    private static class RawRecordId {

        short segment;

        int number;

    }

    private RawRecordId readRawRecordId(int recordNumber, int rawOffset, int recordIdOffset) {
        ByteBuffer value = value(recordNumber, rawOffset, recordIdOffset, RECORD_ID_BYTES);
        RawRecordId id = new RawRecordId();
        id.segment = value.getShort();
        id.number = value.getInt();
        return id;
    }

    private RawRecordId readRawRecordId(int recordNumber, int rawOffset) {
        return readRawRecordId(recordNumber, rawOffset, 0);
    }

    private RecordId readRecordId(RawRecordId raw) {
        return new RecordId(dereferenceSegmentId(raw.segment), raw.number);
    }

    RecordId readRecordId(int recordNumber, int rawOffset, int recordIdOffset) {
        return readRecordId(readRawRecordId(recordNumber, rawOffset, recordIdOffset));
    }

    RecordId readRecordId(int recordNumber, int rawOffset) {
        return readRecordId(recordNumber, rawOffset, 0);
    }

    RecordId readRecordId(int recordNumber) {
        return readRecordId(recordNumber, 0, 0);
    }

    private static final short MEDIUM_LENGTH_MASK = 0x3FFF;

    private static final long LONG_LENGTH_MASK = 0x3FFFFFFFFFFFFFFFL;

    long readLength(int recordNumber) {
        byte marker = readByte(recordNumber);

        if ((marker & 0x80) == 0) {
            return marker;
        }

        if ((marker & 0x40) == 0) {
            return (readShort(recordNumber) & MEDIUM_LENGTH_MASK) + SMALL_LIMIT;
        }

        return (readLong(recordNumber) & LONG_LENGTH_MASK) + MEDIUM_LIMIT;
    }

    private static final int SMALL_LENGTH_SIZE = Byte.BYTES;

    private static final int MEDIUM_LENGTH_SIZE = Short.BYTES;

    private static final int LONG_LENGTH_SIZE = Long.BYTES;

    private static String decode(ByteBuffer buffer) {
        return Charsets.UTF_8.decode(buffer).toString();
    }

    private String readSmallString(int recordNumber, int length) {
        return decode(value(recordNumber, SMALL_LENGTH_SIZE, length));
    }

    private String readMediumString(int recordNumber, int length) {
        return decode(value(recordNumber, MEDIUM_LENGTH_SIZE, length));
    }

    private String readLongString(int recordNumber, int length) {
        int elements = (length + BLOCK_SIZE - 1) / BLOCK_SIZE;
        ListRecord list = new ListRecord(readRecordId(recordNumber, LONG_LENGTH_SIZE), elements);
        try (SegmentStream stream = new SegmentStream(new RecordId(id, recordNumber), list, length)) {
            return stream.getString();
        }
    }

    private String readString(int recordNumber, long length) {
        if (length < SMALL_LIMIT) {
            return readSmallString(recordNumber, (int) length);
        }
        if (length < MEDIUM_LIMIT) {
            return readMediumString(recordNumber, (int) length);
        }
        if (length < Integer.MAX_VALUE) {
            return readLongString(recordNumber, (int) length);
        }
        throw new IllegalStateException("String is too long: " + length);
    }

    String readString(int recordNumber) {
        return readString(recordNumber, readLength(recordNumber));
    }

    private static class RawTemplate {

        RawRecordId primaryType;

        RawRecordId[] mixins;

        boolean manyChildNodes;

        boolean zeroChildNodes;

        RawRecordId childNodeName;

        RawRecordId propertyNames;

        byte[] propertyTypes;

    }

    private RawTemplate readRawTemplate(int recordNumber) {
        RawTemplate template = new RawTemplate();

        // header [primaryType] [mixinType...] [childName] [propertyNameList [propertyType...]]

        int head = readInt(recordNumber);
        boolean hasPrimaryType = (head & (1 << 31)) != 0;
        boolean hasMixinTypes = (head & (1 << 30)) != 0;
        template.zeroChildNodes = (head & (1 << 29)) != 0;
        template.manyChildNodes = (head & (1 << 28)) != 0;
        int mixinCount = (head >> 18) & ((1 << 10) - 1);
        int propertyCount = head & ((1 << 18) - 1);

        int offset = Integer.BYTES;

        if (hasPrimaryType) {
            template.primaryType = readRawRecordId(recordNumber, offset);
            offset += RECORD_ID_BYTES;
        }

        if (hasMixinTypes) {
            template.mixins = new RawRecordId[mixinCount];
            for (int i = 0; i < mixinCount; i++) {
                template.mixins[i] = readRawRecordId(recordNumber, offset);
                offset += RECORD_ID_BYTES;
            }
        }

        if (!template.zeroChildNodes && !template.manyChildNodes) {
            template.childNodeName = readRawRecordId(recordNumber, offset);
            offset += RECORD_ID_BYTES;
        }

        if (propertyCount > 0) {
            template.propertyNames = readRawRecordId(recordNumber, offset);
            offset += RECORD_ID_BYTES;
            template.propertyTypes = new byte[propertyCount];
            for (int i = 0; i < propertyCount; i++) {
                template.propertyTypes[i] = readByte(recordNumber, offset);
                offset += Byte.BYTES;
            }
        }

        return template;
    }

    private PropertyState readTemplatePrimaryType(RawTemplate raw) {
        if (raw.primaryType == null) {
            return null;
        }
        String value = reader.readString(readRecordId(raw.primaryType));
        return PropertyStates.createProperty("jcr:primaryType", value, Type.NAME);
    }

    private PropertyState readTemplateMixinTypes(RawTemplate raw) {
        if (raw.mixins == null) {
            return null;
        }
        String[] values = new String[raw.mixins.length];
        for (int i = 0; i < raw.mixins.length; i++) {
            values[i] = reader.readString(readRecordId(raw.mixins[i]));
        }
        return PropertyStates.createProperty("jcr:mixinTypes", Arrays.asList(values), Type.NAMES);
    }

    private String readTemplateChildName(RawTemplate raw) {
        if (raw.zeroChildNodes) {
            return Template.ZERO_CHILD_NODES;
        }
        if (raw.manyChildNodes) {
            return Template.MANY_CHILD_NODES;
        }
        return reader.readString(readRecordId(raw.childNodeName));
    }

    private PropertyTemplate[] readTemplateProperties(RawTemplate raw) {
        if (raw.propertyTypes == null) {
            return null;
        }
        PropertyTemplate[] properties = new PropertyTemplate[raw.propertyTypes.length];
        ListRecord names = new ListRecord(readRecordId(raw.propertyNames), raw.propertyTypes.length);
        for (int i = 0; i < raw.propertyTypes.length; i++) {
            String name = reader.readString(names.getEntry(i));
            byte type = raw.propertyTypes[i];
            properties[i] = new PropertyTemplate(i, name, Type.fromTag(Math.abs(type), type < 0));
        }
        return properties;
    }

    private Template readTemplate(RawTemplate raw) {
        PropertyState primaryType = readTemplatePrimaryType(raw);
        PropertyState mixinTypes = readTemplateMixinTypes(raw);
        String childName = readTemplateChildName(raw);
        PropertyTemplate[] properties = readTemplateProperties(raw);
        return new Template(reader, primaryType, mixinTypes, properties, childName);
    }

    Template readTemplate(int recordNumber) {
        return readTemplate(readRawTemplate(recordNumber));
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
