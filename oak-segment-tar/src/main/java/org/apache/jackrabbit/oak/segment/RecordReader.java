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

import static com.google.common.base.Preconditions.checkPositionIndexes;
import static org.apache.jackrabbit.oak.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentWriter.BLOCK_SIZE;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.segment.io.raw.RawLongValue;
import org.apache.jackrabbit.oak.segment.io.raw.RawRecordId;
import org.apache.jackrabbit.oak.segment.io.raw.RawRecordReader;
import org.apache.jackrabbit.oak.segment.io.raw.RawShortValue;
import org.apache.jackrabbit.oak.segment.io.raw.RawTemplate;
import org.apache.jackrabbit.oak.segment.io.raw.RawValue;

/**
 * Implements logic for reading records from a segment.
 */
class RecordReader {

    private final SegmentId id;

    private final ByteBuffer data;

    private final SegmentReader reader;

    private final RecordNumbers recordNumbers;

    private final SegmentReferences segmentReferences;

    private final SegmentIdProvider segmentIdProvider;

    private final RawRecordReader raw = RawRecordReader.of(
            RecordReader.this::readSegmentId,
            RecordReader.this::readValue
    );

    RecordReader(SegmentId id, ByteBuffer data, SegmentReader reader, RecordNumbers recordNumbers, SegmentReferences segmentReferences, SegmentIdProvider segmentIdProvider) {
        this.id = id;
        this.data = data;
        this.reader = reader;
        this.recordNumbers = recordNumbers;
        this.segmentReferences = segmentReferences;
        this.segmentIdProvider = segmentIdProvider;
    }

    private UUID readSegmentId(int segmentReference) {
        if (segmentReference == 0) {
            return id.asUUID();
        }
        SegmentId segmentId = segmentReferences.getSegmentId(segmentReference);
        if (segmentId == null) {
            throw new IllegalStateException("invalid segment reference");
        }
        return segmentId.asUUID();
    }

    private ByteBuffer readValue(int recordNumber, int length) {
        int offset = recordNumbers.getOffset(recordNumber);

        if (offset == -1) {
            throw new IllegalStateException("invalid record number " + recordNumber);
        }

        checkPositionIndexes(offset, offset + length, MAX_SEGMENT_SIZE);
        ByteBuffer slice = data.duplicate();
        slice.position(slice.limit() - (MAX_SEGMENT_SIZE - offset));
        slice.limit(slice.position() + length);
        return slice.slice();
    }

    byte readByte(int recordNumber) {
        return raw.readByte(recordNumber);
    }

    byte readByte(int recordNumber, int offset) {
        return raw.readByte(recordNumber, offset);
    }

    short readShort(int recordNumber) {
        return raw.readShort(recordNumber);
    }

    int readInt(int recordNumber) {
        return raw.readInt(recordNumber);
    }

    int readInt(int recordNumber, int offset) {
        return raw.readInt(recordNumber, offset);
    }

    long readLong(int recordNumber) {
        return readValue(recordNumber, Long.BYTES).getLong();
    }

    void readBytes(int recordNumber, int position, byte[] buffer, int offset, int length) {
        raw.readBytes(recordNumber, position, length).get(buffer, offset, length);
    }

    ByteBuffer readBytes(int recordNumber, int position, int length) {
        return raw.readBytes(recordNumber, position, length);
    }

    private RawRecordId readRawRecordId(int recordNumber, int rawOffset, int recordIdOffset) {
        return raw.readRecordId(recordNumber, rawOffset + recordIdOffset * RawRecordId.BYTES);
    }

    private RecordId readRecordId(RawRecordId raw) {
        return new RecordId(dereferenceSegmentId(raw.getSegmentId()), raw.getRecordNumber());
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

    long readLength(int recordNumber) {
        return raw.readLength(recordNumber);
    }

    private String readLongString(int recordNumber, RawLongValue s) {
        int elements = (s.getLength() + BLOCK_SIZE - 1) / BLOCK_SIZE;
        ListRecord list = new ListRecord(readRecordId(s.getRecordId()), elements);
        try (SegmentStream stream = new SegmentStream(new RecordId(id, recordNumber), list, s.getLength())) {
            return stream.getString();
        }
    }

    String readString(int recordNumber) {
        RawValue s = raw.readValue(recordNumber);
        if (s instanceof RawShortValue) {
            return new String(((RawShortValue) s).getValue(), Charsets.UTF_8);
        }
        if (s instanceof RawLongValue) {
            return readLongString(recordNumber, (RawLongValue) s);
        }
        throw new IllegalStateException("invalid record value");
    }

    private RawTemplate readRawTemplate(int recordNumber) {
        return raw.readTemplate(recordNumber);
    }

    private PropertyState readTemplatePrimaryType(RawTemplate raw) {
        RawRecordId primaryType = raw.getPrimaryType();
        if (primaryType == null) {
            return null;
        }
        String value = reader.readString(readRecordId(primaryType));
        return PropertyStates.createProperty("jcr:primaryType", value, Type.NAME);
    }

    private PropertyState readTemplateMixinTypes(RawTemplate raw) {
        List<RawRecordId> mixins = raw.getMixins();
        if (mixins == null) {
            return null;
        }
        List<String> values = new ArrayList<>(mixins.size());
        for (RawRecordId mixin : mixins) {
            values.add(reader.readString(readRecordId(mixin)));
        }
        return PropertyStates.createProperty("jcr:mixinTypes", values, Type.NAMES);
    }

    private String readTemplateChildName(RawTemplate raw) {
        if (raw.hasNoChildNodes()) {
            return Template.ZERO_CHILD_NODES;
        }
        if (raw.hasManyChildNodes()) {
            return Template.MANY_CHILD_NODES;
        }
        return reader.readString(readRecordId(raw.getChildNodeName()));
    }

    private PropertyTemplate[] readTemplateProperties(RawTemplate raw) {
        List<Byte> propertyTypes = raw.getPropertyTypes();
        if (propertyTypes == null) {
            return null;
        }
        PropertyTemplate[] properties = new PropertyTemplate[propertyTypes.size()];
        ListRecord names = new ListRecord(readRecordId(raw.getPropertyNames()), propertyTypes.size());
        for (int i = 0; i < propertyTypes.size(); i++) {
            String name = reader.readString(names.getEntry(i));
            byte type = propertyTypes.get(i);
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

    private SegmentId dereferenceSegmentId(UUID segmentId) {
        long msb = segmentId.getMostSignificantBits();
        long lsb = segmentId.getLeastSignificantBits();
        return segmentIdProvider.newSegmentId(msb, lsb);
    }

}
