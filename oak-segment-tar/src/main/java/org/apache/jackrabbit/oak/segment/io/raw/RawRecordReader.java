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

import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_DELTA;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_MARKER_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_DELTA;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_MARKER_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LENGTH_MARKER_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LIMIT;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.oak.segment.io.raw.RawTemplate.Builder;

/**
 * Read raw records from the underlying storage format.
 */
public abstract class RawRecordReader {

    /**
     * Return the record data for a given record number.
     *
     * @param recordNumber The record number.
     * @param size         The size of the record.
     * @return An instance of {@link ByteBuffer}.
     * @throws IllegalStateException if the record number is invalid.
     */
    protected abstract ByteBuffer value(int recordNumber, int size);

    /**
     * Return the segment ID for a given segment reference.
     *
     * @param segmentReference The segment reference.
     * @return An instance of {@link UUID}, or {@code null}
     * @throws IllegalStateException if the segment reference is invalid.
     */
    protected abstract UUID segmentId(int segmentReference);

    private ByteBuffer value(int recordNumber, int offset, int length) {
        ByteBuffer value = value(recordNumber, length + offset);
        value.position(offset);
        value.limit(offset + length);
        return value.slice();
    }

    /**
     * Read the first byte of a record.
     *
     * @param recordNumber The record number.
     * @return The first byte of the record.
     */
    public byte readByte(int recordNumber) {
        return value(recordNumber, Byte.BYTES).get();
    }

    /**
     * Read a byte from a record at a specific offset.
     *
     * @param recordNumber The record number.
     * @param offset       The offset of the data to read.
     * @return The byte at the specified offset.
     */
    public byte readByte(int recordNumber, int offset) {
        return value(recordNumber, offset, Byte.BYTES).get();
    }

    /**
     * Read the first short integer of a record.
     *
     * @param recordNumber The record number.
     * @return The first short integer of the record.
     */
    public short readShort(int recordNumber) {
        return value(recordNumber, Short.BYTES).getShort();
    }

    /**
     * Read the first integer of a record.
     *
     * @param recordNumber The record number.
     * @return The first integer of a record.
     */
    public int readInt(int recordNumber) {
        return value(recordNumber, Integer.BYTES).getInt();
    }

    /**
     * Read an integer from a record at a specific offset.
     *
     * @param recordNumber The record number.
     * @param offset       The offset of the data to read.
     * @return The integer from the record at the specified offset.
     */
    public int readInt(int recordNumber, int offset) {
        return value(recordNumber, offset, Integer.BYTES).getInt();
    }

    private long readLong(int recordNumber) {
        return value(recordNumber, Long.BYTES).getLong();
    }

    /**
     * Read a sequence of bytes from a record.
     *
     * @param recordNumber The record number.
     * @param position     The position in the record where to start reading
     *                     from.
     * @param length       The amount of bytes to read.
     * @return An instance of {@link ByteBuffer}.
     */
    public ByteBuffer readBytes(int recordNumber, int position, int length) {
        return value(recordNumber, position, length);
    }

    private RawRecordId readRecordId(ByteBuffer value) {
        int segmentReference = value.getShort() & 0xffff;
        int recordNumber = value.getInt();
        return new RawRecordId(segmentId(segmentReference), recordNumber);
    }

    /**
     * Read a record identifier from a record at a specific offset.
     *
     * @param recordNumber The number of the record.
     * @param offset       The offset of the record ID in the record.
     * @return An instance of {@link RawRecordId}.
     */
    public RawRecordId readRecordId(int recordNumber, int offset) {
        return readRecordId(value(recordNumber, offset, RawRecordId.BYTES));
    }

    private static boolean isShortLength(byte marker) {
        return (marker & SMALL_LENGTH_MARKER_MASK) == SMALL_LENGTH_MARKER;
    }

    private static boolean isMediumLength(byte marker) {
        return ((byte) (marker & MEDIUM_LENGTH_MARKER_MASK)) == ((byte) MEDIUM_LENGTH_MARKER);
    }

    private static boolean isLongLength(byte marker) {
        return ((byte) (marker & LONG_LENGTH_MARKER_MASK)) == ((byte) LONG_LENGTH_MARKER);
    }

    /**
     * Read a length from the beginning of a record. A length is a variable
     * length integer that represents the size of a variable-length piece of
     * data, e.g. of a string.
     *
     * @param recordNumber The record number.
     * @return The length from the record.
     */
    public long readLength(int recordNumber) {
        byte marker = readByte(recordNumber);
        if (isShortLength(marker)) {
            // Small length, 1 byte, starting with 0xxx xxxx
            return marker;
        }
        if (isMediumLength(marker)) {
            // Medium length, 2 bytes, starting with 10xx xxxx
            return (readShort(recordNumber) & MEDIUM_LENGTH_MASK) + MEDIUM_LENGTH_DELTA;
        }
        if (isLongLength(marker)) {
            // Long length, 8 bytes, starting 110x xxxx
            return (readLong(recordNumber) & LONG_LENGTH_MASK) + LONG_LENGTH_DELTA;
        }
        throw new IllegalStateException("invalid length marker");
    }

    private static String decode(ByteBuffer buffer) {
        return Charsets.UTF_8.decode(buffer).toString();
    }

    private RawShortString readSmallString(int recordNumber, int length) {
        return new RawShortString(decode(value(recordNumber, SMALL_LENGTH_SIZE, length)));
    }

    private RawShortString readMediumString(int recordNumber, int length) {
        return new RawShortString(decode(value(recordNumber, MEDIUM_LENGTH_SIZE, length)));
    }

    private RawLongString readLongString(int recordNumber, int length) {
        return new RawLongString(readRecordId(recordNumber, LONG_LENGTH_SIZE), length);
    }

    private RawString readString(int recordNumber, long length) {
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

    /**
     * Read a string record.
     *
     * @param recordNumber The record number.
     * @return An instance of {@link RawString}.
     */
    public RawString readString(int recordNumber) {
        return readString(recordNumber, readLength(recordNumber));
    }

    /**
     * Read a template record.
     *
     * @param recordNumber The record number.
     * @return An instance of {@link RawTemplate}.
     */
    public RawTemplate readTemplate(int recordNumber) {
        Builder builder = RawTemplate.builder();

        // The template is a variable-length record composed of the following
        // fields:
        //
        //     header primaryType? mixinType{0,n} childName? (propertyNameList propertyType{1,n})?
        //
        // where `header` is a mandatory 32bit integer and determines the
        // presence and the amount of the subsequent fields; `primaryType` is a
        // record ID pointing to a string that represents the primary type of
        // the node; `mixinType` is a list of record IDs, each of them pointing
        // to a string that represents a mixin type; `childName` is a record ID
        // pointing to a string that represents the name of the only child of
        // this node; `propertyNameList` is a record ID pointing to a list
        // record containing the name of the properties of this node;
        // `propertyType` is a list of 8bit integers, each of them representing
        // the type of a property of this node.
        //
        // The header is composed of the following flags and fields.
        //
        //     ABCD EEEE  EEEE EEFF  FFFF FFFF  FFFF FFFF
        //
        // where `A` is `1` iff the template has a `primaryType` field; `B` is
        // `1` iff the template has a non-empty `mixinType` field; `C` is `1`
        // iff the node doesn't have any child nodes; `D` is `1` iff the node
        // has more than one child node; `E` is a 10bit integer that represents
        // the number of mixins in the node; `F` is a 18bit integer that
        // represents the number of property in the node.

        int header = readInt(recordNumber);
        boolean hasPrimaryType = (header & (1L << 31)) != 0;
        boolean hasMixinTypes = (header & (1 << 30)) != 0;
        boolean noChildNodes = (header & (1 << 29)) != 0;
        boolean manyChildNodes = (header & (1 << 28)) != 0;
        int mixinCount = (header >> 18) & ((1 << 10) - 1);
        int propertyCount = header & ((1 << 18) - 1);

        if (noChildNodes) {
            builder.withNoChildNodes();
        }

        if (manyChildNodes) {
            builder.withManyChildNodes();
        }

        int offset = Integer.BYTES;

        if (hasPrimaryType) {
            builder.withPrimaryType(readRecordId(recordNumber, offset));
            offset += RawRecordId.BYTES;
        }

        if (hasMixinTypes) {
            RawRecordId[] mixins = new RawRecordId[mixinCount];
            for (int i = 0; i < mixinCount; i++) {
                mixins[i] = readRecordId(recordNumber, offset);
                offset += RawRecordId.BYTES;
            }
            builder.withMixins(mixins);
        }

        if (!noChildNodes && !manyChildNodes) {
            builder.withChildNodeName(readRecordId(recordNumber, offset));
            offset += RawRecordId.BYTES;
        }

        if (propertyCount > 0) {
            builder.withPropertyNames(readRecordId(recordNumber, offset));
            offset += RawRecordId.BYTES;
            byte[] propertyTypes = new byte[propertyCount];
            for (int i = 0; i < propertyCount; i++) {
                propertyTypes[i] = readByte(recordNumber, offset);
                offset += Byte.BYTES;
            }
            builder.withPropertyTypes(propertyTypes);
        }

        return builder.build();
    }

}
