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

import java.nio.ByteBuffer;

import com.google.common.base.Charsets;

public abstract class RawRecordReader {

    private static final short MEDIUM_LENGTH_MASK = 0x3FFF;

    private static final long LONG_LENGTH_MASK = 0x3FFFFFFFFFFFFFFFL;

    private static final int SMALL_LENGTH_SIZE = Byte.BYTES;

    private static final int MEDIUM_LENGTH_SIZE = Short.BYTES;

    private static final int LONG_LENGTH_SIZE = Long.BYTES;

    private static final int SMALL_LIMIT = 1 << 7;

    private static final int MEDIUM_LIMIT = (1 << (16 - 2)) + SMALL_LIMIT;

    protected abstract ByteBuffer value(int recordNumber, int length);

    private ByteBuffer value(int recordNumber, int offset, int length) {
        ByteBuffer value = value(recordNumber, length + offset);
        value.position(offset);
        value.limit(offset + length);
        return value.slice();
    }

    public byte readByte(int recordNumber) {
        return value(recordNumber, Byte.BYTES).get();
    }

    public byte readByte(int recordNumber, int offset) {
        return value(recordNumber, offset, Byte.BYTES).get();
    }

    public short readShort(int recordNumber) {
        return value(recordNumber, Short.BYTES).getShort();
    }

    public int readInt(int recordNumber) {
        return value(recordNumber, Integer.BYTES).getInt();
    }

    public int readInt(int recordNumber, int offset) {
        return value(recordNumber, offset, Integer.BYTES).getInt();
    }

    private long readLong(int recordNumber) {
        return value(recordNumber, Long.BYTES).getLong();
    }

    public ByteBuffer readBytes(int recordNumber, int position, int length) {
        return value(recordNumber, position, length);
    }

    private RawRecordId readRecordId(ByteBuffer value) {
        int segmentIndex = value.getShort() & 0xffff;
        int recordNumber = value.getInt();
        return new RawRecordId(segmentIndex, recordNumber);
    }

    public RawRecordId readRecordId(int recordNumber, int offset) {
        return readRecordId(value(recordNumber, offset, RawRecordId.BYTES));
    }

    public long readLength(int recordNumber) {
        byte marker = readByte(recordNumber);
        if ((marker & 0x80) == 0) {
            return marker;
        }
        if ((marker & 0x40) == 0) {
            return (readShort(recordNumber) & MEDIUM_LENGTH_MASK) + SMALL_LIMIT;
        }
        return (readLong(recordNumber) & LONG_LENGTH_MASK) + MEDIUM_LIMIT;
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

    public RawString readString(int recordNumber) {
        return readString(recordNumber, readLength(recordNumber));
    }

    public RawTemplate readTemplate(int recordNumber) {
        // header primaryType? mixinType{0,n} childName? (propertyNameList propertyType{1,n})?

        int header = readInt(recordNumber);
        boolean hasPrimaryType = (header & (1L << 31)) != 0;
        boolean hasMixinTypes = (header & (1 << 30)) != 0;
        boolean zeroChildNodes = (header & (1 << 29)) != 0;
        boolean manyChildNodes = (header & (1 << 28)) != 0;
        int mixinCount = (header >> 18) & ((1 << 10) - 1);
        int propertyCount = header & ((1 << 18) - 1);

        int offset = Integer.BYTES;

        RawRecordId primaryType = null;
        if (hasPrimaryType) {
            primaryType = readRecordId(recordNumber, offset);
            offset += RawRecordId.BYTES;
        }

        RawRecordId[] mixins = null;
        if (hasMixinTypes) {
            mixins = new RawRecordId[mixinCount];
            for (int i = 0; i < mixinCount; i++) {
                mixins[i] = readRecordId(recordNumber, offset);
                offset += RawRecordId.BYTES;
            }
        }

        RawRecordId childNodeName = null;
        if (!zeroChildNodes && !manyChildNodes) {
            childNodeName = readRecordId(recordNumber, offset);
            offset += RawRecordId.BYTES;
        }

        RawRecordId propertyNames = null;
        byte[] propertyTypes = null;
        if (propertyCount > 0) {
            propertyNames = readRecordId(recordNumber, offset);
            offset += RawRecordId.BYTES;
            propertyTypes = new byte[propertyCount];
            for (int i = 0; i < propertyCount; i++) {
                propertyTypes[i] = readByte(recordNumber, offset);
                offset += Byte.BYTES;
            }
        }

        return new RawTemplate(
                primaryType,
                mixins,
                manyChildNodes,
                zeroChildNodes,
                childNodeName,
                propertyNames,
                propertyTypes
        );
    }

}
