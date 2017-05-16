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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.junit.Test;

public class RawRecordReaderTest {

    private static RawRecordReader readerReturning(ByteBuffer value) {
        return new RawRecordReader() {

            @Override
            protected ByteBuffer value(int recordNumber, int length) {
                return value.duplicate();
            }

            @Override
            protected UUID segmentId(int segmentReference) {
                return null;
            }

        };
    }

    @Test
    public void testReadByte() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES);
        buffer.duplicate().put((byte) 2);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(2, reader.readByte(1));
    }

    @Test
    public void testReadByteAtOffset() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(3 + Byte.BYTES);
        buffer.duplicate().put(3, (byte) 2);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(2, reader.readByte(1, 3));
    }

    @Test
    public void readShort() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
        buffer.duplicate().putShort((short) 2);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(2, reader.readShort(1));
    }

    @Test
    public void readInt() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.duplicate().putInt(2);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(2, reader.readInt(1));
    }

    @Test
    public void readIntAtOffset() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + 3);
        buffer.duplicate().putInt(3, 2);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(2, reader.readInt(1, 3));
    }

    @Test
    public void readLong() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(ByteBuffer.wrap(new byte[] {2, 3}), reader.readBytes(1, 1, 2));
    }

    @Test
    public void readRecordId() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(RawRecordId.BYTES + 3);
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(3);
        duplicate.putShort((short) 0).putInt(2);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(new RawRecordId(null, 2), reader.readRecordId(1, 3));
    }

    @Test
    public void readSmallLength() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES);
        buffer.duplicate().put((byte) 0x7f);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(0x7f, reader.readLength(1));
    }

    @Test
    public void readMediumLength() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
        buffer.duplicate().putShort((short) 0xbfff);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(0x80 + 0x3fff, reader.readLength(1));
    }

    @Test
    public void readLargeLength() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.duplicate().putLong(0xdfffffffffffffffL);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(0x80 + 0x4000 + 0x1fffffffffffffffL, reader.readLength(1));
    }

    @Test
    public void readSmallString() throws Exception {
        String value = Strings.repeat("x", 0x7f);
        byte[] encoded = value.getBytes(Charsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES + encoded.length);
        buffer.duplicate().put((byte) (encoded.length & 0x7f)).put(encoded);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(new RawShortString(value), reader.readString(1));
    }

    @Test
    public void readMediumString() throws Exception {
        String value = Strings.repeat("x", 0x80);
        byte[] encoded = value.getBytes(Charsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES + encoded.length);
        buffer.duplicate().putShort((short) (((encoded.length - 0x80) & 0x3fff) | 0x8000)).put(encoded);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(new RawShortString(value), reader.readString(1));
    }

    @Test
    public void readLongString() throws Exception {
        long length = 0x80 + 0x4000;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + RawRecordId.BYTES);
        buffer.duplicate().putLong(((length - 0x80 - 0x4000) & 0x3fffffffffffffffL) | 0xc000000000000000L).putShort((short) 0).putInt(2);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(new RawLongString(new RawRecordId(null, 2), (int) length), reader.readString(1));
    }

    @Test(expected = IllegalStateException.class)
    public void readTooLongString() throws Exception {
        long length = Long.MAX_VALUE;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + RawRecordId.BYTES);
        buffer.duplicate().putLong(((length - 0x80 - 0x4000) & 0x3fffffffffffffffL) | 0xc000000000000000L).putShort((short) 0).putInt(2);
        RawRecordReader reader = readerReturning(buffer);
        assertEquals(new RawLongString(new RawRecordId(null, 2), (int) length), reader.readString(1));
    }

    @Test
    public void readTemplateWithNoChildNodes() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.duplicate().putInt(0x20000000);
        RawRecordReader reader = readerReturning(buffer);
        RawTemplate template = RawTemplate.builder()
                .withNoChildNodes()
                .build();
        assertEquals(template, reader.readTemplate(1));
    }

    @Test
    public void readTemplateWithManyChildNodes() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.duplicate().putInt(0x10000000);
        RawRecordReader reader = readerReturning(buffer);
        RawTemplate template = RawTemplate.builder()
                .withManyChildNodes()
                .build();
        assertEquals(template, reader.readTemplate(1));
    }

    @Test
    public void readTemplateWithPrimaryType() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + RawRecordId.BYTES);
        buffer.duplicate().putInt(0xa0000000).putShort((short) 0).putInt(2);
        RawRecordReader reader = readerReturning(buffer);
        RawTemplate template = RawTemplate.builder()
                .withNoChildNodes()
                .withPrimaryType(new RawRecordId(null, 2))
                .build();
        assertEquals(template, reader.readTemplate(1));
    }

    @Test
    public void readTemplateWithMixinTypes() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + RawRecordId.BYTES * 3);
        buffer.duplicate()
                .putInt(0x600c0000)
                .putShort((short) 0).putInt(2)
                .putShort((short) 0).putInt(4)
                .putShort((short) 0).putInt(6);
        RawRecordReader reader = readerReturning(buffer);
        RawTemplate template = RawTemplate.builder()
                .withNoChildNodes()
                .withMixins(new RawRecordId[] {
                        new RawRecordId(null, 2),
                        new RawRecordId(null, 4),
                        new RawRecordId(null, 6),
                })
                .build();
        assertEquals(template, reader.readTemplate(1));
    }

    @Test
    public void readTemplateWithChildNodeName() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + RawRecordId.BYTES);
        buffer.duplicate()
                .putInt(0)
                .putShort((short) 0).putInt(2);
        RawRecordReader reader = readerReturning(buffer);
        RawTemplate template = RawTemplate.builder()
                .withChildNodeName(new RawRecordId(null, 2))
                .build();
        assertEquals(template, reader.readTemplate(1));
    }

    @Test
    public void readTemplateWithProperties() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + RawRecordId.BYTES + Byte.BYTES * 3);
        buffer.duplicate()
                .putInt(0x20000003)
                .putShort((short) 0).putInt(2)
                .put(new byte[] {3, 4, 5});
        RawRecordReader reader = readerReturning(buffer);
        RawTemplate template = RawTemplate.builder()
                .withNoChildNodes()
                .withPropertyNames(new RawRecordId(null, 2))
                .withPropertyTypes(new byte[] {3, 4, 5})
                .build();
        assertEquals(template, reader.readTemplate(1));
    }

}
