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

import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_LIMIT;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LIMIT;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.junit.Test;

public class RawRecordWriterTest {

    private static RawRecordWriter writerReturning(ByteBuffer buffer) {
        return writerReturning(-1, buffer);
    }

    private static RawRecordWriter writerReturning(int segmentReference, ByteBuffer buffer) {
        return RawRecordWriter.of(s -> segmentReference, (n, t, s, r) -> buffer.duplicate());
    }

    @Test
    public void testWriteSmallString() throws Exception {
        String value = Strings.repeat("x", SMALL_LIMIT - 1);
        byte[] data = value.getBytes(Charsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(SMALL_LENGTH_SIZE + data.length);
        writerReturning(buffer).writeValue(1, 1, data, 0, data.length);
        ByteBuffer expected = ByteBuffer.allocate(SMALL_LENGTH_SIZE + data.length);
        expected.duplicate().put((byte) 0x7f).put(data);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteMediumString() throws Exception {
        String value = Strings.repeat("x", MEDIUM_LIMIT - 1);
        byte[] data = value.getBytes(Charsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(MEDIUM_LENGTH_SIZE + data.length);
        writerReturning(buffer).writeValue(1, 1, data, 0, data.length);
        ByteBuffer expected = ByteBuffer.allocate(MEDIUM_LENGTH_SIZE + data.length);
        expected.duplicate().putShort((short) 0xbfff).put(data);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteLongValue() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(LONG_LENGTH_SIZE + RawRecordId.BYTES);
        writerReturning(1, buffer).writeValue(2, 3, RawRecordId.of(null, 4), LONG_LENGTH_LIMIT - 1);
        ByteBuffer expected = ByteBuffer.allocate(LONG_LENGTH_SIZE + RawRecordId.BYTES);
        expected.duplicate().putLong(0xDFFFFFFFFFFFFFFFL).putShort((short) 1).putInt(4);
        assertEquals(expected, buffer);
    }

}
