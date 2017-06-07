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

import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_LIMIT;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MAP_DIFF_HEADER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MAP_LEAF_EMPTY_HEADER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_BLOB_ID_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LIMIT;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.junit.Test;

public class RawRecordWriterTest {

    private static RawRecordWriter writerReturning(ByteBuffer buffer) {
        return RawRecordWriter.of(s -> -1, (n, t, s, r) -> buffer.duplicate());
    }

    private static RawRecordWriter writerReturning(UUID sid, int reference, ByteBuffer buffer) {
        return RawRecordWriter.of(s -> s == sid ? reference : -1, (n, t, s, r) -> buffer.duplicate());
    }

    @Test
    public void testWriteSmallValue() throws Exception {
        String value = Strings.repeat("x", SMALL_LIMIT - 1);
        byte[] data = value.getBytes(Charsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(SMALL_LENGTH_SIZE + data.length);
        writerReturning(buffer).writeValue(1, 1, data, 0, data.length);
        ByteBuffer expected = ByteBuffer.allocate(SMALL_LENGTH_SIZE + data.length);
        expected.duplicate().put((byte) 0x7f).put(data);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteMediumValue() throws Exception {
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
        UUID sid = randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(LONG_LENGTH_SIZE + RawRecordId.BYTES);
        writerReturning(sid, 1, buffer).writeValue(2, 3, RawRecordId.of(sid, 4), LONG_LENGTH_LIMIT - 1);
        ByteBuffer expected = ByteBuffer.allocate(LONG_LENGTH_SIZE + RawRecordId.BYTES);
        expected.duplicate().putLong(0xDFFFFFFFFFFFFFFFL).putShort((short) 1).putInt(4);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteSmallBlobId() throws Exception {
        String value = Strings.repeat("x", 16);
        byte[] data = value.getBytes(Charsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(SMALL_BLOB_ID_LENGTH_SIZE + data.length);
        writerReturning(buffer).writeBlobId(1, 2, data);
        ByteBuffer expected = ByteBuffer.allocate(SMALL_BLOB_ID_LENGTH_SIZE + data.length);
        expected.duplicate().putShort((short) 0xE010).put(data);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteLongBlobId() throws Exception {
        UUID sid = randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(SMALL_BLOB_ID_LENGTH_SIZE + RawRecordId.BYTES);
        writerReturning(sid, 1, buffer).writeBlobId(2, 3, RawRecordId.of(sid, 4));
        ByteBuffer expected = ByteBuffer.allocate(SMALL_BLOB_ID_LENGTH_SIZE + RawRecordId.BYTES);
        expected.duplicate().put((byte) 0xF0).putShort((short) 1).putInt(4);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteBlock() throws Exception {
        String value = Strings.repeat("x", 1024);
        byte[] data = value.getBytes(Charsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(data.length);
        writerReturning(buffer).writeBlock(1, 2, data, 0, data.length);
        assertEquals(ByteBuffer.wrap(data), buffer);
    }

    @Test
    public void testWriteEmptyMapLeaf() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        writerReturning(buffer).writeMapLeaf(1, 2);
        ByteBuffer expected = ByteBuffer.allocate(Integer.BYTES);
        expected.duplicate().putInt(MAP_LEAF_EMPTY_HEADER);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteMapLeaf() throws Exception {
        UUID sid = randomUUID();
        RawMapLeaf leaf = RawMapLeaf.of(1, asList(
                RawMapEntry.of(10, RawRecordId.of(sid, 11), RawRecordId.of(sid, 12)),
                RawMapEntry.of(20, RawRecordId.of(sid, 21), RawRecordId.of(sid, 22))
        ));
        int size = Integer.BYTES + (Integer.BYTES + 2 * RawRecordId.BYTES) * leaf.getEntries().size();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writerReturning(sid, 0, buffer).writeMapLeaf(1, 2, leaf);
        ByteBuffer expected = ByteBuffer.allocate(size);
        expected.duplicate()
                .putInt(0x10000002)
                .putInt(10)
                .putInt(20)
                .putShort((short) 0).putInt(11)
                .putShort((short) 0).putInt(12)
                .putShort((short) 0).putInt(21)
                .putShort((short) 0).putInt(22);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteMapBranch() throws Exception {
        UUID sid = randomUUID();
        RawMapBranch branch = RawMapBranch.of(1, 2, 3, asList(
                RawRecordId.of(sid, 1),
                RawRecordId.of(sid, 2)
        ));
        int size = 2 * Integer.BYTES + RawRecordId.BYTES * branch.getReferences().size();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writerReturning(sid, 0, buffer).writeMapBranch(1, 2, branch);
        ByteBuffer expected = ByteBuffer.allocate(size);
        expected.duplicate()
                .putInt(0x10000002)
                .putInt(3)
                .putShort((short) 0).putInt(1)
                .putShort((short) 0).putInt(2);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteListBucket() throws Exception {
        UUID sid = randomUUID();
        List<RawRecordId> bucket = asList(
                RawRecordId.of(sid, 1),
                RawRecordId.of(sid, 2)
        );
        int size = RawRecordId.BYTES * bucket.size();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writerReturning(sid, 3, buffer).writeListBucket(4, 5, bucket);
        ByteBuffer expected = ByteBuffer.allocate(size);
        expected.duplicate()
                .putShort((short) 3).putInt(1)
                .putShort((short) 3).putInt(2);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteList() throws Exception {
        UUID sid = randomUUID();
        RawList list = RawList.of(10, RawRecordId.of(sid, 1));
        int size = Integer.BYTES + RawRecordId.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writerReturning(sid, 2, buffer).writeList(4, 5, list);
        ByteBuffer expected = ByteBuffer.allocate(size);
        expected.duplicate()
                .putInt(10)
                .putShort((short) 2)
                .putInt(1);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteEmptyList() throws Exception {
        int size = Integer.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writerReturning(buffer).writeList(1, 2, RawList.empty());
        ByteBuffer expected = ByteBuffer.allocate(size);
        expected.duplicate().putInt(0);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteNode() throws Exception {
        UUID sid = randomUUID();
        RawNode node = RawNode.builder()
                .withTemplate(RawRecordId.of(sid, 10))
                .build();
        int size = 2 * RawRecordId.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writerReturning(sid, 1, buffer).writeNode(2, 3, node);
        ByteBuffer expected = ByteBuffer.allocate(size);
        expected.duplicate()
                .putShort((short) 0).putInt(2)
                .putShort((short) 1).putInt(10);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteNodeWithStableId() throws Exception {
        UUID sid = randomUUID();
        RawNode node = RawNode.builder()
                .withTemplate(RawRecordId.of(sid, 10))
                .withStableId(RawRecordId.of(sid, 20))
                .build();
        int size = 2 * RawRecordId.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writerReturning(sid, 1, buffer).writeNode(2, 3, node);
        ByteBuffer expected = ByteBuffer.allocate(size);
        expected.duplicate()
                .putShort((short) 1).putInt(20)
                .putShort((short) 1).putInt(10);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteNodeWithPropertiesList() throws Exception {
        UUID sid = randomUUID();
        RawNode node = RawNode.builder()
                .withTemplate(RawRecordId.of(sid, 10))
                .withPropertiesList(RawRecordId.of(sid, 20))
                .build();
        int size = 3 * RawRecordId.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writerReturning(sid, 1, buffer).writeNode(2, 3, node);
        ByteBuffer expected = ByteBuffer.allocate(size);
        expected.duplicate()
                .putShort((short) 0).putInt(2)
                .putShort((short) 1).putInt(10)
                .putShort((short) 1).putInt(20);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteNodeWithChild() throws Exception {
        UUID sid = randomUUID();
        RawNode node = RawNode.builder()
                .withTemplate(RawRecordId.of(sid, 10))
                .withChild(RawRecordId.of(sid, 20))
                .withPropertiesList(RawRecordId.of(sid, 30))
                .build();
        int size = 4 * RawRecordId.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writerReturning(sid, 1, buffer).writeNode(2, 3, node);
        ByteBuffer expected = ByteBuffer.allocate(size);
        expected.duplicate()
                .putShort((short) 0).putInt(2)
                .putShort((short) 1).putInt(10)
                .putShort((short) 1).putInt(20)
                .putShort((short) 1).putInt(30);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteNodeWithChildrenMap() throws Exception {
        UUID sid = randomUUID();
        RawNode node = RawNode.builder()
                .withTemplate(RawRecordId.of(sid, 10))
                .withChildrenMap(RawRecordId.of(sid, 20))
                .withPropertiesList(RawRecordId.of(sid, 30))
                .build();
        int size = 4 * RawRecordId.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writerReturning(sid, 1, buffer).writeNode(2, 3, node);
        ByteBuffer expected = ByteBuffer.allocate(size);
        expected.duplicate()
                .putShort((short) 0).putInt(2)
                .putShort((short) 1).putInt(10)
                .putShort((short) 1).putInt(20)
                .putShort((short) 1).putInt(30);
        assertEquals(expected, buffer);
    }

    @Test
    public void testWriteMapDiff() throws Exception {
        UUID sid = randomUUID();
        RawMapDiff diff = RawMapDiff.of(
                RawRecordId.of(sid, 10),
                RawMapEntry.of(42, RawRecordId.of(sid, 20), RawRecordId.of(sid, 30))
        );
        int size = 2 * Integer.BYTES + 3 * RawRecordId.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writerReturning(sid, 1, buffer).writeMapDiff(2, 3, diff);
        ByteBuffer expected = ByteBuffer.allocate(size);
        expected.duplicate()
                .putInt(MAP_DIFF_HEADER)
                .putInt(42)
                .putShort((short) 1).putInt(20)
                .putShort((short) 1).putInt(30)
                .putShort((short) 1).putInt(10);
        assertEquals(expected, buffer);
    }

}
