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

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.segment.SegmentWriterBuilder.segmentWriterBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Before;
import org.junit.Test;

public class SegmentReaderTest {

    private MemoryStore store;

    private SegmentWriter writer;

    private ByteBuffer readSegment(SegmentId id) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        store.readSegment(id).writeTo(stream);
        return ByteBuffer.wrap(stream.toByteArray());
    }

    @Before
    public void createWriter() throws Exception {
        store = new MemoryStore();
        writer = segmentWriterBuilder("t").build(store);
    }

    @Test
    public void testSegmentReferences() throws Exception {
        RecordId one = writer.writeString("one");
        writer.flush();

        RecordId two = writer.writeString("two");
        writer.flush();

        RecordId lst = writer.writeList(Arrays.asList(one, two));
        writer.flush();

        // The following assertions show that the three records live in three
        // different segments. This also means that the segment where the list
        // record lives will have references to the segments where the string
        // records live.
        assertNotEquals(one.getSegmentId(), two.getSegmentId());
        assertNotEquals(two.getSegmentId(), lst.getSegmentId());
        assertNotEquals(lst.getSegmentId(), one.getSegmentId());

        SegmentReader reader = SegmentReader.of(readSegment(lst.getSegmentId()));

        // The reader should be able to correctly fetch the number of segment
        // references.
        assertEquals(2, reader.segmentReferenceCount());

        // The segment references should be correct.
        Set<UUID> expectedReferences = newHashSet(
                one.asUUID(),
                two.asUUID()
        );
        Set<UUID> actualReferences = newHashSet(
                reader.segmentReference(0),
                reader.segmentReference(1)
        );
        assertEquals(expectedReferences, actualReferences);
    }

    @Test
    public void testRecordReferences() throws Exception {
        RecordId one = writer.writeString("one");
        RecordId two = writer.writeString("two");
        writer.flush();

        SegmentReader reader = SegmentReader.of(readSegment(one.getSegmentId()));

        // The reader should report the correct number of record entries in the
        // segment. The following assertions takes in consideration the segment
        // info, that is automatically written as the first record in the
        // segment.
        assertEquals(3, reader.recordCount());

        // Assuming that records are not empty, it should be possible to read
        // every one of them from the segment given their record number. We
        // can't assert more than this, because doing so would require this test
        // to either know the correct length of the record or to know how to
        // parse a record given its type.
        for (int i = 0; i < reader.recordCount(); i++) {
            assertNotNull(reader.recordValue(reader.recordEntry(i).number(), 1));
        }
    }

}
