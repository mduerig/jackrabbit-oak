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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.base.Charsets;
import org.junit.Test;

public class SegmentWriterTest {

    private static ByteBuffer buffer(String s) {
        return ByteBuffer.wrap(s.getBytes(Charsets.UTF_8));
    }

    @Test
    public void testVersion() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(1, reader.version());
    }

    @Test
    public void testGeneration() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(2, reader.generation());
    }

    @Test
    public void testInitialSegmentReferencesCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(0, reader.segmentReferencesCount());
    }

    @Test
    public void testSegmentReferencesCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, buffer("1"), newHashSet(randomUUID()));
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(1, reader.segmentReferencesCount());
    }

    @Test
    public void testSegmentReferences() throws Exception {
        UUID reference = randomUUID();
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, buffer("1"), newHashSet(reference));
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(reference, reader.segmentReference(0));
    }

    @Test
    public void testInitialRecordsCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(0, reader.recordsCount());
    }

    @Test
    public void testRecordsCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, buffer("1"), null);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(1, reader.recordsCount());
    }

    @Test
    public void testRecordNumber() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, buffer("1"), null);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(1, reader.recordEntry(0).number());
    }

    @Test
    public void testRecordType() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, buffer("1"), null);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(10, reader.recordEntry(0).type());
    }

    @Test
    public void testRecordValue() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, buffer("1"), null);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(buffer("1"), reader.recordValue(1, 1));
    }

    @Test
    public void testRecordOrder() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(3, 3, buffer("3"), null);
        writer.addRecord(1, 1, buffer("1"), null);
        writer.addRecord(2, 2, buffer("2"), null);
        SegmentReader reader = SegmentReader.of(writer);
        List<Integer> numbers = new ArrayList<>();
        for (int i = 0; i < reader.recordsCount(); i++) {
            numbers.add(reader.recordEntry(i).number());
        }
        assertEquals(newArrayList(1, 2, 3), numbers);
    }

    @Test
    public void testCountDuplicateReference() throws Exception {
        UUID s = randomUUID();
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 1, buffer("1"), newHashSet(s));
        writer.addRecord(2, 2, buffer("2"), newHashSet(s));
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(1, reader.segmentReferencesCount());
    }

    @Test
    public void testDuplicateReference() throws Exception {
        UUID s = randomUUID();
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 1, buffer("1"), newHashSet(s));
        writer.addRecord(2, 2, buffer("2"), newHashSet(s));
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(s, reader.segmentReference(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicateRecordNumber() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 1, buffer("1"), newHashSet(randomUUID()));
        writer.addRecord(1, 2, buffer("2"), newHashSet(randomUUID()));
    }

}
