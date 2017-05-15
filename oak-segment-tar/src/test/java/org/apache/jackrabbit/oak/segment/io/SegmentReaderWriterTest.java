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
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

public class SegmentReaderWriterTest {

    private static Set<Integer> recordNumbers(SegmentAccess s) {
        Set<Integer> numbers = new HashSet<>();
        for (int i = 0; i < s.recordCount(); i++) {
            numbers.add(s.recordEntry(i).number());
        }
        return numbers;
    }

    @Test
    public void testVersion() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(reader.version(), writer.version());
    }

    @Test
    public void testGeneration() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(writer.generation(), reader.generation());
    }

    @Test
    public void testInitialSegmentReferencesCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(writer.segmentReferenceCount(), reader.segmentReferenceCount());
    }

    @Test
    public void testSegmentReferencesCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, 1, newHashSet(randomUUID()));
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(writer.segmentReferenceCount(), reader.segmentReferenceCount());
    }

    @Test
    public void testSegmentReferences() throws Exception {
        UUID reference = randomUUID();
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, 1, newHashSet(reference));
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(writer.segmentReference(0), reader.segmentReference(0));
    }

    @Test
    public void testInitialRecordsCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(writer.recordCount(), reader.recordCount());
    }

    @Test
    public void testRecordsCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, 1, null);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(writer.recordCount(), reader.recordCount());
    }

    @Test
    public void testRecordNumber() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, 1, null);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(writer.recordEntry(0).number(), reader.recordEntry(0).number());
    }

    @Test
    public void testRecordType() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, 1, null);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(writer.recordEntry(0).type(), reader.recordEntry(0).type());
    }

    @Test
    public void testRecordValue() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, 1, null);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(writer.recordValue(1, 1), reader.recordValue(1, 1));
    }

    @Test
    public void testRecordOrder() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(3, 3, 1, null);
        writer.addRecord(1, 1, 1, null);
        writer.addRecord(2, 2, 1, null);
        SegmentReader reader = SegmentReader.of(writer);
        assertEquals(recordNumbers(writer), recordNumbers(reader));
    }

}
