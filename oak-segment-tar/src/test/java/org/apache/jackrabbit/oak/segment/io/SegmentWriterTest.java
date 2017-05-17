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

import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.junit.Test;

public class SegmentWriterTest {

    @Test
    public void testVersion() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        assertEquals(1, writer.version());
    }

    @Test
    public void testGeneration() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        assertEquals(2, writer.generation());
    }

    @Test
    public void testInitialSegmentReferencesCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        assertEquals(0, writer.segmentReferenceCount());
    }

    @Test
    public void testSegmentReferencesCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, 1, newHashSet(randomUUID()));
        assertEquals(1, writer.segmentReferenceCount());
    }

    @Test
    public void testSegmentReferences() throws Exception {
        UUID reference = randomUUID();
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, 1, newHashSet(reference));
        assertEquals(reference, writer.segmentReference(0));
    }

    @Test
    public void testInitialRecordsCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        assertEquals(0, writer.recordCount());
    }

    @Test
    public void testRecordsCount() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, 1, null);
        assertEquals(1, writer.recordCount());
    }

    @Test
    public void testRecordNumber() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, 1, null);
        assertEquals(1, writer.recordEntry(0).number());
    }

    @Test
    public void testRecordType() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, 1, null);
        assertEquals(10, writer.recordEntry(0).type());
    }

    @Test
    public void testRecordValue() throws Exception {
        String value = Strings.repeat("x", 10);
        byte[] coded = value.getBytes(Charsets.UTF_8);
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 10, coded.length, null).put(coded);
        assertEquals(ByteBuffer.wrap(coded), writer.recordValue(1, coded.length));
    }

    @Test
    public void testCountDuplicateReference() throws Exception {
        UUID s = randomUUID();
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 1, 1, newHashSet(s));
        writer.addRecord(2, 2, 1, newHashSet(s));
        assertEquals(1, writer.segmentReferenceCount());
    }

    @Test
    public void testDuplicateReference() throws Exception {
        UUID s = randomUUID();
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 1, 1, newHashSet(s));
        writer.addRecord(2, 2, 1, newHashSet(s));
        assertEquals(s, writer.segmentReference(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicateRecordNumber() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        writer.addRecord(1, 1, 1, newHashSet(randomUUID()));
        writer.addRecord(1, 2, 1, newHashSet(randomUUID()));
    }

    @Test
    public void testSegmentReferenceIndex() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        UUID s = randomUUID();
        writer.addRecord(1, 1, 1, newHashSet(s));
        assertEquals(0, writer.segmentReferenceIndex(s));
    }

    @Test
    public void testSegmentReferenceIndexNotFound() throws Exception {
        SegmentWriter writer = SegmentWriter.of(1, 2);
        assertEquals(-1, writer.segmentReferenceIndex(randomUUID()));
    }

}
