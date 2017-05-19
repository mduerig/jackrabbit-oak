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

import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;
import org.junit.Before;
import org.junit.Test;

public class MutableRecordNumbersTest {

    private MutableRecordNumbers table;

    private static List<Integer> recordNumbers(MutableRecordNumbers n) {
        return newArrayList(transform(n.iterator(), Entry::getRecordNumber));
    }

    @Before
    public void setUp() throws Exception {
        table = new MutableRecordNumbers();
    }

    @Test
    public void nonExistingRecordNumberShouldReturnSentinel() {
        assertEquals(-1, table.getOffset(42));
    }

    @Test
    public void lookupShouldReturnOffset() {
        int recordNumber = table.addRecord(RecordType.VALUE, 42);
        assertEquals(42, table.getOffset(recordNumber));
    }

    @Test
    public void sizeShouldBeValid() {
        assertEquals(0, table.size());
        table.addRecord(RecordType.VALUE, 42);
        assertEquals(1, table.size());
    }

    @Test
    public void iteratingShouldBeCorrect() {
        Map<Integer, Integer> expected = new HashMap<>();
        for (int i = 0; i < 100000; i++) {
            expected.put(table.addRecord(RecordType.VALUE, i), i);
        }
        Map<Integer, Integer> iterated = new HashMap<>();
        for (Entry entry : table) {
            iterated.put(entry.getRecordNumber(), entry.getOffset());
        }
        assertEquals(expected, iterated);
    }

    @Test
    public void testAddSpecificRecordNumberSize() throws Exception {
        table.addRecord(10, 1, 100);
        assertEquals(1, table.size());
    }

    @Test
    public void testAddSpecificRecordNumberEntry() throws Exception {
        table.addRecord(10, 1, 100);
        Entry entry = table.iterator().next();
        assertEquals(10, entry.getRecordNumber());
        assertEquals(1, entry.getType().ordinal());
        assertEquals(100, entry.getOffset());
    }

    @Test
    public void testAddSpecificRecordNumberOffset() throws Exception {
        table.addRecord(10, 1, 100);
        assertEquals(100, table.getOffset(10));
    }

    @Test
    public void testAddSpecificRecordNumberOrder() throws Exception {
        table.addRecord(20, 2, 200);
        table.addRecord(10, 1, 100);
        assertEquals(newArrayList(10, 20), recordNumbers(table));
    }

    @Test
    public void testAddSpecificAndGeneratedRecordNumberOrder() throws Exception {
        int n = table.addRecord(RecordType.VALUE, 100);
        table.addRecord(20, 2, 200);
        assertEquals(newArrayList(n, 20), recordNumbers(table));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddSpecificRecordNumberTwice() throws Exception {
        table.addRecord(100, 1, 100);
        table.addRecord(100, 1, 100);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddAssignedSpecificRecordNumber() throws Exception {
        int n = table.addRecord(RecordType.VALUE, 100);
        table.addRecord(n, 2, 200);
    }

}
