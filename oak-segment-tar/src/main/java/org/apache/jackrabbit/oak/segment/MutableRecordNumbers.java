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

import static java.util.Arrays.copyOf;
import static java.util.Arrays.fill;

import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

/**
 * A thread-safe, mutable record table.
 */
class MutableRecordNumbers implements RecordNumbers {

    private int[] recordEntries;

    private int next;

    private int size;

    MutableRecordNumbers() {
        recordEntries = new int[16384];
        fill(recordEntries, -1);
    }

    @Override
    public int getOffset(int recordNumber) {
        int recordEntry = getRecordEntry(recordEntries, recordNumber);

        if (recordEntry == -1) {
            synchronized (this) {
                recordEntry = getRecordEntry(recordEntries, recordNumber);
            }
        }
        return recordEntry;
    }

    private static int getRecordEntry(int[] entries, int index) {
        return index * 2 >= entries.length
                ? -1
                : entries[index * 2];
    }

    @Override
    public synchronized Iterator<Entry> iterator() {
        return new AbstractIterator<Entry>() {

            final int[] entries = copyOf(recordEntries, next * 2);

            int index = 0;

            @Override
            protected Entry computeNext() {
                while (index < entries.length) {
                    final int recordNumber = index / 2;
                    final int offset = entries[index++];
                    final int type = entries[index++];
                    if (offset == -1) {
                        continue;
                    }
                    return new Entry() {

                        @Override
                        public int getRecordNumber() {
                            return recordNumber;
                        }

                        @Override
                        public int getOffset() {
                            return offset;
                        }

                        @Override
                        public RecordType getType() {
                            return RecordType.values()[type];
                        }

                    };
                }
                return endOfData();
            }

        };
    }

    /**
     * Return the size of this table.
     *
     * @return the size of this table.
     */
    public synchronized int size() {
        return size;
    }

    /**
     * Add a new offset to this table and generate a record number for it.
     *
     * @param type   the type of the record.
     * @param offset an offset to be added to this table.
     * @return the record number associated to the offset.
     */
    synchronized int addRecord(RecordType type, int offset) {
        return internalAddRecord(next, type.ordinal(), offset);
    }

    /**
     * Add an entry to this table for a specific record number.
     *
     * @param number The record number.
     * @param type   The type of the record.
     * @param offset The offset of the record.
     * @throws IllegalArgumentException if the provided record number already
     *                               exists in this table.
     */
    synchronized void addRecord(int number, int type, int offset) {
        internalAddRecord(number, type, offset);
    }

    private int internalAddRecord(int recordId, int type, int offset) {
        int index = 2 * recordId;
        while (index >= recordEntries.length) {
            recordEntries = copyOf(recordEntries, recordEntries.length * 2);
            fill(recordEntries, recordEntries.length / 2, recordEntries.length, -1);
        }
        if (recordEntries[index] != -1) {
            throw new IllegalArgumentException("record number already assigned: " + recordId);
        }
        recordEntries[index] = offset;
        recordEntries[index + 1] = type;
        next = recordId >= next ? recordId + 1 : next;
        size++;
        return recordId;
    }

}
