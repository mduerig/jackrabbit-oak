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
package org.apache.jackrabbit.oak.plugins.segment;

import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ID_BYTES;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.segment.Segment.Reader;

/**
 * Record within a segment.
 */
class Record {
    private final RecordId id;

    private RecordId persistId;

    static boolean fastEquals(Object a, Object b) {
        return a instanceof Record && fastEquals((Record) a, b);
    }

    static boolean fastEquals(Record a, Object b) {
        return b instanceof Record && fastEquals(a, (Record) b);
    }

    static boolean fastEquals(Record a, Record b) {
        return RecordId.fastEquals(a.persistId, b.persistId);
    }

    /**
     * Creates a new object for the identified record.
     *
     * @param id record identified
     */
    Record(@Nonnull RecordId id) {
        this.id = id;
        this.persistId = id;
    }

    // michid review wasCompactedTo
    boolean wasCompactedTo(Record after) {
        CompactionMap map = getSegmentId().getTracker().getCompactionMap();
        return map.wasCompactedTo(getRecordId(), after.getRecordId());
    }

    void relink() {
        // michid implement relink
        // sync?
        // persistId = ...
    }

    /**
     * Returns the identifier of this record.
     *
     * @return record identifier
     */
    RecordId getRecordId() {
        return id;
    }

    SegmentId getSegmentId() {
        return id.getSegmentId();
    }

    /**
     * Returns the segment offset of this record.
     *
     * @return segment offset of this record
     */
    final int getOffset() {
        return id.getOffset();
    }

    /**
     * Returns the segment that contains this record.
     *
     * @return segment that contains this record
     */
    Segment getSegment() {
        return getSegmentId().getSegment();
    }

    Reader getReader() {
        return getSegment().getReader(id);
    }

    Reader getReader(int offset) {
        return getSegment().getReader(id, offset);
    }

    Reader getReader(int offset, int ids) {
        return getReader(offset + ids * RECORD_ID_BYTES);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object that) {
        // michid clarify semantics/usages of equals/fastEquals
        return fastEquals(this, that);
    }

    @Override
    public int hashCode() {
        return getSegmentId().hashCode() ^ getOffset();
    }

    @Override
    public String toString() {
        return getRecordId().toString();
    }

}
