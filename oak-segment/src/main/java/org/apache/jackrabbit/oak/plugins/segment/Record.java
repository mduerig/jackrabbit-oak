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

import static com.google.common.base.Preconditions.checkState;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Record within a segment.
 */
class Record {

    // michid undo type specialisation
    static boolean fastEqualsMM(MapRecord a, MapRecord b) {
        return fastEqualsRR(a, b);
    }

    static boolean fastEqualsNN(NodeState a, NodeState b) {
        if (a instanceof SegmentNodeState && b instanceof SegmentNodeState) {
            if (((SegmentNodeState) a).getId().equals(((SegmentNodeState) b).getId())) {
                checkState(AbstractNodeState.equals(a, b)); // michid remove this performance killer
                return true;
            }
        }

        return a instanceof Record && fastEqualsRO((Record) a, b);
    }

    static boolean fastEqualsNO(SegmentNodeState a, Object b) {
        return b instanceof Record && fastEqualsRR(a, (Record) b);
    }

    static boolean fastEqualsBO(SegmentBlob a, Object b) {
        return b instanceof Record && fastEqualsRR(a, (Record) b);
    }

    private static boolean fastEqualsRR(Record a, Record b) {
        return a.offset == b.offset && a.segmentId.equals(b.segmentId);
    }

    private static boolean fastEqualsRO(Record a, Object b) {
        return b instanceof Record && fastEqualsRR(a, (Record) b);
    }

    /**
     * Identifier of the segment that contains this record.
     */
    private final SegmentId segmentId;

    /**
     * Segment offset of this record.
     */
    private final int offset;

    /**
     * Creates a new object for the identified record.
     *
     * @param id record identified
     */
    protected Record(@Nonnull RecordId id) {
        this(id.getSegmentId(), id.getOffset());
    }

    protected Record(@Nonnull SegmentId segmentId, int offset) {
        this.segmentId = segmentId;
        this.offset = offset;
    }

    /**
     * Returns the tracker of the segment that contains this record.
     *
     * @return segment tracker
     */
    protected SegmentTracker getTracker() {
        return segmentId.getTracker();
    }

    /**
     * Returns the segment that contains this record.
     *
     * @return segment that contains this record
     */
    protected Segment getSegment() {
        return segmentId.getSegment();
    }

    /**
     * Returns the identifier of this record.
     *
     * @return record identifier
     */
    public RecordId getRecordId() {
        return new RecordId(segmentId, offset);
    }

    /**
     * Returns the segment offset of this record.
     *
     * @return segment offset of this record
     */
    protected final int getOffset() {
        return offset;
    }

    /**
     * Returns the segment offset of the given byte position in this record.
     *
     * @param position byte position within this record
     * @return segment offset of the given byte position
     */
    protected final int getOffset(int position) {
        return getOffset() + position;
    }

    /**
     * Returns the segment offset of a byte position in this record.
     * The position is calculated from the given number of raw bytes and
     * record identifiers.
     *
     * @param bytes number of raw bytes before the position
     * @param ids number of record identifiers before the position
     * @return segment offset of the specified byte position
     */
    protected final int getOffset(int bytes, int ids) {
        return getOffset(bytes + ids * Segment.RECORD_ID_BYTES);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object that) {
        return fastEqualsRO(this, that);
    }

    @Override
    public int hashCode() {
        return segmentId.hashCode() ^ offset;
    }

    @Override
    public String toString() {
        return getRecordId().toString();
    }

}
