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

import java.util.Objects;
import java.util.UUID;

/**
 * An identifier of a record. The identifier may point to a record from a
 * different segment, in which case {@link #getSegmentId()} returns a non-{@code
 * null} value, or to a record from the same segment, in which case {@link
 * #getSegmentId()} returns {@code null}.
 */
public class RawRecordId {

    public static final int BYTES = Short.BYTES + Integer.BYTES;

    private final UUID segmentId;

    private final int recordNumber;

    RawRecordId(UUID segmentId, int recordNumber) {
        this.segmentId = segmentId;
        this.recordNumber = recordNumber;
    }

    /**
     * The identifier of the segment containing the record. It can be {@code
     * null} if the record is contained in the same segment as this record ID.
     *
     * @return An instance of {@link UUID}. It can be {@code null}.
     */
    public UUID getSegmentId() {
        return segmentId;
    }

    /**
     * The record number of the referenced record.
     *
     * @return A positive integer.
     */
    public int getRecordNumber() {
        return recordNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        return equals((RawRecordId) o);
    }

    private boolean equals(RawRecordId that) {
        return segmentId == that.segmentId && recordNumber == that.recordNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentId, recordNumber);
    }

    @Override
    public String toString() {
        return String.format("RawRecordId{segmentId=%d, recordNumber=%d}", segmentId, recordNumber);
    }

}
