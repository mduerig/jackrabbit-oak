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

public class RawRecordId {

    public static final int BYTES = Short.BYTES + Integer.BYTES;

    private final int segmentIndex;

    private final int recordNumber;

    RawRecordId(int segmentIndex, int recordNumber) {
        this.segmentIndex = segmentIndex;
        this.recordNumber = recordNumber;
    }

    public int getSegmentIndex() {
        return segmentIndex;
    }

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
        return segmentIndex == that.segmentIndex && recordNumber == that.recordNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentIndex, recordNumber);
    }

    @Override
    public String toString() {
        return String.format("RawRecordId{segmentIndex=%d, recordNumber=%d}", segmentIndex, recordNumber);
    }

}
