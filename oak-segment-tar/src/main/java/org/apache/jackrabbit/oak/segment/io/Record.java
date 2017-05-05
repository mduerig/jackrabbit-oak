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

/**
 * Metadata about a record.
 */
public class Record {

    private final int number;

    private final int type;

    private final int position;

    Record(int number, int type, int position) {
        this.number = number;
        this.type = type;
        this.position = position;
    }

    /**
     * Return the number of this record. The record number is the logical
     * identifier of the record in a segment.
     *
     * @return a positive integer.
     */
    public int number() {
        return number;
    }

    /**
     * Return the type of this record. The type of the record is defined by
     * the user.
     *
     * @return a positive integer.
     */
    public int type() {
        return type;
    }

    /**
     * Return the position of this record. The position is considered from the
     * end of an imaginary segment having size {@link Constants#MAX_SEGMENT_SIZE}.
     * It has to be adapted to the real size fo the segment before it can be
     * used to fetch a record.
     * <p>
     * This field is not visible outside of this package because it represents
     * an implementation detail of how records are stored into a segment. A
     * record must always be addressed by its record number outside of this
     * package.
     *
     * @return a positive integer.
     */
    int offset() {
        return position;
    }

}
