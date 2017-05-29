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

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Allows access to the data contained in a segment.
 */
public interface SegmentAccess {

    /**
     * The identifier of this segment.
     *
     * @return An instance of {@link UUID}.
     */
    UUID id();

    /**
     * Returns the version of this segment.
     *
     * @return A positive integer.
     */
    int version();

    /**
     * Returns the generation of this segment.
     *
     * @return A positive integer.
     */
    int generation();

    /**
     * Returns the number of references to other segments in this segment.
     *
     * @return A positive integer.
     */
    int segmentReferenceCount();

    /**
     * Returns the the i-th reference to another segment from this segment.
     *
     * @param i An integer greater than or equal to zero 0 and strictly less
     *          than {@link #segmentReferenceCount()}.
     * @return An instance of {@link UUID}.
     */
    UUID segmentReference(int i);

    /**
     * Returns the number of records in this segment.
     *
     * @return A positive integer.
     */
    int recordCount();

    /**
     * Returns the i-th record entry from this segment.
     *
     * @param i An integer greater than or equal to zero and strictly less than
     *          {@link #recordCount()}.
     * @return An instance of {@link Record}.
     */
    Record recordEntry(int i);

    /**
     * Return the value of the record with the provided record number from this
     * segment.
     *
     * @param number The record number.
     * @param size   The size of the record.
     * @return An instance of {@link ByteBuffer} or {@code null} if the record
     * with the provided record number can't be found.
     */
    ByteBuffer recordValue(int number, int size);

}
