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

interface Constants {

    int HEADER_SIZE = 32;

    int VERSION_OFFSET = 3;

    int GENERATION_OFFSET = 10;

    int SEGMENT_REFERENCES_COUNT_OFFSET = 14;

    int RECORDS_COUNT_OFFSET = 18;

    int SEGMENT_REFERENCE_SIZE = 16;

    int RECORD_REFERENCE_SIZE = 9;

    /**
     * The number of bytes (or bits of address space) to use for the
     * alignment boundary of segment records.
     */
    int RECORD_ALIGN_BITS = 2;

    /**
     * Maximum segment size. Record identifiers are stored as three-byte
     * sequences with the first byte indicating the segment and the next
     * two the offset within that segment. Since all records are aligned
     * at four-byte boundaries, the two bytes can address up to 256kB of
     * record data.
     */
    int MAX_SEGMENT_SIZE = 1 << (16 + RECORD_ALIGN_BITS);

}
