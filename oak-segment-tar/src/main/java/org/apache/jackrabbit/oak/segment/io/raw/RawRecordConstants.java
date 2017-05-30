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

class RawRecordConstants {

    // These constants define the encoding of small lengths. If
    // SMALL_LENGTH_MASK and SMALL_LENGTH_DELTA would be defined, they would
    // have the values 0x7F and 0, respectively. Their usage is implicit when a
    // short length (which needs to be strictly smaller than 0x80) is casted to
    // a byte.

    static final int SMALL_LENGTH_SIZE = Byte.BYTES;

    static final int SMALL_LIMIT = 1 << 7;

    static final int SMALL_LENGTH_MARKER_BYTE_MASK = 0x80;

    static final int SMALL_LENGTH_MARKER_BYTE = 0x00;

    // These constants define the encoding of medium lengths.

    static final int MEDIUM_LIMIT = (1 << (16 - 2)) + SMALL_LIMIT;

    static final int MEDIUM_LENGTH_DELTA = SMALL_LIMIT;

    static final int MEDIUM_LENGTH_SIZE = Short.BYTES;

    static final short MEDIUM_LENGTH_MASK = 0x3FFF;

    static final short MEDIUM_LENGTH_MARKER = (short) 0x8000;

    static final byte MEDIUM_LENGTH_MARKER_BYTE_MASK = (byte) 0xC0;

    static final byte MEDIUM_LENGTH_MARKER_BYTE = (byte) 0x80;

    // These constants define the encoding of long lengths.

    static final long LONG_LENGTH_LIMIT = (1L << (Long.SIZE - 3)) + MEDIUM_LIMIT;

    static final int LONG_LENGTH_DELTA = MEDIUM_LIMIT;

    static final int LONG_LENGTH_SIZE = Long.BYTES;

    static final long LONG_LENGTH_MASK = 0x1FFFFFFFFFFFFFFFL;

    static final long LONG_LENGTH_MARKER = 0xC000000000000000L;

    static final byte LONG_LENGTH_MARKER_BYTE_MASK = (byte) 0xE0;

    static final byte LONG_LENGTH_MARKER_BYTE = (byte) 0xC0;

    // These constants define the encoding of small blob IDs.

    static final int SMALL_BLOB_ID_LENGTH_SIZE = Short.BYTES;

    static final int SMALL_BLOB_ID_LENGTH_MASK = 0x1FFF;

    static final int SMALL_BLOB_ID_LENGTH_MARKER = 0xE000;

    // These constants define the encoding of large blob IDs.

    static final int LONG_BLOB_ID_LENGTH_SIZE = Byte.BYTES;

    static final byte LONG_BLOB_ID_LENGTH = (byte) 0xF0;

    // These constants define the encoding of both map branches and leaves.

    static final int MAP_HEADER_SIZE = Integer.BYTES;

    static final int MAP_HEADER_SIZE_BITS = 28;

    // These constants define the encoding of map leaves.

    static final int MAP_LEAF_HASH_SIZE = Integer.BYTES;

    static final int MAP_LEAF_EMPTY_HEADER = 0;

    // These constants define the encoding of map branches.

    static final int MAP_BRANCH_BITMAP_SIZE = Integer.BYTES;

    RawRecordConstants() {
        // Prevent instantiation
    }

}
