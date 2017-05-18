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

import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_MARKER_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LENGTH_MARKER_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LIMIT;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Charsets;

public abstract class RawRecordWriter {

    protected abstract ByteBuffer addRecord(int number, int type, int requestedSize, Set<UUID> references);

    private static int lengthSize(int length) {
        if (length < SMALL_LIMIT) {
            return SMALL_LENGTH_SIZE;
        }
        if (length < MEDIUM_LIMIT) {
            return MEDIUM_LENGTH_SIZE;
        }
        throw new IllegalArgumentException("invalid length: " + length);
    }

    private static ByteBuffer writeLength(ByteBuffer buffer, int length) {
        if (length < SMALL_LIMIT) {
            return buffer.put((byte) (length & ~SMALL_LENGTH_MARKER_MASK));
        }
        if (length < MEDIUM_LIMIT) {
            return buffer.putShort((short) ((length & ~MEDIUM_LENGTH_MARKER_MASK) | MEDIUM_LENGTH_MARKER));
        }
        throw new IllegalArgumentException("invalid length: " + length);
    }

    private boolean write(int number, int type, byte[] data) {
        ByteBuffer buffer = addRecord(number, type, data.length + lengthSize(data.length), null);
        if (buffer == null) {
            return false;
        }
        writeLength(buffer, data.length).put(data);
        return true;
    }

    public boolean write(int number, int type, String s) {
        return write(number, type, s.getBytes(Charsets.UTF_8));
    }

}
