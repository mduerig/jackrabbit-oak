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

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;

import org.apache.jackrabbit.oak.plugins.segment.Segment.Reader;

/**
 * A record of type "BLOCK".
 */
class BlockRecord implements Writable {
    private final Record record;
    private final int size;

    BlockRecord(RecordId id, int size) {
        this.record = Record.getRecord(checkNotNull(id), this);
        this.size = size;
    }

    /**
     * Reads bytes from this block. Up to the given number of bytes are
     * read starting from the given position within this block. The number
     * of bytes read is returned.
     *
     * @param position position within this block
     * @param buffer target buffer
     * @param offset offset within the target buffer
     * @param length maximum number of bytes to read
     * @return number of bytes that could be read
     */
    public int read(int position, byte[] buffer, int offset, int length) {
        checkElementIndex(position, size);
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);

        if (position + length > size) {
            length = size - position;
        }
        if (length > 0) {
            Reader reader = record.getReader(position);
            reader.readBytes(buffer, offset, length);
        }
        return length;
    }

    @Override
    public RecordId writeTo(SegmentWriter writer) {
        byte[] bytes = new byte[size];
        read(0, bytes, 0, size);
        return writer.writeBlock(bytes, 0, size);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else {
            return object instanceof BlockRecord &&
                Record.fastEquals(record, ((BlockRecord) object).record);
        }
    }

    @Override
    public int hashCode() {
        return record.hashCode() ^ size;
    }

    @Override
    public String toString() {
        return record.toString();
    }
}
