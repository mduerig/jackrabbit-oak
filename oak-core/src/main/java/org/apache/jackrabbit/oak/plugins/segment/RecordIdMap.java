/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.segment;

import static java.lang.System.arraycopy;
import static java.util.Arrays.binarySearch;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.decode;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.encode;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * A memory optimised map of {@code short} key to {@link RecordId} values.
 * <p>
 * The map doesn't keep references to the actual record ids
 * it contains.
 */
public class RecordIdMap {
    private final SegmentTracker tracker;

    private short[] keys;
    private byte[] values;

    public RecordIdMap(SegmentTracker tracker) {
        this.tracker = tracker;
    }

    private static void putLong(byte[] bytes, int index, long value) {
        bytes[(index)] = (byte) (value >> 56);
        bytes[(index + 1)] = (byte) (value >> 48);
        bytes[(index + 2)] = (byte) (value >> 40);
        bytes[(index + 3)] = (byte) (value >> 32);
        bytes[(index + 4)] = (byte) (value >> 24);
        bytes[(index + 5)] = (byte) (value >> 16);
        bytes[(index + 6)] = (byte) (value >> 8);
        bytes[(index + 7)] = (byte) (value);
    }

    private static long getLong(byte[] bytes, int index) {
        return ((((long) bytes[(index)] & 0xff) << 56) |
                (((long) bytes[(index + 1)] & 0xff) << 48) |
                (((long) bytes[(index + 2)] & 0xff) << 40) |
                (((long) bytes[(index + 3)] & 0xff) << 32) |
                (((long) bytes[(index + 4)] & 0xff) << 24) |
                (((long) bytes[(index + 5)] & 0xff) << 16) |
                (((long) bytes[(index + 6)] & 0xff) <<  8) |
                (((long) bytes[(index + 7)] & 0xff)));
    }

    private static void putShort(byte[] bytes, int index, short value) {
        bytes[(index)] = (byte) (value >> 8);
        bytes[(index + 1)] = (byte) (value);
    }

    private static short getShortValue(byte[] bytes, int index) {
        return (short)((bytes[index] << 8) | (bytes[index + 1] & 0xff));
    }

    /**
     * Associates {@code key} with {@code value} if not already present
     * @param key
     * @param value
     * @return  {@code true} if added, {@code false} if already present
     */
    public boolean put(short key, @Nonnull RecordId value) {
        if (keys == null) {
            keys = new short[1];
            keys[0] = key;
            values = new byte[18];
            putLong(values, 0, value.getSegmentId().getMostSignificantBits());
            putLong(values, 8, value.getSegmentId().getLeastSignificantBits());
            putShort(values, 16, encode(value.getOffset()));
            return true;
        } else {
            int k = binarySearch(keys, key);
            if (k < 0) {
                int l = -k - 1;
                short[] newKeys = new short[keys.length + 1];
                byte[] newValues = new byte[(values.length + 18)];
                arraycopy(keys, 0, newKeys, 0, l);
                arraycopy(values, 0, newValues, 0, l * 18);
                newKeys[l] = key;
                putLong(newValues, l * 18, value.getSegmentId().getMostSignificantBits());
                putLong(newValues, l * 18 + 8, value.getSegmentId().getLeastSignificantBits());
                putShort(newValues, l * 18 + 16, encode(value.getOffset()));
                int c = keys.length - l;
                if (c > 0) {
                    arraycopy(keys, l, newKeys, l + 1, c);
                    arraycopy(values, l * 18, newValues, (l + 1) * 18, c * 18);
                }
                keys = newKeys;
                values = newValues;
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Returns the value associated with a given {@code key} or {@code null} if none.
     * @param key  the key to retrieve
     * @return  the value associated with a given {@code key} or {@code null} if none.
     */
    @CheckForNull
    public RecordId get(short key) {
        int k = binarySearch(keys, key);
        if (k >= 0) {
            return getRecordId(k);
        } else {
            return null;
        }
    }

    /**
     * Check whether {@code key} is present is this map.
     * @param key  the key to check for
     * @return  {@code true} iff {@code key} is present.
     */
    public boolean containsKey(short key) {
        return keys != null && binarySearch(keys, key) >= 0;
    }

    /**
     * @return the number of keys in this map
     */
    public int size() {
        return keys.length;
    }

    /**
     * Retrieve the key at a given index. Keys are ordered according
     * the natural ordering of shorts.
     * @param index
     * @return the key at {@code index}
     */
    public short getKey(int index) {
        return keys[index];
    }

    /**
     * Retrieve the value at a given index. Keys are ordered according
     * the natural ordering of shorts.
     * @param index
     * @return the value at {@code index}
     */
    @Nonnull
    public RecordId getRecordId(int index) {
        return new RecordId(
            new SegmentId(tracker, getLong(values, index * 18), getLong(values, index * 18 + 8)),
            decode(getShortValue(values, index * 18 + 16)));
    }
}
