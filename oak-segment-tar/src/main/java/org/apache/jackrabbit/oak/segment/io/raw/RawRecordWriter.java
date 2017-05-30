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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_BLOB_ID_LENGTH;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_BLOB_ID_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_DELTA;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.LONG_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MAP_BRANCH_BITMAP_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MAP_HEADER_SIZE_BITS;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MAP_LEAF_EMPTY_HEADER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MAP_LEAF_HASH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_DELTA;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_BLOB_ID_LENGTH_MARKER;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_BLOB_ID_LENGTH_MASK;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_BLOB_ID_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LENGTH_SIZE;
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.SMALL_LIMIT;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public final class RawRecordWriter {

    public interface RecordAdder {

        ByteBuffer addRecord(int number, int type, int requestedSize, Set<UUID> references);

    }

    public interface SegmentReferenceReader {

        int readSegmentReference(UUID segmentId);

    }

    public static RawRecordWriter of(SegmentReferenceReader segmentReferenceReader, RecordAdder recordAdder) {
        return new RawRecordWriter(checkNotNull(segmentReferenceReader), checkNotNull(recordAdder));
    }

    private final SegmentReferenceReader segmentReferenceReader;

    private final RecordAdder recordAdder;

    private RawRecordWriter(SegmentReferenceReader segmentReferenceReader, RecordAdder recordAdder) {
        this.segmentReferenceReader = segmentReferenceReader;
        this.recordAdder = recordAdder;
    }

    private ByteBuffer addRecord(int number, int type, int requestedSize, Set<UUID> references) {
        return recordAdder.addRecord(number, type, requestedSize, references);
    }

    private short segmentReference(UUID segmentId) {
        return (short) (segmentReferenceReader.readSegmentReference(segmentId) & 0xffff);
    }

    public boolean writeValue(int number, int type, byte[] data, int offset, int length) {
        if (length < SMALL_LIMIT) {
            return writeSmallValue(number, type, data, offset, length);
        }
        if (length < MEDIUM_LIMIT) {
            return writeMediumValue(number, type, data, offset, length);
        }
        throw new IllegalArgumentException("invalid length: " + length);
    }

    private boolean writeSmallValue(int number, int type, byte[] data, int offset, int length) {
        ByteBuffer buffer = addRecord(number, type, length + SMALL_LENGTH_SIZE, null);
        if (buffer == null) {
            return false;
        }
        buffer.put((byte) length).put(data, offset, length);
        return true;
    }

    private boolean writeMediumValue(int number, int type, byte[] data, int offset, int length) {
        ByteBuffer buffer = addRecord(number, type, length + MEDIUM_LENGTH_SIZE, null);
        if (buffer == null) {
            return false;
        }
        buffer.putShort(mediumLength(length)).put(data, offset, length);
        return true;
    }

    private static short mediumLength(int length) {
        return (short) (((length - MEDIUM_LENGTH_DELTA) & MEDIUM_LENGTH_MASK) | MEDIUM_LENGTH_MARKER);
    }

    public boolean writeValue(int number, int type, RawRecordId id, long length) {
        ByteBuffer buffer = addRecord(number, type, LONG_LENGTH_SIZE + RawRecordId.BYTES, singleton(id.getSegmentId()));
        if (buffer == null) {
            return false;
        }
        buffer.putLong(longLength(length))
                .putShort(segmentReference(id.getSegmentId()))
                .putInt(id.getRecordNumber());
        return true;
    }

    private static long longLength(long length) {
        return ((length - LONG_LENGTH_DELTA) & LONG_LENGTH_MASK) | LONG_LENGTH_MARKER;
    }

    public boolean writeBlobId(int number, int type, byte[] blobId) {
        ByteBuffer buffer = addRecord(number, type, SMALL_BLOB_ID_LENGTH_SIZE + blobId.length, null);
        if (buffer == null) {
            return false;
        }
        buffer.putShort(smallBlobIdLength(blobId.length)).put(blobId);
        return true;
    }

    private static short smallBlobIdLength(int length) {
        return (short) ((length & SMALL_BLOB_ID_LENGTH_MASK) | SMALL_BLOB_ID_LENGTH_MARKER);
    }

    public boolean writeBlobId(int number, int type, RawRecordId id) {
        ByteBuffer buffer = addRecord(number, type, LONG_BLOB_ID_LENGTH_SIZE + RawRecordId.BYTES, null);
        if (buffer == null) {
            return false;
        }
        buffer.put(LONG_BLOB_ID_LENGTH)
                .putShort(segmentReference(id.getSegmentId()))
                .putInt(id.getRecordNumber());
        return true;
    }

    public boolean writeBlock(int number, int type, byte[] data, int offset, int length) {
        ByteBuffer buffer = addRecord(number, type, length, null);
        if (buffer == null) {
            return false;
        }
        buffer.put(data, offset, length);
        return true;
    }

    public boolean writeMapLeaf(int number, int type) {
        ByteBuffer buffer = addRecord(number, type, RawRecordConstants.MAP_HEADER_SIZE, null);
        if (buffer == null) {
            return false;
        }
        buffer.putInt(MAP_LEAF_EMPTY_HEADER);
        return true;
    }

    public boolean writeMapLeaf(int number, int type, RawMapLeaf leaf) {
        ByteBuffer buffer = addRecord(number, type, RawRecordConstants.MAP_HEADER_SIZE + leaf.getEntries().size() * (MAP_LEAF_HASH_SIZE + 2 * RawRecordId.BYTES), mapLeafReferences(leaf));
        if (buffer == null) {
            return false;
        }
        buffer.putInt(mapHeader(leaf.getLevel(), leaf.getEntries().size()));
        for (RawMapEntry entry : leaf.getEntries()) {
            buffer.putInt(entry.getHash());
        }
        for (RawMapEntry entry : leaf.getEntries()) {
            buffer.putShort(segmentReference(entry.getKey().getSegmentId()));
            buffer.putInt(entry.getKey().getRecordNumber());
            buffer.putShort(segmentReference(entry.getValue().getSegmentId()));
            buffer.putInt(entry.getValue().getRecordNumber());
        }
        return true;
    }

    private static Set<UUID> mapLeafReferences(RawMapLeaf leaf) {
        Set<UUID> refs = null;
        for (RawMapEntry entry : leaf.getEntries()) {
            if (refs == null) {
                refs = new HashSet<>();
            }
            refs.add(entry.getKey().getSegmentId());
            refs.add(entry.getValue().getSegmentId());
        }
        return refs;
    }

    private static int mapHeader(int level, int size) {
        return (level << MAP_HEADER_SIZE_BITS) | size;
    }

    public boolean writeMapBranch(int number, int type, RawMapBranch branch) {
        ByteBuffer buffer = addRecord(number, type, MAP_BRANCH_BITMAP_SIZE + RawRecordConstants.MAP_HEADER_SIZE + branch.getReferences().size() * RawRecordId.BYTES, mapBranchReferences(branch));
        if (buffer == null) {
            return false;
        }
        buffer.putInt(mapHeader(branch.getLevel(), branch.getCount()));
        buffer.putInt(branch.getBitmap());
        for (RawRecordId reference : branch.getReferences()) {
            buffer.putShort(segmentReference(reference.getSegmentId()));
            buffer.putInt(reference.getRecordNumber());
        }
        return true;
    }

    private static Set<UUID> mapBranchReferences(RawMapBranch branch) {
        Set<UUID> refs = null;
        for (RawRecordId reference : branch.getReferences()) {
            if (refs == null) {
                refs = new HashSet<>();
            }
            refs.add(reference.getSegmentId());
        }
        return refs;
    }

    public boolean writeListBucket(int number, int type, List<RawRecordId> list) {
        ByteBuffer buffer = addRecord(number, type, RawRecordId.BYTES * list.size(), listBucketReferences(list));
        if (buffer == null) {
            return false;
        }
        for (RawRecordId id : list) {
            buffer.putShort(segmentReference(id.getSegmentId()));
            buffer.putInt(id.getRecordNumber());
        }
        return true;
    }

    private static Set<UUID> listBucketReferences(List<RawRecordId> list) {
        Set<UUID> refs = null;
        for (RawRecordId id : list) {
            if (refs == null) {
                refs = new HashSet<>();
            }
            refs.add(id.getSegmentId());
        }
        return refs;
    }

    public boolean writeList(int number, int type, RawList list) {
        ByteBuffer buffer = addRecord(number, type, listSize(list), listReferences(list));
        if (buffer == null) {
            return false;
        }
        buffer.putInt(list.getSize());
        if (list.getBucket() != null) {
            buffer.putShort(segmentReference(list.getBucket().getSegmentId()));
            buffer.putInt(list.getBucket().getRecordNumber());
        }
        return true;
    }

    private static int listSize(RawList list) {
        int size = Integer.BYTES;
        if (list.getBucket() != null) {
            size += RawRecordId.BYTES;
        }
        return size;
    }

    private static Set<UUID> listReferences(RawList list) {
        if (list.getBucket() == null) {
            return null;
        }
        return singleton(list.getBucket().getSegmentId());
    }

}
