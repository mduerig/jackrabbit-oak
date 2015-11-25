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

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Arrays.sort;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.segment.MapRecord.SIZE_BITS;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.BLOCK;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.BRANCH;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.BUCKET;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.LEAF;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.LIST;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.NODE;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.TEMPLATE;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.VALUE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.SMALL_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class RecordWriters {
    private RecordWriters() {}

    /**
     * Base class for all record writers
     */
    public static abstract class RecordWriter<T> {
        private final RecordType type;
        protected final int size;
        protected final Collection<RecordId> ids;

        protected RecordWriter(RecordType type, int size,
                Collection<RecordId> ids) {
            this.type = type;
            this.size = size;
            this.ids = ids;
        }

        protected RecordWriter(RecordType type, int size, RecordId id) {
            this(type, size, singleton(id));
        }

        protected RecordWriter(RecordType type, int size) {
            this(type, size, Collections.<RecordId> emptyList());
        }

        public final T write(SegmentBuilder builder) {
            RecordId id = builder.prepare(type, size, ids);
            return writeRecordContent(id, builder);
        }

        protected abstract T writeRecordContent(RecordId id,
                SegmentBuilder builder);
    }

    /**
     * Map Leaf record writer.
     * @see RecordType#LEAF
     */
    public static class MapLeafWriter extends RecordWriter<MapRecord> {

        private final int level;
        private final Collection<MapEntry> entries;

        protected MapLeafWriter() {
            super(LEAF, 4);
            this.level = -1;
            this.entries = null;
        }

        protected MapLeafWriter(int level, Collection<MapEntry> entries) {
            super(LEAF, 4 + entries.size() * 4, extractIds(entries));
            this.level = level;
            this.entries = entries;
        }

        private static List<RecordId> extractIds(Collection<MapEntry> entries) {
            List<RecordId> ids = newArrayListWithCapacity(2 * entries.size());
            for (MapEntry entry : entries) {
                ids.add(entry.getKey());
                ids.add(entry.getValue());
            }
            return ids;
        }

        @Override
        protected MapRecord writeRecordContent(RecordId id,
                SegmentBuilder builder) {
            if (entries != null) {
                int size = entries.size();
                builder.writeInt((level << SIZE_BITS) | size);

                // copy the entries to an array so we can sort them before
                // writing
                final MapEntry[] array = entries.toArray(new MapEntry[size]);
                sort(array);

                for (MapEntry entry : array) {
                    builder.writeInt(entry.getHash());
                }
                for (MapEntry entry : array) {
                    builder.writeRecordId(entry.getKey());
                    builder.writeRecordId(entry.getValue());
                }
            } else {
                builder.writeInt(0);
            }
            return new MapRecord(id);
        }
    }

    /**
     * Map Branch record writer.
     * @see RecordType#BRANCH
     */
    public static class MapBranchWriter extends RecordWriter<MapRecord> {

        private final int level;
        private final int bitmap;

        protected MapBranchWriter(int level, int bitmap, List<RecordId> ids) {
            super(BRANCH, 8, ids);
            this.level = level;
            this.bitmap = bitmap;
        }

        @Override
        protected MapRecord writeRecordContent(RecordId id,
                SegmentBuilder builder) {
            builder.writeInt(level);
            builder.writeInt(bitmap);
            for (RecordId buckedId : ids) {
                builder.writeRecordId(buckedId);
            }
            return new MapRecord(id);
        }
    }

    /**
     * List record writer.
     * @see RecordType#LIST
     */
    public static class ListWriter extends RecordWriter<RecordId> {

        private final int count;
        private final RecordId lid;

        protected ListWriter() {
            super(LIST, 4);
            count = 0;
            lid = null;
        }

        protected ListWriter(int count, RecordId lid) {
            super(LIST, 4, lid);
            this.count = count;
            this.lid = lid;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBuilder builder) {
            builder.writeInt(count);
            if (lid != null) {
                builder.writeRecordId(lid);
            }
            return id;
        }
    }

    /**
     * List Bucket record writer.
     * 
     * @see RecordType#BUCKET
     */
    public static class ListBucketWriter extends RecordWriter<RecordId> {

        protected ListBucketWriter(List<RecordId> ids) {
            super(BUCKET, 0, ids);
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBuilder builder) {
            for (RecordId bucketId : ids) {
                builder.writeRecordId(bucketId);
            }
            return id;
        }
    }

    /**
     * Block record writer.
     * @see SegmentWriter#writeBlock
     * @see RecordType#BLOCK
     */
    public static class BlockWriter extends RecordWriter<RecordId> {

        private final byte[] bytes;
        private final int offset;

        protected BlockWriter(byte[] bytes, int offset, int length) {
            super(BLOCK, length);
            this.bytes = bytes;
            this.offset = offset;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBuilder builder) {
            builder.writeBytes(bytes, offset, size);
            return id;
        }
    }

    /**
     * Single RecordId record writer.
     * @see SegmentWriter#writeValueRecord
     * @see RecordType#VALUE
     */
    public static class SingleValueWriter extends RecordWriter<RecordId> {

        private final RecordId rid;
        private final long len;

        protected SingleValueWriter(RecordId rid, long len) {
            super(VALUE, 8, rid);
            this.rid = rid;
            this.len = len;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBuilder builder) {
            builder.writeLong(len);
            builder.writeRecordId(rid);
            return id;
        }
    }

    /**
     * Bye array record writer. Used as a special case for short binaries (up to
     * about {@code Segment#MEDIUM_LIMIT}): store them directly as small or
     * medium-sized value records.
     * @see SegmentWriter#writeValueRecord
     * @see Segment#MEDIUM_LIMIT
     * @see RecordType#VALUE
     */
    public static class ByteValueWriter extends RecordWriter<RecordId> {

        private final int length;
        private final byte[] data;

        protected ByteValueWriter(int length, byte[] data) {
            super(VALUE, length + getSizeDelta(length));
            this.length = length;
            this.data = data;
        }

        private static boolean isSmallSize(int length) {
            return length < SMALL_LIMIT;
        }

        private static int getSizeDelta(int length) {
            if (isSmallSize(length)) {
                return 1;
            } else {
                return 2;
            }
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBuilder builder) {
            if (isSmallSize(length)) {
                builder.writeByte((byte) length);
            } else {
                builder.writeShort((short) ((length - SMALL_LIMIT) | 0x8000));
            }
            builder.writeBytes(data, 0, length);
            return id;
        }
    }

    /**
     * Large Blob record writer. A blob ID is considered large if the length of
     * its binary representation is equal to or greater than
     * {@code Segment#BLOB_ID_SMALL_LIMIT}.
     * @see SegmentWriter#writeLargeBlobId
     * @see Segment#BLOB_ID_SMALL_LIMIT
     * @see RecordType#VALUE
     */
    public static class LargeBlobIdWriter extends RecordWriter<RecordId> {

        private final RecordId stringRecord;

        protected LargeBlobIdWriter(RecordId stringRecord) {
            super(VALUE, 1, stringRecord);
            this.stringRecord = stringRecord;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBuilder builder) {
            // The length uses a fake "length" field that is always equal to
            // 0xF0.
            // This allows the code to take apart small from a large blob IDs.
            builder.writeByte((byte) 0xF0);
            builder.writeRecordId(stringRecord);
            builder.addBlobRef(id);
            return id;
        }
    }

    /**
     * Small Blob record writer. A blob ID is considered small if the length of
     * its binary representation is less than
     * {@code Segment#BLOB_ID_SMALL_LIMIT}.
     * @see SegmentWriter#writeSmallBlobId
     * @see Segment#BLOB_ID_SMALL_LIMIT
     * @see RecordType#VALUE
     */
    public static class SmallBlobIdWriter extends RecordWriter<RecordId> {

        private final byte[] blobId;

        protected SmallBlobIdWriter(byte[] blobId) {
            super(VALUE, 2 + blobId.length);
            this.blobId = blobId;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBuilder builder) {
            int length = blobId.length;
            builder.writeShort((short) (length | 0xE000));
            builder.writeBytes(blobId, 0, length);
            builder.addBlobRef(id);
            return id;
        }
    }

    /**
     * Template record writer.
     * @see RecordType#TEMPLATE
     */
    public static class TemplateWriter extends RecordWriter<RecordId> {

        private final RecordId[] propertyNames;
        private final byte[] propertyTypes;
        private final int finalHead;
        private final RecordId finalPrimaryId;
        private final List<RecordId> finalMixinIds;
        private final RecordId finalChildNameId;
        private final RecordId finalPropNamesId;
        private final SegmentVersion version;

        protected TemplateWriter(Collection<RecordId> ids,
                final RecordId[] propertyNames, final byte[] propertyTypes,
                final int finalHead, final RecordId finalPrimaryId,
                final List<RecordId> finalMixinIds,
                final RecordId finalChildNameId,
                final RecordId finalPropNamesId, SegmentVersion version) {
            super(TEMPLATE, 4 + propertyTypes.length, ids);
            this.propertyNames = propertyNames;
            this.propertyTypes = propertyTypes;
            this.finalHead = finalHead;
            this.finalPrimaryId = finalPrimaryId;
            this.finalMixinIds = finalMixinIds;
            this.finalChildNameId = finalChildNameId;
            this.finalPropNamesId = finalPropNamesId;
            this.version = version;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBuilder builder) {
            builder.writeInt(finalHead);
            if (finalPrimaryId != null) {
                builder.writeRecordId(finalPrimaryId);
            }
            if (finalMixinIds != null) {
                for (RecordId mixinId : finalMixinIds) {
                    builder.writeRecordId(mixinId);
                }
            }
            if (finalChildNameId != null) {
                builder.writeRecordId(finalChildNameId);
            }
            if (version.onOrAfter(V_11)) {
                if (finalPropNamesId != null) {
                    builder.writeRecordId(finalPropNamesId);
                }
            }
            for (int i = 0; i < propertyNames.length; i++) {
                if (!version.onOrAfter(V_11)) {
                    // V10 only
                    builder.writeRecordId(propertyNames[i]);
                }
                builder.writeByte(propertyTypes[i]);
            }
            return id;
        }
    }

    /**
     * Node State record writer.
     * @see RecordType#NODE
     */
    public static class NodeStateWriter extends RecordWriter<SegmentNodeState> {

        protected NodeStateWriter(List<RecordId> ids) {
            super(NODE, 0, ids);
        }

        @Override
        protected SegmentNodeState writeRecordContent(RecordId id,
                SegmentBuilder builder) {
            for (RecordId recordId : ids) {
                builder.writeRecordId(recordId);
            }
            return new SegmentNodeState(id);
        }
    }

}
