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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static com.google.common.collect.Lists.partition;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.io.ByteStreams.read;
import static com.google.common.io.Closeables.close;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.segment.MapRecord.BUCKETS_PER_LEVEL;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newBlobIdWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newBlockWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newListBucketWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newListWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newMapBranchWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newMapLeafWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newNodeStateWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newTemplateWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newValueWriter;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.align;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.readString;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.plugins.segment.RecordWriters.RecordWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts nodes, properties, and values to records, which are written to segments.
 */
public class SegmentWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentWriter.class);

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    private final SegmentBufferWriterPool segmentBufferWriterPool;

    private static final int STRING_RECORDS_CACHE_SIZE = Integer.getInteger(
            "oak.segment.writer.stringsCacheSize", 15000);

    /**
     * Cache of recently stored string records, used to avoid storing duplicates
     * of frequently occurring data.
     */
    final Map<String, RecordId> stringCache = newItemsCache(
            STRING_RECORDS_CACHE_SIZE);

    private static final int TPL_RECORDS_CACHE_SIZE = Integer.getInteger(
            "oak.segment.writer.templatesCacheSize", 3000);

    /**
     * Cache of recently stored template records, used to avoid storing
     * duplicates of frequently occurring data.
     */
    final Map<Template, RecordId> templateCache = newItemsCache(TPL_RECORDS_CACHE_SIZE);

    private static final <T> Map<T, RecordId> newItemsCache(final int size) {
        final boolean disabled = true;  // michid re-enable caches but make generation part of cache key to avoid backrefs
        final int safeSize = size <= 0 ? 0 : (int) (size * 1.2);
        return new LinkedHashMap<T, RecordId>(safeSize, 0.9f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<T, RecordId> e) {
                return size() > size;
            }
            @Override
            public synchronized RecordId get(Object key) {
                if (disabled) {
                    return null;
                }
                return super.get(key);
            }
            @Override
            public synchronized RecordId put(T key, RecordId value) {
                if (disabled) {
                    return null;
                }
                return super.put(key, value);
            }
            @Override
            public synchronized void clear() {
                super.clear();
            }
        };
    }

    private final SegmentStore store;

    /**
     * Version of the segment storage format.
     */
    private final SegmentVersion version;

    /**
     * @param store     store to write to
     * @param version   segment version to write
     * @param wid       id of this writer
     */
    public SegmentWriter(SegmentStore store, SegmentVersion version, String wid) {
        this.store = store;
        this.version = version;
        this.segmentBufferWriterPool = new SegmentBufferWriterPool(store, version, wid);
    }

    public void flush() throws IOException {
        segmentBufferWriterPool.flush();
    }

    public void dropCache() {
        stringCache.clear();
        templateCache.clear();
    }

    MapRecord writeMap(MapRecord base, Map<String, RecordId> changes) throws IOException {
        SegmentBufferWriter writer = segmentBufferWriterPool.borrowWriter(currentThread());
        try {
            return writeMap(base, changes, writer);
        } finally {
            segmentBufferWriterPool.returnWriter(currentThread(), writer);
        }
    }

    private MapRecord writeMap(MapRecord base, Map<String, RecordId> changes, SegmentBufferWriter writer)
            throws IOException {
        if (base != null && base.isDiff()) {
            Segment segment = base.getSegment();
            RecordId key = segment.readRecordId(base.getOffset(8));
            String name = readString(key);
            if (!changes.containsKey(name)) {
                changes.put(name, segment.readRecordId(base.getOffset(8, 1)));
            }
            base = new MapRecord(segment.readRecordId(base.getOffset(8, 2)));
        }

        if (base != null && changes.size() == 1) {
            Map.Entry<String, RecordId> change =
                changes.entrySet().iterator().next();
            RecordId value = change.getValue();
            if (value != null) {
                MapEntry entry = base.getEntry(change.getKey());
                if (entry != null) {
                    if (value.equals(entry.getValue())) {
                        return base;
                    } else {
                        return writeMapRecord(newMapBranchWriter(entry.getHash(),
                                asList(entry.getKey(), value, base.getRecordId())), writer);
                    }
                }
            }
        }

        List<MapEntry> entries = newArrayList();
        for (Map.Entry<String, RecordId> entry : changes.entrySet()) {
            String key = entry.getKey();

            RecordId keyId = null;
            if (base != null) {
                MapEntry e = base.getEntry(key);
                if (e != null) {
                    keyId = e.getKey();
                }
            }
            if (keyId == null && entry.getValue() != null) {
                keyId = writeString(key, writer);
            }

            if (keyId != null) {
                entries.add(new MapEntry(key, keyId, entry.getValue()));
            }
        }
        return writeMapBucket(base, entries, 0, writer);
    }

    private static MapRecord writeMapLeaf(int level, Collection<MapEntry> entries, SegmentBufferWriter writer)
            throws IOException {
        checkNotNull(entries);
        int size = entries.size();
        checkElementIndex(size, MapRecord.MAX_SIZE);
        checkPositionIndex(level, MapRecord.MAX_NUMBER_OF_LEVELS);
        checkArgument(size != 0 || level == MapRecord.MAX_NUMBER_OF_LEVELS);
        return writeMapRecord(newMapLeafWriter(level, entries), writer);
    }

    private static MapRecord writeMapBranch(int level, int size, MapRecord[] buckets, SegmentBufferWriter writer)
            throws IOException {
        int bitmap = 0;
        List<RecordId> bucketIds = newArrayListWithCapacity(buckets.length);
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] != null) {
                bitmap |= 1L << i;
                bucketIds.add(buckets[i].getRecordId());
            }
        }
        return writeMapRecord(newMapBranchWriter(level, size, bitmap, bucketIds), writer);
    }

    private static MapRecord writeMapBucket(MapRecord base, Collection<MapEntry> entries, int level,
        SegmentBufferWriter writer) throws IOException {
        // when no changed entries, return the base map (if any) as-is
        if (entries == null || entries.isEmpty()) {
            if (base != null) {
                return base;
            } else if (level == 0) {
                return writeMapRecord(newMapLeafWriter(), writer);
            } else {
                return null;
            }
        }

        // when no base map was given, write a fresh new map
        if (base == null) {
            // use leaf records for small maps or the last map level
            if (entries.size() <= BUCKETS_PER_LEVEL
                    || level == MapRecord.MAX_NUMBER_OF_LEVELS) {
                return writeMapLeaf(level, entries, writer);
            }

            // write a large map by dividing the entries into buckets
            MapRecord[] buckets = new MapRecord[BUCKETS_PER_LEVEL];
            List<List<MapEntry>> changes = splitToBuckets(entries, level);
            for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
                buckets[i] = writeMapBucket(null, changes.get(i), level + 1, writer);
            }

            // combine the buckets into one big map
            return writeMapBranch(level, entries.size(), buckets, writer);
        }

        // if the base map is small, update in memory and write as a new map
        if (base.isLeaf()) {
            Map<String, MapEntry> map = newHashMap();
            for (MapEntry entry : base.getEntries()) {
                map.put(entry.getName(), entry);
            }
            for (MapEntry entry : entries) {
                if (entry.getValue() != null) {
                    map.put(entry.getName(), entry);
                } else {
                    map.remove(entry.getName());
                }
            }
            return writeMapBucket(null, map.values(), level, writer);
        }

        // finally, the if the base map is large, handle updates per bucket
        int newSize = 0;
        int newCount = 0;
        MapRecord[] buckets = base.getBuckets();
        List<List<MapEntry>> changes = splitToBuckets(entries, level);
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            buckets[i] = writeMapBucket(buckets[i], changes.get(i), level + 1, writer);
            if (buckets[i] != null) {
                newSize += buckets[i].size();
                newCount++;
            }
        }

        // OAK-654: what if the updated map is smaller?
        if (newSize > BUCKETS_PER_LEVEL) {
            return writeMapBranch(level, newSize, buckets, writer);
        } else if (newCount <= 1) {
            // up to one bucket contains entries, so return that as the new map
            for (MapRecord bucket : buckets) {
                if (bucket != null) {
                    return bucket;
                }
            }
            // no buckets remaining, return empty map
            return writeMapBucket(null, null, level, writer);
        } else {
            // combine all remaining entries into a leaf record
            List<MapEntry> list = newArrayList();
            for (MapRecord bucket : buckets) {
                if (bucket != null) {
                    addAll(list, bucket.getEntries());
                }
            }
            return writeMapLeaf(level, list, writer);
        }
    }

    public RecordId writeList(List<RecordId> list) throws IOException {
        SegmentBufferWriter writer = segmentBufferWriterPool.borrowWriter(currentThread());
        try {
            return writeList(list, writer);
        } finally {
            segmentBufferWriterPool.returnWriter(currentThread(), writer);
        }
    }

    /**
     * Writes a list record containing the given list of record identifiers.
     *
     * @param list list of record identifiers
     * @return list record identifier
     */
    private static RecordId writeList(List<RecordId> list, SegmentBufferWriter writer) throws IOException {
        checkNotNull(list);
        checkArgument(!list.isEmpty());
        List<RecordId> thisLevel = list;
        while (thisLevel.size() > 1) {
            List<RecordId> nextLevel = newArrayList();
            for (List<RecordId> bucket :
                partition(thisLevel, ListRecord.LEVEL_SIZE)) {
                if (bucket.size() > 1) {
                    nextLevel.add(writeListBucket(bucket, writer));
                } else {
                    nextLevel.add(bucket.get(0));
                }
            }
            thisLevel = nextLevel;
        }
        return thisLevel.iterator().next();
    }

    private static RecordId writeListBucket(List<RecordId> bucket, SegmentBufferWriter writer) throws IOException {
        checkArgument(bucket.size() > 1);
        return newListBucketWriter(bucket).write(writer);
    }

    private static List<List<MapEntry>> splitToBuckets(Collection<MapEntry> entries, int level) {
        List<MapEntry> empty = null;
        int mask = (1 << MapRecord.BITS_PER_LEVEL) - 1;
        int shift = 32 - (level + 1) * MapRecord.BITS_PER_LEVEL;

        List<List<MapEntry>> buckets =
                newArrayList(nCopies(MapRecord.BUCKETS_PER_LEVEL, empty));
        for (MapEntry entry : entries) {
            int index = (entry.getHash() >> shift) & mask;
            List<MapEntry> bucket = buckets.get(index);
            if (bucket == null) {
                bucket = newArrayList();
                buckets.set(index, bucket);
            }
            bucket.add(entry);
        }
        return buckets;
    }

    private static RecordId writeValueRecord(long length, RecordId blocks, SegmentBufferWriter writer)
            throws IOException {
        long len = (length - Segment.MEDIUM_LIMIT) | (0x3L << 62);
        return newValueWriter(blocks, len).write(writer);
    }

    private static RecordId writeValueRecord(int length, byte[] data, SegmentBufferWriter writer)
            throws IOException {
        checkArgument(length < Segment.MEDIUM_LIMIT);
        return newValueWriter(length, data).write(writer);
    }

    public RecordId writeString(String string) throws IOException {
        SegmentBufferWriter writer = segmentBufferWriterPool.borrowWriter(currentThread());
        try {
            return writeString(string, writer);
        } finally {
            segmentBufferWriterPool.returnWriter(currentThread(), writer);
        }
    }

    /**
     * Writes a string value record.
     *
     * @param string string to be written
     * @return value record identifier
     */
    private RecordId writeString(String string, SegmentBufferWriter writer) throws IOException {
        RecordId id = stringCache.get(string);
        if (id != null) {
            return id; // shortcut if the same string was recently stored
        }

        byte[] data = string.getBytes(UTF_8);

        if (data.length < Segment.MEDIUM_LIMIT) {
            // only cache short strings to avoid excessive memory use
            id = writeValueRecord(data.length, data, writer);
            stringCache.put(string, id);
            return id;
        }

        int pos = 0;
        List<RecordId> blockIds = newArrayListWithExpectedSize(
            data.length / BLOCK_SIZE + 1);

        // write as many full bulk segments as possible
        while (pos + MAX_SEGMENT_SIZE <= data.length) {
            SegmentId bulkId = store.getTracker().newBulkSegmentId();
            store.writeSegment(bulkId, data, pos, MAX_SEGMENT_SIZE);
            for (int i = 0; i < MAX_SEGMENT_SIZE; i += BLOCK_SIZE) {
                blockIds.add(new RecordId(bulkId, i));
            }
            pos += MAX_SEGMENT_SIZE;
        }

        // inline the remaining data as block records
        while (pos < data.length) {
            int len = Math.min(BLOCK_SIZE, data.length - pos);
            blockIds.add(writeBlock(data, pos, len, writer));
            pos += len;
        }

        return writeValueRecord(data.length, writeList(blockIds, writer), writer);
    }

    public SegmentBlob writeBlob(Blob blob) throws IOException {
        SegmentBufferWriter writer = segmentBufferWriterPool.borrowWriter(currentThread());
        try {
            return writeBlob(blob, writer);
        } finally {
            segmentBufferWriterPool.returnWriter(currentThread(), writer);
        }
    }

    private SegmentBlob writeBlob(Blob blob, SegmentBufferWriter writer) throws IOException {
        if (blob instanceof SegmentBlob
            && store.containsSegment(((SegmentBlob) blob).getRecordId().getSegmentId())) {
            return (SegmentBlob) blob;
        }

        String reference = blob.getReference();
        if (reference != null && store.getBlobStore() != null) {
            String blobId = store.getBlobStore().getBlobId(reference);
            if (blobId != null) {
                RecordId id = writeBlobId(blobId, writer);
                return new SegmentBlob(id);
            } else {
                LOG.debug("No blob found for reference {}, inlining...", reference);
            }
        }

        return writeStream(blob.getNewStream(), writer);
    }

    /**
     * Write a reference to an external blob. This method handles blob IDs of
     * every length, but behaves differently for small and large blob IDs.
     *
     * @param blobId Blob ID.
     * @return Record ID pointing to the written blob ID.
     * @see Segment#BLOB_ID_SMALL_LIMIT
     */
    private RecordId writeBlobId(String blobId, SegmentBufferWriter writer) throws IOException {
        byte[] data = blobId.getBytes(UTF_8);
        if (data.length < Segment.BLOB_ID_SMALL_LIMIT) {
            return newBlobIdWriter(data).write(writer);
        } else {
            return newBlobIdWriter(writeString(blobId, writer)).write(writer);
        }
    }

    RecordId writeBlock(byte[] bytes, int offset, int length) throws IOException {
        SegmentBufferWriter writer = segmentBufferWriterPool.borrowWriter(currentThread());
        try {
            return writeBlock(bytes, offset, length, writer);
        } finally {
            segmentBufferWriterPool.returnWriter(currentThread(), writer);
        }
    }

    /**
     * Writes a block record containing the given block of bytes.
     *
     * @param bytes source buffer
     * @param offset offset within the source buffer
     * @param length number of bytes to write
     * @return block record identifier
     */
    private static RecordId writeBlock(byte[] bytes, int offset, int length, SegmentBufferWriter writer) throws IOException {
        checkNotNull(bytes);
        checkPositionIndexes(offset, offset + length, bytes.length);
        return newBlockWriter(bytes, offset, length).write(writer);
    }

    SegmentBlob writeExternalBlob(String blobId) throws IOException {
        SegmentBufferWriter writer = segmentBufferWriterPool.borrowWriter(currentThread());
        try {
            return writeExternalBlob(blobId, writer);
        } finally {
            segmentBufferWriterPool.returnWriter(currentThread(), writer);
        }
    }

    private SegmentBlob writeExternalBlob(String blobId, SegmentBufferWriter writer) throws IOException {
        RecordId id = writeBlobId(blobId, writer);
        return new SegmentBlob(id);
    }

    SegmentBlob writeLargeBlob(long length, List<RecordId> list) throws IOException {
        SegmentBufferWriter writer = segmentBufferWriterPool.borrowWriter(currentThread());
        try {
            return writeLargeBlob(length, list, writer);
        } finally {
            segmentBufferWriterPool.returnWriter(currentThread(), writer);
        }
    }

    private static SegmentBlob writeLargeBlob(long length, List<RecordId> list, SegmentBufferWriter writer)
            throws IOException {
        RecordId id = writeValueRecord(length, writeList(list, writer), writer);
        return new SegmentBlob(id);
    }

    public SegmentBlob writeStream(InputStream stream) throws IOException {
        SegmentBufferWriter writer = segmentBufferWriterPool.borrowWriter(currentThread());
        try {
            return writeStream(stream, writer);
        } finally {
            segmentBufferWriterPool.returnWriter(currentThread(), writer);
        }
    }

    /**
     * Writes a stream value record. The given stream is consumed
     * <em>and closed</em> by this method.
     *
     * @param stream stream to be written
     * @return value record identifier
     * @throws IOException if the stream could not be read
     */
    private SegmentBlob writeStream(InputStream stream, SegmentBufferWriter writer) throws IOException {
        boolean threw = true;
        try {
            RecordId id = SegmentStream.getRecordIdIfAvailable(stream, store);
            if (id == null) {
                id = internalWriteStream(stream, writer);
            }
            threw = false;
            return new SegmentBlob(id);
        } finally {
            close(stream, threw);
        }
    }

    private RecordId internalWriteStream(InputStream stream, SegmentBufferWriter writer)
            throws IOException {
        BlobStore blobStore = store.getBlobStore();
        byte[] data = new byte[Segment.MEDIUM_LIMIT];
        int n = read(stream, data, 0, data.length);

        // Special case for short binaries (up to about 16kB):
        // store them directly as small- or medium-sized value records
        if (n < Segment.MEDIUM_LIMIT) {
            return writeValueRecord(n, data, writer);
        } else if (blobStore != null) {
            String blobId = blobStore.writeBlob(new SequenceInputStream(
                    new ByteArrayInputStream(data, 0, n), stream));
            return writeBlobId(blobId, writer);
        }

        data = Arrays.copyOf(data, MAX_SEGMENT_SIZE);
        n += read(stream, data, n, MAX_SEGMENT_SIZE - n);
        long length = n;
        List<RecordId> blockIds =
                newArrayListWithExpectedSize(2 * n / BLOCK_SIZE);

        // Write the data to bulk segments and collect the list of block ids
        while (n != 0) {
            SegmentId bulkId = store.getTracker().newBulkSegmentId();
            int len = align(n, 1 << Segment.RECORD_ALIGN_BITS);
            LOG.debug("Writing bulk segment {} ({} bytes)", bulkId, n);
            store.writeSegment(bulkId, data, 0, len);

            for (int i = 0; i < n; i += BLOCK_SIZE) {
                blockIds.add(new RecordId(bulkId, data.length - len + i));
            }

            n = read(stream, data, 0, data.length);
            length += n;
        }

        return writeValueRecord(length, writeList(blockIds, writer), writer);
    }

    private RecordId writeProperty(PropertyState state, SegmentBufferWriter writer) throws IOException {
        Map<String, RecordId> previousValues = emptyMap();
        return writeProperty(state, previousValues, writer);
    }

    private RecordId writeProperty(PropertyState state, Map<String, RecordId> previousValues,
            SegmentBufferWriter writer) throws IOException {
        Type<?> type = state.getType();
        int count = state.count();

        List<RecordId> valueIds = newArrayList();
        for (int i = 0; i < count; i++) {
            if (type.tag() == PropertyType.BINARY) {
                try {
                    SegmentBlob blob =
                            writeBlob(state.getValue(BINARY, i), writer);
                    valueIds.add(blob.getRecordId());
                } catch (IOException e) {
                    throw new IllegalStateException("Unexpected IOException", e);
                }
            } else {
                String value = state.getValue(STRING, i);
                RecordId valueId = previousValues.get(value);
                if (valueId == null) {
                    valueId = writeString(value, writer);
                }
                valueIds.add(valueId);
            }
        }

        if (!type.isArray()) {
            return valueIds.iterator().next();
        } else if (count == 0) {
            return newListWriter().write(writer);
        } else {
            return newListWriter(count, writeList(valueIds, writer)).write(writer);
        }
    }

    private RecordId writeTemplate(Template template, SegmentBufferWriter writer) throws IOException {
        checkNotNull(template);

        RecordId id = templateCache.get(template);
        if (id != null) {
            return id; // shortcut if the same template was recently stored
        }

        Collection<RecordId> ids = newArrayList();
        int head = 0;

        RecordId primaryId = null;
        PropertyState primaryType = template.getPrimaryType();
        if (primaryType != null) {
            head |= 1 << 31;
            primaryId = writeString(primaryType.getValue(NAME), writer);
            ids.add(primaryId);
        }

        List<RecordId> mixinIds = null;
        PropertyState mixinTypes = template.getMixinTypes();
        if (mixinTypes != null) {
            head |= 1 << 30;
            mixinIds = newArrayList();
            for (String mixin : mixinTypes.getValue(NAMES)) {
                mixinIds.add(writeString(mixin, writer));
            }
            ids.addAll(mixinIds);
            checkState(mixinIds.size() < (1 << 10));
            head |= mixinIds.size() << 18;
        }

        RecordId childNameId = null;
        String childName = template.getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            head |= 1 << 29;
        } else if (childName == Template.MANY_CHILD_NODES) {
            head |= 1 << 28;
        } else {
            childNameId = writeString(childName, writer);
            ids.add(childNameId);
        }

        PropertyTemplate[] properties = template.getPropertyTemplates();
        RecordId[] propertyNames = new RecordId[properties.length];
        byte[] propertyTypes = new byte[properties.length];
        for (int i = 0; i < properties.length; i++) {
            // Note: if the property names are stored in more than 255 separate
            // segments, this will not work.
            propertyNames[i] = writeString(properties[i].getName(), writer);
            Type<?> type = properties[i].getType();
            if (type.isArray()) {
                propertyTypes[i] = (byte) -type.tag();
            } else {
                propertyTypes[i] = (byte) type.tag();
            }
        }

        RecordId propNamesId = null;
        if (version.onOrAfter(V_11)) {
            if (propertyNames.length > 0) {
                propNamesId = writeList(asList(propertyNames), writer);
                ids.add(propNamesId);
            }
        } else {
            ids.addAll(asList(propertyNames));
        }

        checkState(propertyNames.length < (1 << 18));
        head |= propertyNames.length;

        RecordId tid = newTemplateWriter(ids, propertyNames,
            propertyTypes, head, primaryId, mixinIds, childNameId,
            propNamesId, version).write(writer);
        templateCache.put(template, tid);
        return tid;
    }

    private boolean hasSegment(NodeState node) {
        return (node instanceof SegmentNodeState)
            && store.containsSegment(((SegmentNodeState) node).getRecordId().getSegmentId());
    }

    private boolean hasSegment(PropertyState property) {
        return (property instanceof SegmentPropertyState)
            && store.containsSegment(((SegmentPropertyState) property).getRecordId().getSegmentId());
    }

    // michid defer compacted items are not in the compaction map -> performance regression
    //        split compaction map into 1) id based equality and 2) cache (like string and template) for nodes
    public SegmentNodeState writeNode(NodeState state) throws IOException {
        SegmentBufferWriter writer = segmentBufferWriterPool.borrowWriter(currentThread());
        try {
            return writeNode(state, writer);
        } finally {
            segmentBufferWriterPool.returnWriter(currentThread(), writer);
        }
    }

    private SegmentNodeState writeNode(NodeState state, SegmentBufferWriter writer) throws IOException {
        if (state instanceof SegmentNodeState) {
            SegmentNodeState sns = uncompact((SegmentNodeState) state);
            if (sns != state || hasSegment(sns)) {
                if (!isOldGen(sns.getRecordId(), writer)) {
                    return sns;
                }
            }
        }

        SegmentNodeState before = null;
        Template beforeTemplate = null;
        ModifiedNodeState after = null;
        if (state instanceof ModifiedNodeState) {
            after = (ModifiedNodeState) state;
            NodeState base = after.getBaseState();
            if (base instanceof SegmentNodeState) {
                SegmentNodeState sns = uncompact((SegmentNodeState) base);
                if (sns != base || hasSegment(sns)) {
                    if (!isOldGen(sns.getRecordId(), writer)) {
                        before = sns;
                        beforeTemplate = before.getTemplate();
                    }
                }
            }
        }

        Template template = new Template(state);
        RecordId templateId;
        if (template.equals(beforeTemplate)) {
            templateId = before.getTemplateId();
        } else {
            templateId = writeTemplate(template, writer);
        }

        List<RecordId> ids = newArrayList();
        ids.add(templateId);

        String childName = template.getChildName();
        if (childName == Template.MANY_CHILD_NODES) {
            MapRecord base;
            Map<String, RecordId> childNodes;
            if (before != null
                && before.getChildNodeCount(2) > 1
                && after.getChildNodeCount(2) > 1) {
                base = before.getChildNodeMap();
                childNodes = new ChildNodeCollectorDiff(writer).diff(before, after);
            } else {
                base = null;
                childNodes = newHashMap();
                for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                    childNodes.put(
                        entry.getName(),
                        writeNode(entry.getNodeState(), writer).getRecordId());
                }
            }
            ids.add(writeMap(base, childNodes, writer).getRecordId());
        } else if (childName != Template.ZERO_CHILD_NODES) {
            ids.add(writeNode(state.getChildNode(template.getChildName()), writer).getRecordId());
        }

        List<RecordId> pIds = newArrayList();
        for (PropertyTemplate pt : template.getPropertyTemplates()) {
            String name = pt.getName();
            PropertyState property = state.getProperty(name);

            if (hasSegment(property)) {
                RecordId pid = ((SegmentPropertyState) property).getRecordId();
                if (isOldGen(pid, writer)) {
                    pIds.add(writeProperty(property, writer));
                } else {
                    pIds.add(pid);
                }
            } else if (before == null || !hasSegment(before)) {
                pIds.add(writeProperty(property, writer));
            } else {
                // reuse previously stored property, if possible
                PropertyTemplate bt = beforeTemplate.getPropertyTemplate(name);
                if (bt == null) {
                    pIds.add(writeProperty(property, writer)); // new property
                } else {
                    SegmentPropertyState bp = beforeTemplate.getProperty(before.getRecordId(), bt.getIndex());
                    if (property.equals(bp)) {
                        pIds.add(bp.getRecordId()); // no changes
                    } else if (bp.isArray() && bp.getType() != BINARIES) {
                        // reuse entries from the previous list
                        pIds.add(writeProperty(property, bp.getValueRecords(), writer));
                    } else {
                        pIds.add(writeProperty(property, writer));
                    }
                }
            }
        }

        if (!pIds.isEmpty()) {
            if (version.onOrAfter(V_11)) {
                ids.add(writeList(pIds, writer));
            } else {
                ids.addAll(pIds);
            }
        }
        return writeNodeState(newNodeStateWriter(ids), writer);
    }

    /**
     * If the given node was compacted, return the compacted node, otherwise
     * return the passed node. This is to avoid pointing to old nodes, if they
     * have been compacted.
     *
     * @param state the node
     * @return the compacted node (if it was compacted)
     */
    private SegmentNodeState uncompact(SegmentNodeState state) {
        RecordId id = store.getTracker().getCompactionMap().get(state.getRecordId());
        if (id != null) {
            return new SegmentNodeState(id);
        } else {
            return state;
        }
    }

    private static SegmentNodeState writeNodeState(RecordWriter recordWriter, SegmentBufferWriter writer) throws IOException {
        return new SegmentNodeState(recordWriter.write(writer));
    }

    private static MapRecord writeMapRecord(RecordWriter recordWriter, SegmentBufferWriter writer) throws IOException {
        return new MapRecord(recordWriter.write(writer));
    }

    private class ChildNodeCollectorDiff extends DefaultNodeStateDiff {
        private final Map<String, RecordId> childNodes = newHashMap();
        private final SegmentBufferWriter writer;
        private IOException exception;

        public ChildNodeCollectorDiff(SegmentBufferWriter writer) {
            this.writer = writer;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            try {
                childNodes.put(name, writeNode(after, writer).getRecordId());
            } catch (IOException e) {
                exception = e;
                return false;
            }
            return true;
        }

        @Override
        public boolean childNodeChanged(
            String name, NodeState before, NodeState after) {
            try {
                childNodes.put(name, writeNode(after, writer).getRecordId());
            } catch (IOException e) {
                exception = e;
                return false;
            }
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            childNodes.put(name, null);
            return true;
        }

        public Map<String, RecordId> diff(SegmentNodeState before, ModifiedNodeState after) throws IOException {
            after.compareAgainstBaseState(before, this);
            if (exception != null) {
                throw new IOException(exception);
            } else {
                return childNodes;
            }
        }
    }

    private static boolean isOldGen(RecordId id, SegmentBufferWriter writer) {
        int thatGen = id.getSegment().getGcGen();
        int thisGen = writer.getGeneration();
        return thatGen < thisGen;
    }

}
