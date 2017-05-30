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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.identityHashCode;
import static java.util.Arrays.sort;
import static org.apache.jackrabbit.oak.segment.RecordType.BLOB_ID;
import static org.apache.jackrabbit.oak.segment.RecordType.BLOCK;
import static org.apache.jackrabbit.oak.segment.RecordType.BRANCH;
import static org.apache.jackrabbit.oak.segment.RecordType.BUCKET;
import static org.apache.jackrabbit.oak.segment.RecordType.LEAF;
import static org.apache.jackrabbit.oak.segment.RecordType.LIST;
import static org.apache.jackrabbit.oak.segment.RecordType.VALUE;
import static org.apache.jackrabbit.oak.segment.Segment.RECORD_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.oak.segment.io.raw.RawList;
import org.apache.jackrabbit.oak.segment.io.raw.RawMapBranch;
import org.apache.jackrabbit.oak.segment.io.raw.RawMapEntry;
import org.apache.jackrabbit.oak.segment.io.raw.RawMapLeaf;
import org.apache.jackrabbit.oak.segment.io.raw.RawRecordId;
import org.apache.jackrabbit.oak.segment.io.raw.RawRecordWriter;

/**
 * This class encapsulates the state of a segment being written. It provides
 * methods for writing primitive data types and for pre-allocating buffer space
 * in the current segment. Should the current segment not have enough space left
 * the current segment is flushed and a fresh one is allocated.
 * <p>
 * The common usage pattern is:
 * <pre>
 *    SegmentBufferWriter writer = ...
 *    writer.prepare(...)  // allocate buffer
 *    writer.writeXYZ(...)
 * </pre>
 * The behaviour of this class is undefined should the pre-allocated buffer be
 * overrun be calling any of the write methods.
 * <p>
 * Instances of this class are <em>not thread safe</em>. See also the class
 * comment of {@link SegmentWriter}.
 */
public class SegmentBufferWriter implements WriteOperationHandler {

    /**
     * Enable an extra check logging warnings should this writer create segments
     * referencing segments from an older generation.
     */
    private static final boolean ENABLE_GENERATION_CHECK = Boolean.getBoolean("enable-generation-check");

    private final SegmentStore segmentStore;

    @Nonnull
    private final SegmentIdProvider idProvider;

    @Nonnull
    private final SegmentReader reader;

    /**
     * Id of this writer.
     */
    @Nonnull
    private final String wid;

    private final int generation;

    private Segment segment;

    private int nextRecordNumber;

    private SingleSegmentBufferWriter singleSegmentBufferWriter;

    private volatile RawRecordWriter raw;

    /**
     * Mark this buffer as dirty. A dirty buffer needs to be flushed to disk
     * regularly to avoid data loss.
     */
    private boolean dirty;

    public SegmentBufferWriter(
            SegmentStore segmentStore,
            @Nonnull SegmentIdProvider idProvider,
            @Nonnull SegmentReader reader,
            @CheckForNull String wid,
            int generation
    ) {
        this.segmentStore = segmentStore;
        this.idProvider = checkNotNull(idProvider);
        this.reader = checkNotNull(reader);
        this.generation = generation;
        if (wid == null) {
            this.wid = "w-" + identityHashCode(this);
        } else {
            this.wid = wid;
        }
    }

    @Nonnull
    @Override
    public RecordId execute(@Nonnull WriteOperation writeOperation) throws IOException {
        return writeOperation.execute(this);
    }

    int getGeneration() {
        return generation;
    }

    /**
     * Allocate a new segment and write the segment meta data. The segment meta
     * data is a string of the format {@code "{wid=W,sno=S,t=T}"} where: <ul>
     * <li>{@code W} is the writer id {@code wid}, </li> <li>{@code S} is a
     * unique, increasing sequence number corresponding to the allocation order
     * of the segments in this store, </li> <li>{@code T} is a time stamp
     * according to {@link System#currentTimeMillis()}.</li> </ul> The segment
     * meta data is guaranteed to be the first string record in a segment.
     */
    private void newSegment() throws IOException {
        SegmentId segmentId = idProvider.newDataSegmentId();

        nextRecordNumber = 0;

        singleSegmentBufferWriter = new SingleSegmentBufferWriter(segmentStore, generation, idProvider, segmentId, ENABLE_GENERATION_CHECK);
        raw = RawRecordWriter.of(singleSegmentBufferWriter::readSegmentReference, singleSegmentBufferWriter::addRecord);
        String metaInfo = String.format("{\"wid\":\"%s\",\"sno\":%d,\"t\":%d}", wid, idProvider.getSegmentIdCount(), currentTimeMillis());
        segment = singleSegmentBufferWriter.newSegment(reader, metaInfo);

        byte[] data = metaInfo.getBytes(Charsets.UTF_8);
        writeValue(data.length, data);

        // Trick: writeValue() sets the dirty flag to true, but since we don't
        // want to flush a segment containing only the meta-info and no other
        // record, we set the dirty flag back to false.

        dirty = false;
    }

    public void writeByte(byte value) {
        singleSegmentBufferWriter.writeByte(value);
    }

    public void writeShort(short value) {
        singleSegmentBufferWriter.writeShort(value);
    }

    public void writeInt(int value) {
        singleSegmentBufferWriter.writeInt(value);
    }

    public void writeLong(long value) {
        singleSegmentBufferWriter.writeLong(value);
    }

    /**
     * Write a record id, and marks the record id as referenced (removes it from
     * the unreferenced set).
     *
     * @param recordId the record id
     */
    public void writeRecordId(RecordId recordId) {
        singleSegmentBufferWriter.writeRecordId(recordId);
    }

    /**
     * Write a record ID. Optionally, mark this record ID as being a reference.
     * If a record ID is marked as a reference, the referenced record can't be a
     * root record in this segment.
     *
     * @param recordId  the record ID.
     * @param reference {@code true} if this record ID is a reference, {@code
     *                  false} otherwise.
     */
    public void writeRecordId(RecordId recordId, boolean reference) {
        singleSegmentBufferWriter.writeRecordId(recordId, reference);
    }

    public void writeBytes(byte[] data, int offset, int length) {
        singleSegmentBufferWriter.writeBytes(data, offset, length);
    }

    /**
     * Adds a segment header to the buffer and writes a segment to the segment
     * store. This is done automatically (called from prepare) when there is not
     * enough space for a record. It can also be called explicitly.
     */
    @Override
    public void flush() throws IOException {
        if (singleSegmentBufferWriter == null || !dirty) {
            return;
        }
        singleSegmentBufferWriter.flush();
        newSegment();
    }

    private static Set<UUID> segmentReferences(Collection<RecordId> rids) {
        Set<UUID> references = null;
        for (RecordId rid : rids) {
            if (references == null) {
                references = newHashSet();
            }
            references.add(rid.asUUID());
        }
        return references;
    }

    /**
     * Before writing a record (which are written backwards, from the end of the
     * file to the beginning), this method is called, to ensure there is enough
     * space. A new segment is also created if there is not enough space in the
     * segment lookup table or elsewhere.
     * <p>
     * This method does not actually write into the segment, just allocates the
     * space (flushing the segment if needed and starting a new one), and sets
     * the write position (records are written from the end to the beginning,
     * but within a record from left to right).
     *
     * @param type  the record type (only used for root records)
     * @param size  the size of the record, excluding the size used for the
     *              record ids
     * @param ids   the record ids
     * @return a new record id
     */
    RecordId prepare(RecordType type, int size, Collection<RecordId> ids) throws IOException {
        Set<UUID> references = segmentReferences(ids);
        int recordSize = size + ids.size() * RECORD_SIZE;

        if (segment == null) {
            newSegment();
        }

        int number;

        number = nextRecordNumber++;
        if (addRecord(number, type.ordinal(), recordSize, references) != null) {
            dirty = true;
            return new RecordId(segment.getSegmentId(), number);
        }

        flush();

        number = nextRecordNumber++;
        if (addRecord(number, type.ordinal(), recordSize, references) != null) {
            dirty = true;
            return new RecordId(segment.getSegmentId(), number);
        }

        throw new IllegalArgumentException("the record is too big");
    }

    private ByteBuffer addRecord(int number, int type, int size, Set<UUID> references) {
        return singleSegmentBufferWriter.addRecord(number, type, size, references);
    }

    RecordId writeMapLeaf(int level, Collection<MapEntry> entries) throws IOException {
        MapEntry[] sorted = entries.toArray(new MapEntry[entries.size()]);
        sort(sorted);

        List<RawMapEntry> rawEntries = new ArrayList<>(sorted.length);
        for (MapEntry entry : sorted) {
            rawEntries.add(RawMapEntry.of(
                    entry.getHash(),
                    asRawRecordId(entry.getKey()),
                    asRawRecordId(entry.getValue())
            ));
        }

        return writeMapLeaf(RawMapLeaf.of(level, rawEntries));
    }

    private RecordId writeMapLeaf(RawMapLeaf leaf) throws IOException {
        return writeRecord(LEAF, (n, t) -> raw.writeMapLeaf(n, t, leaf));
    }

    RecordId writeMapLeaf() throws IOException {
        return writeRecord(LEAF, (n, t) -> raw.writeMapLeaf(n, t));
    }

    RecordId writeMapBranch(int level, int entryCount, int bitmap, List<RecordId> ids) throws IOException {
        return writeMapBranch(RawMapBranch.of(level, entryCount, bitmap, asRawRecordIdList(ids)));
    }

    RecordId writeMapBranch(int bitmap, List<RecordId> ids) throws IOException {
        return writeMapBranch(RawMapBranch.of(0, -1, bitmap, asRawRecordIdList(ids)));
    }

    private RecordId writeMapBranch(RawMapBranch branch) throws IOException {
        return writeRecord(BRANCH, (n, t) -> raw.writeMapBranch(n, t, branch));
    }

    RecordId writeList() throws IOException {
        return writeList(RawList.empty());
    }

    RecordId writeList(int count, RecordId lid) throws IOException {
        return writeList(RawList.of(count, asRawRecordId(lid)));
    }

    private RecordId writeList(RawList list) throws IOException {
        return writeRecord(LIST, (n, t) -> raw.writeList(n, t, list));
    }

    RecordId writeListBucket(List<RecordId> ids) throws IOException {
        return writeRawListBucket(asRawRecordIdList(ids));
    }

    private RecordId writeRawListBucket(List<RawRecordId> ids) throws IOException {
        return writeRecord(BUCKET, (n, t) -> raw.writeListBucket(n, t, ids));
    }

    RecordId writeBlock(byte[] bytes, int offset, int length) throws IOException {
        return writeRecord(BLOCK, (n, t) -> raw.writeBlock(n, t, bytes, offset, length));
    }

    RecordId writeValue(RecordId id, long length) throws IOException {
        return writeValue(asRawRecordId(id), length);
    }

    private RecordId writeValue(RawRecordId id, long length) throws IOException {
        return writeRecord(VALUE, (n, t) -> raw.writeValue(n, t, id, length));
    }

    RecordId writeValue(int length, byte[] data) throws IOException {
        return writeRecord(VALUE, (n, t) -> raw.writeValue(n, t, data, 0, length));
    }

    RecordId writeBlobId(RecordId id) throws IOException {
        return writeBlobId(asRawRecordId(id));
    }

    private RecordId writeBlobId(RawRecordId id) throws IOException {
        return writeRecord(BLOB_ID, (n, t) -> raw.writeBlobId(n, t, id));
    }

    RecordId writeBlobId(byte[] id) throws IOException {
        return writeRecord(BLOB_ID, (n, t) -> raw.writeBlobId(n, t, id));
    }

    RecordId writeTemplate(
            Collection<RecordId> ids,
            RecordId[] propertyNames,
            byte[] propertyTypes,
            int head,
            RecordId primaryId,
            List<RecordId> mixinIds,
            RecordId childNameId,
            RecordId propNamesId
    ) throws IOException {
        return RecordWriters.newTemplateWriter(
                ids,
                propertyNames,
                propertyTypes,
                head,
                primaryId,
                mixinIds,
                childNameId,
                propNamesId
        ).write(this);
    }

    private RawRecordId asRawRecordId(RecordId id) {
        return RawRecordId.of(id.getSegmentId().asUUID(), id.getRecordNumber());
    }

    private List<RawRecordId> asRawRecordIdList(List<RecordId> ids) {
        List<RawRecordId> rids = new ArrayList<>(ids.size());
        for (RecordId id : ids) {
            rids.add(asRawRecordId(id));
        }
        return rids;
    }

    private interface RecordContentWriter {

        boolean writeRecordContent(int number, int type);

    }

    private RecordId writeRecord(RecordType type, RecordContentWriter writer) throws IOException {
        return writeRecord(type.ordinal(), writer);
    }

    private RecordId writeRecord(int type, RecordContentWriter writer) throws IOException {
        int number;

        if (raw == null) {
            newSegment();
        }

        number = nextRecordNumber++;
        if (writer.writeRecordContent(number, type)) {
            dirty = true;
            return new RecordId(segment.getSegmentId(), number);
        }

        flush();

        number = nextRecordNumber++;
        if (writer.writeRecordContent(number, type)) {
            dirty = true;
            return new RecordId(segment.getSegmentId(), number);
        }

        throw new IllegalStateException("unable to write record");
    }

}
