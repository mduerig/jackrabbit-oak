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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.fill;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.segment.CacheWeights.OBJECT_HEADER_SIZE;
import static org.apache.jackrabbit.oak.segment.RecordNumbers.EMPTY_RECORD_NUMBERS;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.isValid;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;
import org.apache.commons.io.HexDump;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;

/**
 * A list of records.
 * <p>
 * Record data is not kept in memory, but some entries are cached (templates,
 * all strings in the segment).
 * <p>
 * This class includes method to read records from the raw bytes.
 */
public class Segment {

    static final int HEADER_SIZE = 32;

    /**
     * Size of a line in the table of references to external segments.
     */
    static final int SEGMENT_REFERENCE_SIZE = 16;

    /**
     * Size of a line in the table mapping record numbers to their type and
     * offset.
     */
    static final int RECORD_SIZE = 9;

    /**
     * Number of bytes used for storing a record identifier. Two bytes
     * are used for identifying the segment and four for the record offset
     * within that segment.
     */
    static final int RECORD_ID_BYTES = 2 + 4;

    /**
     * The limit on segment references within one segment. Since record
     * identifiers use one byte to indicate the referenced segment, a single
     * segment can hold references to up to 255 segments plus itself.
     */
    static final int SEGMENT_REFERENCE_LIMIT = (1 << 8) - 1; // 255

    /**
     * The number of bytes (or bits of address space) to use for the
     * alignment boundary of segment records.
     */
    static final int RECORD_ALIGN_BITS = 2; // align at the four-byte boundary

    /**
     * Maximum segment size. Record identifiers are stored as three-byte
     * sequences with the first byte indicating the segment and the next
     * two the offset within that segment. Since all records are aligned
     * at four-byte boundaries, the two bytes can address up to 256kB of
     * record data.
     */
    static final int MAX_SEGMENT_SIZE = 1 << (16 + RECORD_ALIGN_BITS); // 256kB

    /**
     * The size limit for small values. The variable length of small values
     * is encoded as a single byte with the high bit as zero, which gives us
     * seven bits for encoding the length of the value.
     */
    static final int SMALL_LIMIT = 1 << 7;

    /**
     * The size limit for medium values. The variable length of medium values
     * is encoded as two bytes with the highest bits of the first byte set to
     * one and zero, which gives us 14 bits for encoding the length of the
     * value. And since small values are never stored as medium ones, we can
     * extend the size range to cover that many longer values.
     */
    static final int MEDIUM_LIMIT = (1 << (16 - 2)) + SMALL_LIMIT;

    /**
     * Maximum size of small blob IDs. A small blob ID is stored in a value
     * record whose length field contains the pattern "1110" in its most
     * significant bits. Since two bytes are used to store both the bit pattern
     * and the actual length of the blob ID, a maximum of 2^12 values can be
     * stored in the length field.
     */
    static final int BLOB_ID_SMALL_LIMIT = 1 << 12;

    static final int GC_GENERATION_OFFSET = 10;

    static final int REFERENCED_SEGMENT_ID_COUNT_OFFSET = 14;

    static final int RECORD_NUMBER_COUNT_OFFSET = 18;

    @Nonnull
    private final SegmentId id;

    @Nonnull
    private final ByteBuffer data;

    /**
     * Version of the segment storage format.
     */
    @Nonnull
    private final SegmentVersion version;

    /**
     * The table translating record numbers to offsets.
     */
    private final RecordNumbers recordNumbers;

    /**
     * The table translating references to segment IDs.
     */
    private final SegmentReferences segmentReferences;

    private final RecordReader recordReader;

    private Segment(@Nonnull SegmentIdProvider idProvider,
            @Nonnull SegmentReader reader,
            @Nonnull final SegmentId id,
            @Nonnull final ByteBuffer data
    ) {
        this.id = checkNotNull(id);
        this.data = checkNotNull(data);
        if (id.isDataSegmentId()) {
            byte segmentVersion = data.get(3);
            checkState(data.get(0) == '0'
                    && data.get(1) == 'a'
                    && data.get(2) == 'K'
                    && isValid(segmentVersion),
                new Object() {  // Defer evaluation of error message
                    @Override
                    public String toString() {
                        return "Invalid segment format. Dumping segment " + id + "\n"
                            + toHex(data.array());
                    }
            });
            this.version = SegmentVersion.fromByte(segmentVersion);
            this.recordNumbers = readRecordNumberOffsets();
            this.segmentReferences = readReferencedSegments(idProvider);
        } else {
            this.version = LATEST_VERSION;
            this.recordNumbers = new IdentityRecordNumbers();
            this.segmentReferences = new IllegalSegmentReferences();
        }
        this.recordReader = new RecordReader(id, data, reader, recordNumbers, segmentReferences);
    }

    private static String toHex(byte[] bytes) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            HexDump.dump(bytes, 0, out, 0);
            return out.toString(Charsets.UTF_8.name());
        } catch (IOException e) {
            return "Error dumping segment: " + e.getMessage();
        } finally {
            closeQuietly(out);
        }
    }

    public static Segment newSegment(
            @Nonnull SegmentIdProvider idProvider,
            @Nonnull SegmentReader reader,
            @Nonnull final SegmentId id,
            @Nonnull final ByteBuffer data
    ) {
        return new Segment(idProvider, reader, id, data);
    }

    /**
     * Read the serialized table mapping record numbers to offsets.
     *
     * @return An instance of {@link RecordNumbers}, never {@code null}.
     */
    private RecordNumbers readRecordNumberOffsets() {
        int recordNumberCount = getRecordNumberCount();
        if (recordNumberCount == 0) {
            return EMPTY_RECORD_NUMBERS;
        }

        int position = HEADER_SIZE + data.position()
                + getReferencedSegmentIdCount() * SEGMENT_REFERENCE_SIZE;
        int maxIndex = data.getInt(position + (recordNumberCount - 1) * 9);

        byte[] types = new byte[maxIndex + 1];
        int[] offsets = new int[maxIndex + 1];
        fill(offsets, -1);

        for (int i = 0; i < recordNumberCount; i++) {
            int recordNumber = data.getInt(position);
            position += 4;
            types[recordNumber] = data.get(position);
            position += 1;
            offsets[recordNumber] = data.getInt(position);
            position += 4;
        }

        return new ImmutableRecordNumbers(offsets, types);
    }

    private SegmentReferences readReferencedSegments(
            final SegmentIdProvider idProvider) {
        checkState(getReferencedSegmentIdCount() + 1 < 0xffff,
                "Segment cannot have more than 0xffff references");

        final int referencedSegmentIdCount = getReferencedSegmentIdCount();
        final int refOffset = data.position() + HEADER_SIZE;

        // We need to keep SegmentId references (as opposed to e.g. UUIDs)
        // here as frequently resolving the segment ids via the segment id
        // tables is prohibitively expensive.
        // These SegmentId references are not a problem wrt. heap usage as
        // their individual memoised references to their underlying segment
        // is managed via the SegmentCache. It is the size of that cache that
        // keeps overall heap usage by Segment instances bounded.
        // See OAK-6106.
        final SegmentId[] refIds = new SegmentId[referencedSegmentIdCount];
        return new SegmentReferences() {
            @Override
            public SegmentId getSegmentId(int reference) {
                checkArgument(reference <= referencedSegmentIdCount, "Segment reference out of bounds");
                SegmentId id = refIds[reference - 1];
                if (id == null) {
                    synchronized(refIds) {
                        id = refIds[reference - 1];
                        if (id == null) {
                            int position = refOffset + (reference - 1) * SEGMENT_REFERENCE_SIZE;
                            long msb = data.getLong(position);
                            long lsb = data.getLong(position + 8);
                            id = idProvider.newSegmentId(msb, lsb);
                            refIds[reference - 1] = id;
                        }
                    }
                }
                return id;
            }

            @Nonnull
            @Override
            public Iterator<SegmentId> iterator() {
                return new AbstractIterator<SegmentId>() {
                    private int reference = 1;
                    @Override
                    protected SegmentId computeNext() {
                        if (reference <= referencedSegmentIdCount) {
                            return getSegmentId(reference++);
                        } else {
                            return endOfData();
                        }
                    }
                };
            }
        };
    }

    Segment(@Nonnull SegmentId id,
            @Nonnull SegmentReader reader,
            @Nonnull byte[] buffer,
            @Nonnull RecordNumbers recordNumbers,
            @Nonnull SegmentReferences segmentReferences,
            @Nonnull String info
    ) {
        this.id = checkNotNull(id);
        this.info = checkNotNull(info);
        this.data = ByteBuffer.wrap(checkNotNull(buffer));
        this.version = SegmentVersion.fromByte(buffer[3]);
        this.recordNumbers = recordNumbers;
        this.segmentReferences = segmentReferences;
        this.recordReader = new RecordReader(id, data, reader, recordNumbers, segmentReferences);
        id.loaded(this);
    }

    public SegmentVersion getSegmentVersion() {
        return version;
    }

    public SegmentId getSegmentId() {
        return id;
    }

    public int getReferencedSegmentIdCount() {
        return data.getInt(REFERENCED_SEGMENT_ID_COUNT_OFFSET);
    }

    private int getRecordNumberCount() {
        return data.getInt(RECORD_NUMBER_COUNT_OFFSET);
    }

    public UUID getReferencedSegmentId(int index) {
        return segmentReferences.getSegmentId(index + 1).asUUID();
    }

    /**
     * Determine the gc generation a segment from its data. Note that bulk segments don't have
     * generations (i.e. stay at 0).
     *
     * @param data         the date of the segment
     * @param segmentId    the id of the segment
     * @return  the gc generation of this segment or 0 if this is bulk segment.
     */
    public static int getGcGeneration(ByteBuffer data, UUID segmentId) {
        return isDataSegmentId(segmentId.getLeastSignificantBits())
            ? data.getInt(GC_GENERATION_OFFSET)
            : 0;
    }

    /**
     * Determine the gc generation of this segment. Note that bulk segments don't have
     * generations (i.e. stay at 0).
     * @return  the gc generation of this segment or 0 if this is bulk segment.
     */
    public int getGcGeneration() {
        return getGcGeneration(data, id.asUUID());
    }

    private volatile String info;

    /**
     * Returns the segment meta data of this segment or {@code null} if none is present.
     * <p>
     * The segment meta data is a string of the format {@code "{wid=W,sno=S,gc=G,t=T}"}
     * where:
     * <ul>
     * <li>{@code W} is the writer id {@code wid}, </li>
     * <li>{@code S} is a unique, increasing sequence number corresponding to the allocation order
     * of the segments in this store, </li>
     * <li>{@code G} is the garbage collection generation (i.e. the number of compaction cycles
     * that have been run),</li>
     * <li>{@code T} is a time stamp according to {@link System#currentTimeMillis()}.</li>
     * </ul>
     * @return the segment meta data
     */
    @CheckForNull
    String getSegmentInfo() {
        if (info == null && id.isDataSegmentId()) {
            info = readString(recordNumbers.iterator().next().getRecordNumber());
        }
        return info;
    }

    public int size() {
        return data.remaining();
    }

    private RecordReader getRecordReader() {
        return recordReader;
    }

    byte readByte(int recordNumber) {
        return getRecordReader().readByte(recordNumber);
    }

    byte readByte(int recordNumber, int offset) {
        return getRecordReader().readByte(recordNumber, offset);
    }

    short readShort(int recordNumber) {
        return getRecordReader().readShort(recordNumber);
    }

    int readInt(int recordNumber) {
        return getRecordReader().readInt(recordNumber);
    }

    int readInt(int recordNumber, int offset) {
        return getRecordReader().readInt(recordNumber, offset);
    }

    long readLong(int recordNumber) {
        return getRecordReader().readLong(recordNumber);
    }

    void readBytes(int recordNumber, int position, byte[] buffer, int offset, int length) {
        getRecordReader().readBytes(recordNumber, position, buffer, offset, length);
    }

    ByteBuffer readBytes(int recordNumber, int position, int length) {
        return getRecordReader().readBytes(recordNumber, position, length);
    }

    @Nonnull
    RecordId readRecordId(int recordNumber, int rawOffset, int recordIdOffset) {
        return getRecordReader().readRecordId(recordNumber, rawOffset, recordIdOffset);
    }

    RecordId readRecordId(int recordNumber, int rawOffset) {
        return getRecordReader().readRecordId(recordNumber, rawOffset);
    }

    RecordId readRecordId(int recordNumber) {
        return getRecordReader().readRecordId(recordNumber);
    }

    @Nonnull
    String readString(int offset) {
        return getRecordReader().readString(offset);
    }

    @Nonnull
    Template readTemplate(int recordNumber) {
        return getRecordReader().readTemplate(recordNumber);
    }

    static long readLength(RecordId id) {
        return id.getSegment().readLength(id.getRecordNumber());
    }

    long readLength(int recordNumber) {
        return getRecordReader().readLength(recordNumber);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringWriter string = new StringWriter();
        try (PrintWriter writer = new PrintWriter(string)) {
            int length = data.remaining();

            writer.format("Segment %s (%d bytes)%n", id, length);
            String segmentInfo = getSegmentInfo();
            if (segmentInfo != null) {
                writer.format("Info: %s, Generation: %d%n", segmentInfo, getGcGeneration());
            }
            if (id.isDataSegmentId()) {
                writer.println("--------------------------------------------------------------------------");

                int i = 1;

                for (SegmentId segmentId : segmentReferences) {
                    writer.format("reference %02x: %s%n", i++, segmentId);
                }

                for (Entry entry : recordNumbers) {
                    writer.format("%10s record %08x: %08x%n",
                            entry.getType(), entry.getRecordNumber(), entry.getOffset());
                }
            }
            writer.println("--------------------------------------------------------------------------");
            int pos = data.limit() - ((length + 15) & ~15);
            while (pos < data.limit()) {
                writer.format("%04x: ", (MAX_SEGMENT_SIZE - data.limit() + pos) >> RECORD_ALIGN_BITS);
                for (int i = 0; i < 16; i++) {
                    if (i > 0 && i % 4 == 0) {
                        writer.append(' ');
                    }
                    if (pos + i >= data.position()) {
                        byte b = data.get(pos + i);
                        writer.format("%02x ", b & 0xff);
                    } else {
                        writer.append("   ");
                    }
                }
                writer.append(' ');
                for (int i = 0; i < 16; i++) {
                    if (pos + i >= data.position()) {
                        byte b = data.get(pos + i);
                        if (b >= ' ' && b < 127) {
                            writer.append((char) b);
                        } else {
                            writer.append('.');
                        }
                    } else {
                        writer.append(' ');
                    }
                }
                writer.println();
                pos += 16;
            }
            writer.println("--------------------------------------------------------------------------");
            return string.toString();
        }
    }

    public void writeTo(OutputStream stream) throws IOException {
        ByteBuffer buffer = data.duplicate();
        WritableByteChannel channel = Channels.newChannel(stream);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /**
     * A consumer of record data.
     */
    public interface RecordConsumer {

        /**
         * Consume data about a record.
         *
         * @param number the record number.
         * @param type   the record type.
         * @param offset the offset where the record is stored.
         */
        void consume(int number, RecordType type, int offset);

    }

    /**
     * Iterate over the records contained in this segment.
     *
     * @param consumer an instance of {@link RecordConsumer}.
     */
    public void forEachRecord(RecordConsumer consumer) {
        for (Entry entry : recordNumbers) {
            consumer.consume(entry.getRecordNumber(), entry.getType(), entry.getOffset());
        }
    }

    /**
     * Estimate of how much memory this instance would occupy in the segment
     * cache.
     */
    int estimateMemoryUsage() {
        int size = OBJECT_HEADER_SIZE + 76;
        size += 56; // 7 refs x 8 bytes

        if (id.isDataSegmentId()) {
            int recordNumberCount = getRecordNumberCount();
            size += 5 * recordNumberCount;

            int referencedSegmentIdCount = getReferencedSegmentIdCount();
            size += 8 * referencedSegmentIdCount;

            size += StringUtils.estimateMemoryUsage(info);
        }
        if (!data.isDirect()) {
            // seems to over report by 100+ bytes
            size += size();
        }
        size += id.estimateMemoryUsage();
        return size;
    }
}
