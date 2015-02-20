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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newConcurrentMap;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

/**
 * A list of records.
 * <p>
 * Record data is not kept in memory, but some entries are cached (templates,
 * all strings in the segment).
 * <p>
 * This class includes method to read records from the raw bytes.
 */
public class Segment {

    /**
     * Version of the segment storage format.
     * <ul>
     *     <li>10 = all Oak versions released so far</li>
     * </ul>
     */
    public static final byte STORAGE_FORMAT_VERSION = 10;

    /**
     * Number of bytes used for storing a record identifier. One byte
     * is used for identifying the segment and two for the record offset
     * within that segment.
     */
    static final int RECORD_ID_BYTES = 1 + 2;

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
    public static final int RECORD_ALIGN_BITS = 2; // align at the four-byte boundary

    /**
     * Maximum segment size. Record identifiers are stored as three-byte
     * sequences with the first byte indicating the segment and the next
     * two the offset within that segment. Since all records are aligned
     * at four-byte boundaries, the two bytes can address up to 256kB of
     * record data.
     */
    public static final int MAX_SEGMENT_SIZE = 1 << (16 + RECORD_ALIGN_BITS); // 256kB

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
    public static final int MEDIUM_LIMIT = (1 << (16 - 2)) + SMALL_LIMIT;

    public static int REF_COUNT_OFFSET = 5;

    static int ROOT_COUNT_OFFSET = 6;

    static int BLOBREF_COUNT_OFFSET = 8;

    private final SegmentTracker tracker;

    private final SegmentId id;

    private final ByteBuffer data;

    /**
     * Referenced segment identifiers. Entries are initialized lazily in
     * {@link #getRefId(int)}. Set to {@code null} for bulk segments.
     */
    private final SegmentId[] refids;

    /**
     * String records read from segment. Used to avoid duplicate
     * copies and repeated parsing of the same strings.
     */
    private final ConcurrentMap<Integer, String> strings = newConcurrentMap();

    /**
     * Template records read from segment. Used to avoid duplicate
     * copies and repeated parsing of the same templates.
     */
    private final ConcurrentMap<Integer, Template> templates = newConcurrentMap();

    private volatile long accessed = 0;

    public Segment(SegmentTracker tracker, SegmentId id, ByteBuffer data) {
        this.tracker = checkNotNull(tracker);
        this.id = checkNotNull(id);
        this.data = checkNotNull(data);

        if (id.isDataSegmentId()) {
            checkState(data.get(0) == '0'
                    && data.get(1) == 'a'
                    && data.get(2) == 'K'
                    && data.get(3) == STORAGE_FORMAT_VERSION);
            this.refids = new SegmentId[getRefCount()];
            refids[0] = id;
        } else {
            this.refids = null;
        }
    }

    Segment(SegmentTracker tracker, byte[] buffer) {
        this.tracker = checkNotNull(tracker);
        this.id = tracker.newDataSegmentId();
        this.data = ByteBuffer.wrap(checkNotNull(buffer));

        this.refids = new SegmentId[SEGMENT_REFERENCE_LIMIT + 1];
        refids[0] = id;
    }

    void access() {
        accessed++;
    }

    boolean accessed() {
        accessed >>>= 1;
        return accessed != 0;
    }

    private int mapOffset(int offset) {
        // michid implement mapOffset to take rewritten segments into account
        int pos = data.limit() - MAX_SEGMENT_SIZE + offset;
        checkState(pos >= data.position());
        return pos;
    }

    public class Reader {  // michid move to Page
        private final RecordId id;
        private final int pos0;

        private int pos;

        public Reader(RecordId id, int offset) {
            this.id = id;
            this.pos0 = mapOffset(id.getOffset()) + offset;
            this.pos = pos0;
        }

        public Reader skip(int n) {  // michid remove
            pos += n;
            return this;
        }

        private int consume(int n) {
            int p = pos;
            pos += n;
            return p;
        }

        public RecordId readRecordId() {
            return Segment.this.readRecordId(consume(RECORD_ID_BYTES));
        }

        public int readInt() {
            return Segment.this.readInt(consume(4));
        }

        public byte readByte() {
            return Segment.this.readByte(consume(1));
        }

        public short readShort() {
            return Segment.this.readShort(consume(2));
        }

        public long readLong() {
            return Segment.this.readLong(consume(8));
        }

        public void readBytes(byte[] buffer, int offset, int length) {
            Segment.this.readBytes(consume(length), buffer, offset, length);
        }

        public RecordId readRecordId(int offset, int ids) {
            return Segment.this.readRecordId(pos0 + offset + ids * RECORD_ID_BYTES);
        }

        public RecordId readRecordId(int offset) {
            return Segment.this.readRecordId(pos0 + offset);
        }

        public int readInt(int offset) {
            return Segment.this.readInt(pos0 + offset);
        }

        public Page readPage() {
            return null; // michid implement readPage
        }
    }

    public static Reader createReader(RecordId id) {
        return id.getSegment().getReader(id);
    }

    public Reader getReader(RecordId id) {
        return getReader(id, 0);
    }

    public Reader getReader(RecordId id, int offset) {
        return new Reader(id, offset);
    }

    public SegmentId getSegmentId() {
        return id;
    }

    int getRefCount() {
        return (data.get(REF_COUNT_OFFSET) & 0xff) + 1;
    }

    public int getRootCount() {
        return data.getShort(ROOT_COUNT_OFFSET) & 0xffff;
    }

    public RecordType getRootType(int index) {
        int refCount = getRefCount();
        checkArgument(index < getRootCount());
        return RecordType.values()[data.get(data.position() + refCount * 16 + index * 3) & 0xff];
    }

    public int getRootOffset(int index) {
        int refCount = getRefCount();
        checkArgument(index < getRootCount());
        return (data.getShort(data.position() + refCount * 16 + index * 3 + 1) & 0xffff)
                << RECORD_ALIGN_BITS;
    }

    SegmentId getRefId(int index) {
        if (refids == null || index >= refids.length) {
            String type = "data";
            if (!id.isDataSegmentId()) {
                type = "bulk";
            }
            long delta = System.currentTimeMillis() - id.getCreationTime();
            throw new IllegalStateException("RefId '" + index
                    + "' doesn't exist in " + type + " segment " + id
                    + ". Creation date delta is " + delta + " ms.");
        }
        SegmentId refid = refids[index];
        if (refid == null) {
            synchronized (this) {
                refid = refids[index];
                if (refid == null) {
                    int refpos = data.position() + index * 16;
                    long msb = data.getLong(refpos);
                    long lsb = data.getLong(refpos + 8);
                    refid = tracker.getSegmentId(msb, lsb);
                    refids[index] = refid;
                }
            }
        }
        return refid;
    }

    public List<SegmentId> getReferencedIds() {
        int refcount = getRefCount();
        List<SegmentId> ids = newArrayListWithCapacity(refcount);
        for (int refid = 0; refid < refcount; refid++) {
            ids.add(getRefId(refid));
        }
        return ids;
    }

    public int size() {
        return data.remaining();
    }

    public long getCacheSize() {
        int size = 1024;
        if (!data.isDirect()) {
            size += size();
        }
        if (id.isDataSegmentId()) {
            size += size();
        }
        return size;
    }

    /**
     * Writes this segment to the given output stream.
     *
     * @param stream stream to which this segment will be written
     * @throws IOException on an IO error
     */
    public void writeTo(OutputStream stream) throws IOException {
        ByteBuffer buffer = data.duplicate();
        WritableByteChannel channel = Channels.newChannel(stream);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    void collectBlobReferences(ReferenceCollector collector) {
        int refcount = getRefCount();
        int rootcount = data.getShort(data.position() + ROOT_COUNT_OFFSET) & 0xffff;
        int blobrefcount = data.getShort(data.position() + BLOBREF_COUNT_OFFSET) & 0xffff;
        int blobrefpos = data.position() + refcount * 16 + rootcount * 3;

        for (int i = 0; i < blobrefcount; i++) {
            int offset = (data.getShort(blobrefpos + i * 2) & 0xffff) << 2;
            SegmentBlob blob = new SegmentBlob(new RecordId(id, offset));
            collector.addReference(blob.getBlobId());
        }
    }

    private byte readByte(int pos) {
        return data.get(pos);
    }

    private short readShort(int pos) {
        return data.getShort(pos);
    }

    private int readInt(int pos) {
        return data.getInt(pos);
    }

    private long readLong(int pos) {
        return data.getLong(pos);
    }

    /**
     * Reads the given number of bytes starting from the given offset
     * in this segment.
     *
     * @param pos offset within segment
     * @param buffer target buffer
     * @param offset offset within target buffer
     * @param length number of bytes to read
     */
    private void readBytes(int pos, byte[] buffer, int offset, int length) {
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);
        ByteBuffer d = data.duplicate();
        d.position(pos);
        d.get(buffer, offset, length);
    }

    private RecordId readRecordId(int pos) {
        SegmentId refid = getRefId(data.get(pos) & 0xff);
        int offset = ((data.get(pos + 1) & 0xff) << 8) | (data.get(pos + 2) & 0xff);
        return new RecordId(refid, offset << RECORD_ALIGN_BITS);
    }

    private String readString(int pos) {
        String string = strings.get(pos);
        if (string == null) {
            string = loadString(pos);
            strings.putIfAbsent(pos, string); // only keep the first copy
        }
        return string;
    }

    private String loadString(int pos) {
        long length = readLength(pos);
        if (length < SMALL_LIMIT) {
            byte[] bytes = new byte[(int) length];
            ByteBuffer buffer = data.duplicate();
            buffer.position(pos + 1);
            buffer.get(bytes);
            return new String(bytes, Charsets.UTF_8);
        } else if (length < MEDIUM_LIMIT) {
            byte[] bytes = new byte[(int) length];
            ByteBuffer buffer = data.duplicate();
            buffer.position(pos + 2);
            buffer.get(bytes);
            return new String(bytes, Charsets.UTF_8);
        } else if (length < Integer.MAX_VALUE) {
            int size = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list =
                    new ListRecord(readRecordId(pos + 8), size);
            SegmentStream stream = new SegmentStream(
                    new RecordId(id, pos), list, length);
            try {
                return stream.getString();
            } finally {
                stream.close();
            }
        } else {
            throw new IllegalStateException("String is too long: " + length);
        }
    }

    private static MapRecord readMap(RecordId id) {
        return new MapRecord(id);
    }

    private static Template readTemplate(RecordId id) {
        Segment segment = id.getSegment();
        int pos = segment.mapOffset(id.getOffset());
        return segment.readTemplate(pos);
    }

    private Template readTemplate(int pos) {
        Template template = templates.get(pos);
        if (template == null) {
            template = loadTemplate(pos);
            templates.putIfAbsent(pos, template); // only keep the first copy
        }
        return template;
    }

    private Template loadTemplate(int pos) {
        int head = readInt(pos);
        boolean hasPrimaryType = (head & (1 << 31)) != 0;
        boolean hasMixinTypes = (head & (1 << 30)) != 0;
        boolean zeroChildNodes = (head & (1 << 29)) != 0;
        boolean manyChildNodes = (head & (1 << 28)) != 0;
        int mixinCount = (head >> 18) & ((1 << 10) - 1);
        int propertyCount = head & ((1 << 18) - 1);
        pos += 4;

        PropertyState primaryType = null;
        if (hasPrimaryType) {
            RecordId primaryId = readRecordId(pos);
            primaryType = PropertyStates.createProperty(
                    "jcr:primaryType", readString(primaryId), Type.NAME);
            pos += RECORD_ID_BYTES;
        }

        PropertyState mixinTypes = null;
        if (hasMixinTypes) {
            String[] mixins = new String[mixinCount];
            for (int i = 0; i < mixins.length; i++) {
                RecordId mixinId = readRecordId(pos);
                mixins[i] =  readString(mixinId);
                pos += RECORD_ID_BYTES;
            }
            mixinTypes = PropertyStates.createProperty(
                    "jcr:mixinTypes", Arrays.asList(mixins), Type.NAMES);
        }

        String childName = Template.ZERO_CHILD_NODES;
        if (manyChildNodes) {
            childName = Template.MANY_CHILD_NODES;
        } else if (!zeroChildNodes) {
            RecordId childNameId = readRecordId(pos);
            childName = readString(childNameId);
            pos += RECORD_ID_BYTES;
        }

        PropertyTemplate[] properties =
                new PropertyTemplate[propertyCount];
        for (int i = 0; i < properties.length; i++) {
            RecordId propertyNameId = readRecordId(pos);
            pos += RECORD_ID_BYTES;
            byte type = readByte(pos++);
            properties[i] = new PropertyTemplate(
                    i, readString(propertyNameId),
                    Type.fromTag(Math.abs(type), type < 0));
        }

        return new Template(
                primaryType, mixinTypes, properties, childName);
    }

    private static String readString(RecordId id) {  // michid remove
        Segment segment = id.getSegment();
        int pos = segment.mapOffset(id.getOffset());
        return segment.readString(pos);
    }

    private static long readLength(RecordId id) {  // michid remove
        Segment segment = id.getSegment();
        int pos = segment.mapOffset(id.getOffset());
        return segment.readLength(pos);
    }

    private long readLength(int pos) {
        int length = data.get(pos++) & 0xff;
        if ((length & 0x80) == 0) {
            return length;
        } else if ((length & 0x40) == 0) {
            return ((length & 0x3f) << 8
                    | data.get(pos++) & 0xff)
                    + SMALL_LIMIT;
        } else {
            return (((long) length & 0x3f) << 56
                    | ((long) (data.get(pos++) & 0xff)) << 48
                    | ((long) (data.get(pos++) & 0xff)) << 40
                    | ((long) (data.get(pos++) & 0xff)) << 32
                    | ((long) (data.get(pos++) & 0xff)) << 24
                    | ((long) (data.get(pos++) & 0xff)) << 16
                    | ((long) (data.get(pos++) & 0xff)) << 8
                    | ((long) (data.get(pos++) & 0xff)))
                    + MEDIUM_LIMIT;
        }
    }

    //------------------------------------------------------------< Object >--

    @Override  // michid fix toString -> remap offsets
    public String toString() {
        StringWriter string = new StringWriter();
        PrintWriter writer = new PrintWriter(string);

        int length = data.remaining();

        writer.format("Segment %s (%d bytes)%n", id, length);
        if (id.isDataSegmentId()) {
            writer.println("--------------------------------------------------------------------------");
            int refcount = getRefCount();
            for (int refid = 0; refid < refcount; refid++) {
                writer.format("reference %02x: %s%n", refid, getRefId(refid));
            }
            int rootcount = data.getShort(ROOT_COUNT_OFFSET) & 0xffff;
            int pos = data.position() + refcount * 16;
            for (int rootid = 0; rootid < rootcount; rootid++) {
                writer.format(
                            "root %d: %s at %04x%n", rootid,
                        RecordType.values()[data.get(pos + rootid * 3) & 0xff],
                            data.getShort(pos + rootid * 3 + 1) & 0xffff);
            }
            int blobrefcount = data.getShort(BLOBREF_COUNT_OFFSET) & 0xffff;
            pos += rootcount * 3;
            for (int blobrefid = 0; blobrefid < blobrefcount; blobrefid++) {
                int offset = data.getShort(pos + blobrefid * 2) & 0xffff;
                SegmentBlob blob = new SegmentBlob(
                        new RecordId(id, offset << RECORD_ALIGN_BITS));
                writer.format(
                        "blobref %d: %s at %04x%n", blobrefid,
                        blob.getBlobId(), offset);
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

        writer.close();
        return string.toString();
    }

}
