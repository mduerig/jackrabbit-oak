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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static java.util.Collections.emptySet;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.SMALL_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.plugins.segment.Segment.Reader;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * A BLOB (stream of bytes). This is a record of type "VALUE".
 */
public class SegmentBlob implements Writable, Blob {
    private final Record record;

    public static Iterable<SegmentId> getBulkSegmentIds(Blob blob) {
        if (blob instanceof SegmentBlob) {
            return ((SegmentBlob) blob).getBulkSegmentIds();
        } else {
            return emptySet();
        }
    }

    SegmentBlob(RecordId id) {
        this.record = Record.getRecord(checkNotNull(id), this);
    }

    public RecordId getRecordId() {
        return record.getRecordId();
    }

    private InputStream getInlineStream(Reader reader, int length) {
        byte[] inline = new byte[length];
        reader.readBytes(inline, 0, length);
        return new SegmentStream(record.getRecordId(), inline);
    }

    @Override
    public RecordId writeTo(SegmentWriter writer) {
        try {
            return writer.writeBlob(this).getRecordId();
        } catch (IOException e) {
            e.printStackTrace();  // michid implement catch e
            throw new IllegalStateException(e);
        }
    }

    @Override @Nonnull
    public InputStream getNewStream() {
        Reader reader = record.getReader();
        byte head = reader.readByte();
        if ((head & 0x80) == 0x00) {
            // 0xxx xxxx: small value
            return getInlineStream(reader, head);
        } else if ((head & 0xc0) == 0x80) {
            // 10xx xxxx: medium value
            int length = (reader.skip(-1).readShort() & 0x3fff) + SMALL_LIMIT;
            return getInlineStream(reader, length);
        } else if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            long length = (reader.skip(-1).readLong() & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
            int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(reader.readRecordId(), listSize);
            return new SegmentStream(record.getRecordId(), list, length);
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            String reference = readReference(reader, head);
            return record.getSegment().getSegmentId().getTracker().getStore()
                    .readBlob(reference).getNewStream();
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    @Override
    public long length() {
        Reader reader = record.getReader();
        byte head = reader.readByte();
        if ((head & 0x80) == 0x00) {
            // 0xxx xxxx: small value
            return head;
        } else if ((head & 0xc0) == 0x80) {
            // 10xx xxxx: medium value
            return (reader.skip(-1).readShort() & 0x3fff) + SMALL_LIMIT;
        } else if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            return (reader.skip(-1).readLong() & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            String reference = readReference(reader, head);
            long length = record.getSegment().getSegmentId().getTracker().getStore()
                    .readBlob(reference).length();
            if (length == -1) {
                throw new IllegalStateException(
                        "Unknown length of external binary: " + reference);
            }
            return length;
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    @Override
    @CheckForNull
    public String getReference() {
        String blobId = getBlobId();
        if (blobId != null) {
            BlobStore blobStore = record.getSegment().getSegmentId().getTracker().
                    getStore().getBlobStore();
            if (blobStore != null) {
                return blobStore.getReference(blobId);
            } else {
                throw new IllegalStateException("Attempt to read external blob with blobId [" + blobId + "] " +
                        "without specifying BlobStore");
            }
        }
        return null;
    }


    @Override
    public String getContentIdentity() {
        return record.getRecordId().toString();
    }

    public boolean isExternal() {
        Reader reader = record.getReader();
        byte head = reader.readByte();
        // 1110 xxxx: external value
        return (head & 0xf0) == 0xe0;
    }

    public String getBlobId() {
        Reader reader = record.getReader();
        byte head = reader.readByte();
        if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            return readReference(reader, head);
        } else {
            return null;
        }
    }

    public SegmentBlob clone(SegmentWriter writer, boolean cloneLargeBinaries) throws IOException {
        Reader reader = record.getReader();
        byte head = reader.readByte();
        if ((head & 0x80) == 0x00) {
            // 0xxx xxxx: small value
            return writer.writeStream(new BufferedInputStream(getNewStream()));
        } else if ((head & 0xc0) == 0x80) {
            // 10xx xxxx: medium value
            return writer.writeStream(new BufferedInputStream(getNewStream()));
        } else if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            if (cloneLargeBinaries) {
                return writer.writeStream(new BufferedInputStream(
                        getNewStream()));
            } else {
                // this was the previous (default) behavior
                long length = (reader.skip(-1).readLong() & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
                int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
                ListRecord list = new ListRecord(reader.readRecordId(), listSize);
                return writer.writeLargeBlob(length, list.getEntries());
            }
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            return writer.writeExternalBlob(getBlobId());
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        if (object == this || fastEquals(object)) {
            return true;
        } else if (object instanceof SegmentBlob) {
            SegmentBlob that = (SegmentBlob) object;
            if (this.record.wasCompactedTo(that.record) || that.record.wasCompactedTo(this.record)) {
                return true;
            }
        }
        return object instanceof Blob
                && AbstractBlob.equal(this, (Blob) object);
    }

    private boolean fastEquals(Object other) {
        return other instanceof SegmentBlob && Record.fastEquals(this.record, ((SegmentBlob) other).record);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    //-----------------------------------------------------------< private >--

    private static String readReference(Reader reader, byte head) {
        int length = (head & 0x0f) << 8 | (reader.readByte() & 0xff);
        byte[] bytes = new byte[length];
        reader.readBytes(bytes, 0, length);
        return new String(bytes, UTF_8);
    }

    private Iterable<SegmentId> getBulkSegmentIds() {
        Reader reader = record.getReader();
        byte head = reader.readByte();
        if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            long length = (reader.skip(-1).readLong() & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
            int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(reader.readRecordId(), listSize);
            Set<SegmentId> ids = newIdentityHashSet();
            for (RecordId id : list.getEntries()) {
                ids.add(id.getSegmentId());
            }
            return ids;
        } else {
            return emptySet();
        }
    }

}
