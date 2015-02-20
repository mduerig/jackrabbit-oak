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
public class SegmentBlob implements Blob {
    private final Page page;

    public static Iterable<Page> getBulkPages(Blob blob) {
        if (blob instanceof SegmentBlob) {
            return ((SegmentBlob) blob).getBulkPages();
        } else {
            return emptySet();
        }
    }

    SegmentBlob(@Nonnull Page page) {
        this.page = page;
    }

    public Page getPage() {
        return page;
    }

    private InputStream getInlineStream(Reader reader, int length) {
        byte[] inline = new byte[length];
        reader.readBytes(inline, 0, length);
        return new SegmentStream(page, inline);
    }

    @Override @Nonnull
    public InputStream getNewStream() {
        byte head = page.readByte(0);
        Reader reader = page.getReader();
        if ((head & 0x80) == 0x00) {
            // 0xxx xxxx: small value
            return getInlineStream(reader, head);
        } else if ((head & 0xc0) == 0x80) {
            // 10xx xxxx: medium value
            int length = (reader.readShort() & 0x3fff) + SMALL_LIMIT;
            return getInlineStream(reader, length);
        } else if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            long length = (reader.readLong() & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
            int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(reader.readPage(), listSize);
            return new SegmentStream(page, list, length);
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            String reference = readReference(reader, head);
            return page.getNodeStore().readBlob(reference).getNewStream();
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    @Override
    public long length() {
        byte head = page.readByte(0);
        Reader reader = page.getReader();
        if ((head & 0x80) == 0x00) {
            // 0xxx xxxx: small value
            return head;
        } else if ((head & 0xc0) == 0x80) {
            // 10xx xxxx: medium value
            return (reader.readShort() & 0x3fff) + SMALL_LIMIT;
        } else if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            return (reader.readLong() & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            String reference = readReference(reader, head);
            long length = page.getNodeStore().readBlob(reference).length();
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
            BlobStore blobStore = page.getBlobStore();
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
        return page.getContentIdentity();
    }

    public boolean isExternal() {
        Reader reader = page.getReader();
        byte head = reader.readByte();
        // 1110 xxxx: external value
        return (head & 0xf0) == 0xe0;
    }

    public String getBlobId() {
        Reader reader = page.getReader();
        byte head = reader.readByte();
        if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            return readReference(reader, head);
        } else {
            return null;
        }
    }

    public SegmentBlob clone(SegmentWriter writer, boolean cloneLargeBinaries) throws IOException {
        Reader reader = page.getReader();
        byte head = page.readByte(0);
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
                long length = (reader.readLong() & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
                int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
                ListRecord list = new ListRecord(reader.readPage(), listSize);
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
            if (page.equals(that.page)) {
                return true;
            }
        }
        return object instanceof Blob
                && AbstractBlob.equal(this, (Blob) object);
    }

    private boolean fastEquals(Object other) {
        return other instanceof SegmentBlob && Record.fastEquals(this.page, ((SegmentBlob) other).page);
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

    private Iterable<Page> getBulkPages() {
        byte head = page.readByte(0);
        Reader reader = page.getReader();
        if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            long length = (reader.readLong() & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
            int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(reader.readPage(), listSize);
            Set<Page> ids = newIdentityHashSet();
            for (Page bulkPage : list.getEntries()) {
                ids.add(bulkPage);
            }
            return ids;
        } else {
            return emptySet();
        }
    }
}
