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
import static org.apache.jackrabbit.oak.segment.io.raw.RawRecordConstants.TEMPLATE_HEADER_SIZE;

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

    private void putRecordId(ByteBuffer buffer, RawRecordId id) {
        buffer.putShort(segmentReference(id.getSegmentId())).putInt(id.getRecordNumber());
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
        buffer.putLong(longLength(length));
        putRecordId(buffer, id);
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
        buffer.put(LONG_BLOB_ID_LENGTH);
        putRecordId(buffer, id);
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
            putRecordId(buffer, entry.getKey());
            putRecordId(buffer, entry.getValue());
        }
        return true;
    }

    private static Set<UUID> mapLeafReferences(RawMapLeaf leaf) {
        Set<UUID> refs = null;
        for (RawMapEntry entry : leaf.getEntries()) {
            refs = maybeAdd(refs, entry.getKey());
            refs = maybeAdd(refs, entry.getValue());
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
            putRecordId(buffer, reference);
        }
        return true;
    }

    private static Set<UUID> mapBranchReferences(RawMapBranch branch) {
        Set<UUID> refs = null;
        for (RawRecordId reference : branch.getReferences()) {
            refs = maybeAdd(refs, reference);
        }
        return refs;
    }

    public boolean writeListBucket(int number, int type, List<RawRecordId> list) {
        ByteBuffer buffer = addRecord(number, type, RawRecordId.BYTES * list.size(), listBucketReferences(list));
        if (buffer == null) {
            return false;
        }
        for (RawRecordId id : list) {
            putRecordId(buffer, id);
        }
        return true;
    }

    private static Set<UUID> listBucketReferences(List<RawRecordId> list) {
        Set<UUID> refs = null;
        for (RawRecordId id : list) {
            refs = maybeAdd(refs, id);
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
            putRecordId(buffer, list.getBucket());
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

    public boolean writeTemplate(int number, int type, RawTemplate template) {
        ByteBuffer buffer = addRecord(number, type, templateSize(template), templateReferences(template));
        if (buffer == null) {
            return false;
        }
        buffer.putInt(templateHeader(template));
        if (template.getPrimaryType() != null) {
            putRecordId(buffer, template.getPrimaryType());
        }
        if (template.getMixins() != null) {
            for (RawRecordId id : template.getMixins()) {
                putRecordId(buffer, id);
            }
        }
        if (template.getChildNodeName() != null) {
            putRecordId(buffer, template.getChildNodeName());
        }
        if (template.getPropertyNames() != null) {
            putRecordId(buffer, template.getPropertyNames());
        }
        if (template.getPropertyTypes() != null) {
            for (byte propertyType : template.getPropertyTypes()) {
                buffer.put(propertyType);
            }

        }
        return true;
    }

    private static int templateSize(RawTemplate template) {
        int size = TEMPLATE_HEADER_SIZE;
        if (template.getPrimaryType() != null) {
            size += RawRecordId.BYTES;
        }
        if (template.getMixins() != null) {
            size += RawRecordId.BYTES * template.getMixins().size();
        }
        if (template.getChildNodeName() != null) {
            size += RawRecordId.BYTES;
        }
        if (template.getPropertyNames() != null) {
            size += RawRecordId.BYTES;
        }
        if (template.getPropertyTypes() != null) {
            size += Byte.BYTES * template.getPropertyTypes().size();
        }
        return size;
    }

    private static Set<UUID> templateReferences(RawTemplate template) {
        Set<UUID> refs;
        refs = maybeAdd(null, template.getPrimaryType());
        refs = maybeAdd(refs, template.getChildNodeName());
        refs = maybeAdd(refs, template.getPropertyNames());
        if (template.getMixins() != null) {
            for (RawRecordId id : template.getMixins()) {
                refs = maybeAdd(refs, id);
            }
        }
        return refs;
    }

    private static Set<UUID> maybeAdd(Set<UUID> references, RawRecordId id) {
        if (id == null) {
            return references;
        }
        if (references == null) {
            references = new HashSet<>();
        }
        references.add(id.getSegmentId());
        return references;
    }

    private static int templateHeader(RawTemplate template) {
        long header = 0;
        if (template.getPrimaryType() != null) {
            header |= 1L << 31;
        }
        if (template.getMixins() != null && !template.getMixins().isEmpty()) {
            header |= 1L << 30;
            header |= (template.getMixins().size() & 0x3ff) << 18;
        }
        if (template.hasNoChildNodes()) {
            header |= 1L << 29;
        }
        if (template.hasManyChildNodes()) {
            header |= 1L << 28;
        }
        if (template.getPropertyTypes() != null && !template.getPropertyTypes().isEmpty()) {
            header |= template.getPropertyTypes().size() & 0x3ffff;
        }
        return (int) header;
    }

    public boolean writeNode(int number, int type, RawNode node) {
        ByteBuffer buffer = addRecord(number, type, nodeSize(node), nodeReferences(node));
        if (buffer == null) {
            return false;
        }
        if (node.getStableId() != null) {
            putRecordId(buffer, node.getStableId());
        } else {
            buffer.putShort((short) 0).putInt(number);
        }
        putRecordId(buffer, node.getTemplate());
        if (node.getChild() != null) {
            putRecordId(buffer, node.getChild());
        }
        if (node.getChildrenMap() != null) {
            putRecordId(buffer, node.getChildrenMap());
        }
        if (node.getPropertiesList() != null) {
            putRecordId(buffer, node.getPropertiesList());
        }
        return true;
    }

    private static int nodeSize(RawNode node) {
        int size = 2 * RawRecordId.BYTES;
        if (node.getChild() != null) {
            size += RawRecordId.BYTES;
        }
        if (node.getChildrenMap() != null) {
            size += RawRecordId.BYTES;
        }
        if (node.getPropertiesList() != null) {
            size += RawRecordId.BYTES;
        }
        return size;
    }

    private static Set<UUID> nodeReferences(RawNode node) {
        Set<UUID> refs;
        refs = maybeAdd(null, node.getChild());
        refs = maybeAdd(refs, node.getChildrenMap());
        refs = maybeAdd(refs, node.getPropertiesList());
        refs = maybeAdd(refs, node.getTemplate());
        refs = maybeAdd(refs, node.getStableId());
        return refs;
    }

}
