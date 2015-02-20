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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.lang.Integer.bitCount;
import static java.lang.Integer.highestOneBit;
import static java.lang.Integer.numberOfTrailingZeros;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.jackrabbit.oak.plugins.segment.Segment.Reader;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * A map. The top level record is either a record of type "BRANCH" or "LEAF"
 * (depending on the data).
 */
class MapRecord {

    /**
     * Magic constant from a random number generator, used to generate
     * good hash values.
     */
    private static final int M = 0xDEECE66D;
    private static final int A = 0xB;
    static final long HASH_MASK = 0xFFFFFFFFL;

    /**
     * Generates a hash code for the value, using a random number generator
     * to improve the distribution of the hash values.
     */
    static int getHash(String name) {
        return (name.hashCode() ^ M) * M + A;
    }

    /**
     * Number of bits of the hash code to look at on each level of the trie.
     */
    protected static final int BITS_PER_LEVEL = 5;

    /**
     * Number of buckets at each level of the trie.
     */
    protected static final int BUCKETS_PER_LEVEL = 1 << BITS_PER_LEVEL; // 32

    /**
     * Maximum number of trie levels.
     */
    protected static final int MAX_NUMBER_OF_LEVELS =
            (32 + BITS_PER_LEVEL - 1) / BITS_PER_LEVEL; // 7

    /**
     * Number of bits needed to indicate the current trie level.
     * Currently 4.
     */
    protected static final int LEVEL_BITS = // 4, using nextPowerOfTwo():
            numberOfTrailingZeros(highestOneBit(MAX_NUMBER_OF_LEVELS) << 1);

    /**
     * Number of bits used to indicate the size of a map.
     * Currently 28.
     */
    protected static final int SIZE_BITS = 32 - LEVEL_BITS;

    /**
     * Maximum size of a map.
     */
    protected static final int MAX_SIZE = (1 << SIZE_BITS) - 1; // ~268e6

    private final Page page;

    protected MapRecord(@Nonnull Page page) {
        this.page = checkNotNull(page);
    }

    public Page getPage() {
        return page;
    }

    Reader getReader(int offset) {
        return page.getReader(offset);
    }

    Reader getReader(int offset, int ids) {
        return page.getReader(offset, ids);
    }

    boolean isLeaf() {
        int head = page.readInt(0);
        if (isDiff(head)) {
            Page base = page.readPage(8, 2);
            return new MapRecord(base).isLeaf();
        }
        return !isBranch(head);
    }

    public boolean isDiff() {
        return isDiff(page.getReader().readInt());
    }

    MapRecord[] getBuckets() {
        Reader reader = page.getReader(4);
        MapRecord[] buckets = new MapRecord[BUCKETS_PER_LEVEL];
        int bitmap = reader.readInt();
        int ids = 0;
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            if ((bitmap & (1 << i)) != 0) {
                buckets[i] = new MapRecord(reader.readPage());
            } else {
                buckets[i] = null;
            }
        }
        return buckets;
    }

    private List<MapRecord> getBucketList() {
        List<MapRecord> buckets = newArrayListWithCapacity(BUCKETS_PER_LEVEL);
        Reader reader = page.getReader(4);
        int bitmap = reader.readInt();
        int ids = 0;
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            if ((bitmap & (1 << i)) != 0) {
                buckets.add(new MapRecord(reader.readPage()));
            }
        }
        return buckets;
    }

    int size() {
        int head = page.readInt(0);
        if (isDiff(head)) {
            return new MapRecord(page.readPage(8, 2)).size();
        }
        return getSize(head);
    }

    MapEntry getEntry(String name) {
        checkNotNull(name);
        int hash = getHash(name);
        Reader reader = page.getReader();

        int head = reader.readInt();
        int bitmap = reader.readInt();
        if (isDiff(head)) {
            if (hash == bitmap) {
                Page key = reader.readPage();
                if (name.equals(key.readString(0))) {
                    Page value = reader.readPage();
                    return new MapEntry(name, key, value);
                }
            }
            return new MapRecord(page.readPage(8, 2)).getEntry(name);
        }

        int size = getSize(head);
        if (size == 0) {
            return null; // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            // this is an intermediate branch record
            // check if a matching bucket exists, and recurse 
            int mask = (1 << BITS_PER_LEVEL) - 1;
            int shift = 32 - (level + 1) * BITS_PER_LEVEL;
            int index = (hash >> shift) & mask;
            int bit = 1 << index;
            if ((bitmap & bit) != 0) {
                int ids = bitCount(bitmap & (bit - 1));
                return new MapRecord(page.readPage(8, ids)).getEntry(name);
            } else {
                return null;
            }
        }

        // use interpolation search to find the matching entry in this map leaf
        int shift = 32 - level * BITS_PER_LEVEL;
        long mask = -1L << shift;
        long h = hash & HASH_MASK;
        int p = 0;
        long pH = h & mask;   // lower bound on hash values in this map leaf
        int q = size - 1;
        long qH = pH | ~mask; // upper bound on hash values in this map leaf
        while (p <= q) {
            assert pH <= qH;

            // interpolate the most likely index of the target entry
            // based on its hash code and the lower and upper bounds
            int i = p + (int) ((q - p) * (h - pH) / (qH - pH));
            assert p <= i && i <= q;

            long iH = page.readInt(4 + i * 4) & HASH_MASK;
            int diff = Long.valueOf(iH).compareTo(Long.valueOf(h));
            if (diff == 0) {
                Page keyId = page.readPage(4 + size * 4, i * 2);
                Page valueId = page.readPage(4 + size * 4, i * 2 + 1);
                diff = keyId.readString(0).compareTo(name);
                if (diff == 0) {
                    return new MapEntry(name, keyId, valueId);
                }
            }

            if (diff < 0) {
                p = i + 1;
                pH = iH;
            } else {
                q = i - 1;
                qH = iH;
            }
        }
        return null;
    }

    private RecordId getValue(int hash, RecordId key) {
        checkNotNull(key);
        Reader reader = page.getReader();

        int head = reader.readInt();
        int bitmap = reader.readInt();
        if (isDiff(head)) {
            if (hash == bitmap && key.equals(reader.readRecordId())) {
                return reader.readRecordId();
            }
            return new MapRecord(reader.readPage()).getValue(hash, key);
        }

        int size = getSize(head);
        if (size == 0) {
            return null; // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            // this is an intermediate branch record
            // check if a matching bucket exists, and recurse
            int mask = (1 << BITS_PER_LEVEL) - 1;
            int shift = 32 - (level + 1) * BITS_PER_LEVEL;
            int index = (hash >> shift) & mask;
            int bit = 1 << index;
            if ((bitmap & bit) != 0) {
                int ids = bitCount(bitmap & (bit - 1));
                return new MapRecord(page.readPage(8, ids)).getValue(hash, key);
            } else {
                return null;
            }
        }

        // this is a leaf record; scan the list to find a matching entry
        Long h = hash & HASH_MASK;
        for (int i = 0; i < size; i++) {
            int diff = h.compareTo(reader.readInt(4 + i * 4) & HASH_MASK);
            if (diff > 0) {
                return null;
            } else if (diff == 0) {
                if (key.equals(reader.readRecordId(4 + size * 4, i * 2))) {
                    return reader.readRecordId(4 + size * 4, i * 2 + 1);
                }
            }
        }
        return null;
    }

    Iterable<String> getKeys() {
        Reader reader = page.getReader();

        int head = reader.readInt();
        if (isDiff(head)) {
            return new MapRecord(page.readPage(8, 2)).getKeys();
        }

        int size = getSize(head);
        if (size == 0) {
            return Collections.emptyList(); // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            List<MapRecord> buckets = getBucketList();
            List<Iterable<String>> keys =
                    newArrayListWithCapacity(buckets.size());
            for (MapRecord bucket : buckets) {
                keys.add(bucket.getKeys());
            }
            return concat(keys);
        }

        Page[] pages = new Page[size];
        for (int i = 0; i < size; i++) {
            pages[i] = page.readPage(4 + size * 4, i * 2);
        }

        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = pages[i].readString(0);
        }
        return Arrays.asList(keys);
    }

    Iterable<MapEntry> getEntries() {
        return getEntries(null, null);
    }

    private Iterable<MapEntry> getEntries(
            final Page diffKey, final Page diffValue) {
        Reader reader = page.getReader();

        int head = reader.readInt();
        if (isDiff(head)) {
            reader.skip(4);
            Page key = reader.readPage();
            Page value = reader.readPage();
            Page base = reader.readPage();
            return new MapRecord(base).getEntries(key, value);
        }

        int size = getSize(head);
        if (size == 0) {
            return Collections.emptyList(); // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            List<MapRecord> buckets = getBucketList();
            List<Iterable<MapEntry>> entries =
                    newArrayListWithCapacity(buckets.size());
            for (final MapRecord bucket : buckets) {
                entries.add(new Iterable<MapEntry>() {
                    @Override
                    public Iterator<MapEntry> iterator() {
                        return bucket.getEntries(diffKey, diffValue).iterator();
                    }
                });
            }
            return concat(entries);
        }

        Page[] keys = new Page[size];
        Page[] values = new Page[size];
        for (int i = 0; i < size; i++) {
            keys[i] = page.readPage(4 + size * 4, i * 2);
            if (keys[i].equals(diffKey)) {
                values[i] = diffValue;
            } else {
                values[i] = page.readPage(4 + size * 4, i * 2 + 1);
            }
        }

        MapEntry[] entries = new MapEntry[size];
        for (int i = 0; i < size; i++) {
            String name = keys[i].readString(0);
            entries[i] = new MapEntry(name, keys[i], values[i]);
        }
        return Arrays.asList(entries);
    }

    boolean compare(MapRecord before, final NodeStateDiff diff) {
        if (Record.fastEquals(page, before.page)) {
            return true;
        }

        Reader reader = page.getReader();
        int head = reader.readInt();
        if (isDiff(head)) {
            int hash = reader.readInt();
            final String key = reader.readPage().readString(0);
            final Page value = reader.readPage();
            MapRecord base = new MapRecord(reader.readPage());

            boolean rv = base.compare(before, new DefaultNodeStateDiff() {
                @Override
                public boolean childNodeAdded(String name, NodeState after) {
                    return name.equals(key)
                            || diff.childNodeAdded(name, after);
                }
                @Override
                public boolean childNodeChanged(
                        String name, NodeState before, NodeState after) {
                    return name.equals(key)
                            || diff.childNodeChanged(name, before, after);
                }
                @Override
                public boolean childNodeDeleted(String name, NodeState before) {
                    return diff.childNodeDeleted(name, before);
                }
            });
            if (rv) {
                MapEntry beforeEntry = before.getEntry(key);
                if (beforeEntry == null) {
                    rv = diff.childNodeAdded(
                            key,
                            new SegmentNodeState(value));
                } else if (!value.equals(beforeEntry.getValue())) {
                    rv = diff.childNodeChanged(
                            key,
                            beforeEntry.getNodeState(),
                            new SegmentNodeState(value));
                }
            }
            return rv;
        }

        Reader beforeReader = before.page.getReader();
        int beforeHead = beforeReader.readInt();
        if (isDiff(beforeHead)) {
            int hash = beforeReader.readInt();
            final String key = beforeReader.readPage().readString(0);
            final Page value = beforeReader.readPage();
            MapRecord base = new MapRecord(beforeReader.readPage());

            boolean rv = this.compare(base, new DefaultNodeStateDiff() {
                @Override
                public boolean childNodeAdded(String name, NodeState after) {
                    return diff.childNodeAdded(name, after);
                }
                @Override
                public boolean childNodeChanged(
                        String name, NodeState before, NodeState after) {
                    return name.equals(key)
                            || diff.childNodeChanged(name, before, after);
                }
                @Override
                public boolean childNodeDeleted(String name, NodeState before) {
                    return name.equals(key)
                            || diff.childNodeDeleted(name, before);
                }
            });
            if (rv) {
                MapEntry afterEntry = this.getEntry(key);
                if (afterEntry == null) {
                    rv = diff.childNodeDeleted(
                            key,
                            new SegmentNodeState(value));
                } else if (!value.equals(afterEntry.getValue())) {
                    rv = diff.childNodeChanged(
                            key,
                            new SegmentNodeState(value),
                            afterEntry.getNodeState());
                }
            }
            return rv;
        }

        if (isBranch(beforeHead) && isBranch(head)) {
            return compareBranch(before, this, diff);
        }

        Iterator<MapEntry> beforeEntries = before.getEntries().iterator();
        Iterator<MapEntry> afterEntries = this.getEntries().iterator();

        MapEntry beforeEntry = nextOrNull(beforeEntries);
        MapEntry afterEntry = nextOrNull(afterEntries);
        while (beforeEntry != null || afterEntry != null) {
            int d = compare(beforeEntry, afterEntry);
            if (d < 0) {
                if (!diff.childNodeDeleted(
                        beforeEntry.getName(), beforeEntry.getNodeState())) {
                    return false;
                }
                beforeEntry = nextOrNull(beforeEntries);
            } else if (d == 0) {
                if (!beforeEntry.getValue().equals(afterEntry.getValue())
                        && !diff.childNodeChanged(
                                beforeEntry.getName(),
                                beforeEntry.getNodeState(),
                                afterEntry.getNodeState())) {
                    return false;
                }
                beforeEntry = nextOrNull(beforeEntries);
                afterEntry = nextOrNull(afterEntries);
            } else {
                if (!diff.childNodeAdded(
                        afterEntry.getName(), afterEntry.getNodeState())) {
                    return false;
                }
                afterEntry = nextOrNull(afterEntries);
            }
        }

        return true;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringBuilder builder = null;
        for (MapEntry entry : getEntries()) {
            if (builder == null) {
                builder = new StringBuilder("{ ");
            } else {
                builder.append(", ");
            }
            builder.append(entry);
        }
        if (builder == null) {
            return "{}";
        } else {
            builder.append(" }");
            return builder.toString();
        }
    }

    @Override
    public int hashCode() {
        return 31 * page.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else {
            return object instanceof MapRecord &&
                    Record.fastEquals(page, ((MapRecord) object).page);
        }
    }

    //-----------------------------------------------------------< private >--

    /**
     * Compares two map branches. Given the way the comparison algorithm
     * works, the branches are always guaranteed to be at the same level
     * with the same hash prefixes.
     */
    private static boolean compareBranch(
            MapRecord before, MapRecord after, NodeStateDiff diff) {
        MapRecord[] beforeBuckets = before.getBuckets();
        MapRecord[] afterBuckets = after.getBuckets();
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            if (Objects.equal(beforeBuckets[i], afterBuckets[i])) {
                // these buckets are equal (or both empty), so no changes
            } else if (beforeBuckets[i] == null) {
                // before bucket is empty, so all after entries were added
                MapRecord bucket = afterBuckets[i];
                for (MapEntry entry : bucket.getEntries()) {
                    if (!diff.childNodeAdded(
                            entry.getName(), entry.getNodeState())) {
                        return false;
                    }
                }
            } else if (afterBuckets[i] == null) {
                // after bucket is empty, so all before entries were deleted
                MapRecord bucket = beforeBuckets[i];
                for (MapEntry entry : bucket.getEntries()) {
                    if (!diff.childNodeDeleted(
                            entry.getName(), entry.getNodeState())) {
                        return false;
                    }
                }
            } else {
                // both before and after buckets exist; compare recursively
                MapRecord beforeBucket = beforeBuckets[i];
                MapRecord afterBucket = afterBuckets[i];
                if (!afterBucket.compare(beforeBucket, diff)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static int getSize(int head) {
        return head & ((1 << MapRecord.SIZE_BITS) - 1);
    }

    private static int getLevel(int head) {
        return head >>> MapRecord.SIZE_BITS;
    }

    private static boolean isDiff(int head) {
        return head == -1;
    }

    private static boolean isBranch(int head) {
        return isBranch(getSize(head), getLevel(head));
    }

    private static boolean isBranch(int size, int level) {
        return size > MapRecord.BUCKETS_PER_LEVEL
                && level < MapRecord.MAX_NUMBER_OF_LEVELS;
    }

    private static int compare(MapEntry before, MapEntry after) {
        if (before == null) {
            // A null value signifies the end of the list of entries,
            // which is why the return value here is a bit counter-intuitive
            // (null > non-null). The idea is to make a virtual end-of-list
            // sentinel value appear greater than any normal value.
            return 1;
        } else if (after == null) {
            return -1;  // see above
        } else {
            return ComparisonChain.start()
                    .compare(before.getHash() & HASH_MASK, after.getHash() & HASH_MASK)
                    .compare(before.getName(), after.getName())
                    .result();
        }
    }

    private static MapEntry nextOrNull(Iterator<MapEntry> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }

}
