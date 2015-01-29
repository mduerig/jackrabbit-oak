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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.newSetFromMap;

/**
 * Segment identifier. There are two types of segments: data segments, and bulk
 * segments. Data segments have a header and may reference other segments; bulk
 * segments do not.
 */
public class SegmentId implements Comparable<SegmentId> {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(SegmentId.class);

    /**
     * Checks whether this is a data segment identifier.
     *
     * @return {@code true} for a data segment, {@code false} otherwise
     */
    public static boolean isDataSegmentId(long lsb) {
        return (lsb >>> 60) == 0xAL;
    }

    private final SegmentTracker tracker;

    private final long msb;

    private final long lsb;

    private final long creationTime;

    /**
     * A reference to the segment object, if it is available in memory. It is
     * used for fast lookup. The segment tracker will set or reset this field.
     */
    // TODO: possibly we could remove the volatile
    private volatile Segment segment;

    private SegmentId(SegmentTracker tracker, long msb, long lsb,
            Segment segment, long creationTime) {
        this.tracker = tracker;
        this.msb = msb;
        this.lsb = lsb;
        this.segment = segment;
        this.creationTime = creationTime;
    }

    public SegmentId(SegmentTracker tracker, long msb, long lsb) {
        this(tracker, msb, lsb, null, System.currentTimeMillis());
    }

    /**
     * Checks whether this is a data segment identifier.
     *
     * @return {@code true} for a data segment, {@code false} otherwise
     */
    public boolean isDataSegmentId() {
        return isDataSegmentId(lsb);
    }

    /**
     * Checks whether this is a bulk segment identifier.
     *
     * @return {@code true} for a bulk segment, {@code false} otherwise
     */
    public boolean isBulkSegmentId() {
        return (lsb >>> 60) == 0xBL;
    }

    public boolean equals(long msb, long lsb) {
        return this.msb == msb && this.lsb == lsb;
    }

    public long getMostSignificantBits() {
        return msb;
    }

    public long getLeastSignificantBits() {
        return lsb;
    }

    public Segment getSegment() {
        Segment segment = this.segment;
        if (segment == null) {
            synchronized (this) {
                segment = this.segment;
                if (segment == null) {
                    log.debug("Loading segment {}", this);
                    segment = tracker.getSegment(this);
                }
            }
        }
        segment.access();
        return segment;
    }

    synchronized void setSegment(Segment segment) {
        this.segment = segment;
    }

    public SegmentTracker getTracker() {
        return tracker;
    }

    public long getCreationTime() {
        return creationTime;
    }

    // --------------------------------------------------------< Comparable >--

    @Override
    public int compareTo(SegmentId that) {
        int d = Long.valueOf(this.msb).compareTo(Long.valueOf(that.msb));
        if (d == 0) {
            d = Long.valueOf(this.lsb).compareTo(Long.valueOf(that.lsb));
        }
        return d;
    }

    // ------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return new UUID(msb, lsb).toString();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof SegmentId) {
            SegmentId that = (SegmentId) object;
            return msb == that.msb && lsb == that.lsb;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (int) lsb;
    }

    // ------------------------------------------------------------< Gc >--

    private Set<GCRecordIdCb> refs = newSetFromMap(new WeakHashMap<GCRecordIdCb, Boolean>());

    private volatile boolean stale = false;
    private volatile boolean hasRefs = false;

    public interface GCRecordIdCb {

        RecordId getRecordId();

        void gcRecordId(RecordId newRI);

        void missing();
    }

    private static class GCRecordIDWrapper implements GCRecordIdCb {

        // this helps to work around the SegmentNodeStore#equals (heavy) check
        private final GCRecordIdCb cb;

        public GCRecordIDWrapper(GCRecordIdCb cb) {
            this.cb = cb;
        }

        @Override
        public RecordId getRecordId() {
            return cb.getRecordId();
        }

        @Override
        public void gcRecordId(RecordId newRI) {
            cb.gcRecordId(newRI);
        }

        @Override
        public void missing() {
            cb.missing();
        }

        @Override
        public int hashCode() {
            return cb.getRecordId().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            GCRecordIDWrapper other = (GCRecordIDWrapper) obj;
            if (cb == null) {
                if (other.cb != null)
                    return false;
            } else if (!cb.getRecordId().equals(other.cb.getRecordId()))
                return false;
            return true;
        }

    }

    boolean hasRefs() {
        return hasRefs;
    }

    boolean isStale() {
        return stale;
    }

    synchronized void collectRefs(Collection<RecordId> ids) {
        for (GCRecordIdCb cb : refs) {
            ids.add(cb.getRecordId());
        }
    }

    synchronized void track(GCRecordIdCb cb) {
        if (stale) {
            gc(cb, tracker.getCompactionMap());
        } else {
            refs.add(new GCRecordIDWrapper(cb));
            if (!hasRefs) {
                hasRefs = true;
            }
        }
    }

    synchronized void gc(CompactionMap compaction) {
        Iterator<GCRecordIdCb> iterator = refs.iterator();
        while (iterator.hasNext()) {
            if (gc(iterator.next(), compaction)) {
                iterator.remove();
            } else {
                // TODO why are there still missing items from the compaction
                // map?
                iterator.remove();
            }
        }
        stale = true;
        if (refs.isEmpty()) {
            refs = null;
            hasRefs = false;
        } else {
            // this should reduce the internal allocated table size from a few
            // thousands to 2,3 items
            Set<GCRecordIdCb> refsNew = newSetFromMap(new WeakHashMap<GCRecordIdCb, Boolean>());
            refsNew.addAll(refs);
            refs = refsNew;
        }
    }

    private static boolean gc(GCRecordIdCb cb, CompactionMap compaction) {
        RecordId rid = cb.getRecordId();
        RecordId newRI = compaction.get(rid);
        if (newRI != null) {
            cb.gcRecordId(newRI);
            return true;
        } else {
            cb.missing();
        }
        return false;
    }

}
