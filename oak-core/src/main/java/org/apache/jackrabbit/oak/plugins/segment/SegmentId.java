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

import static com.google.common.collect.Maps.newHashMap;

import java.util.Map;
import java.util.UUID;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Segment identifier. There are two types of segments: data segments, and bulk
 * segments. Data segments have a header and may reference other segments; bulk
 * segments do not.
 */
public class SegmentId implements Comparable<SegmentId> {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(SegmentId.class);

    private final boolean proxy;

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
            Segment segment, long creationTime, boolean proxy) {
        this.tracker = tracker;
        this.msb = msb;
        this.lsb = lsb;
        this.segment = segment;
        this.creationTime = creationTime;
        this.proxy = proxy;
    }

    private final Map<RecordId, Record> records = new WeakHashMap<RecordId, Record>();

    Record getRecord(Record record) {
        synchronized (records) {
            Record r = records.get(record.getRecordId());
            if (r == null) {
                records.put(record.getRecordId(), record);
                return record;
            } else {
                return r;
            }
        }
    }

    // michid think about atomicity of access to proxy segment vs. the original segment
    // michid separate rewrite/relink?
    void rewrite() {
        SegmentWriter writer = new SegmentWriter(tracker.getStore(), tracker);
        Map<Integer, Integer> offsets = newHashMap();

        synchronized (records) {
            for (Record record : records.values()) {
                offsets.put(record.getRecordId().getOffset(), record.rewrite(writer).getOffset());
            }
        }

        SegmentId proxyId = new SegmentId(tracker, msb, lsb, segment, creationTime, true);
        writer.writeProxy(proxyId, this, offsets);
        writer.flush();
    }

    public SegmentId(SegmentTracker tracker, long msb, long lsb) {
        this(tracker, msb, lsb, null, System.currentTimeMillis(), false);
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

    public boolean isProxy() {
        return proxy;
    }
}
