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
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.segment.Record.fastEquals;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.SEGMENT_REFERENCE_LIMIT;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.segment.Segment.Reader;

/**
 * A record of type "LIST".
 */
class ListRecord {
    static final int LEVEL_SIZE = SEGMENT_REFERENCE_LIMIT;

    private final Page page;

    private final int size;

    private final int bucketSize;

    ListRecord(@Nonnull Page page, int size) {
        this.page = checkNotNull(page);
        checkArgument(size >= 0);
        this.size = size;

        int bs = 1;
        while (bs * LEVEL_SIZE < size) {
            bs *= LEVEL_SIZE;
        }
        this.bucketSize = bs;
    }

    public int size() {
        return size;
    }

    public Page getEntry(int index) {
        checkElementIndex(index, size);
        if (size == 1) {
            return page;
        } else {
            int bucketIndex = index / bucketSize;
            int bucketOffset = index % bucketSize;
            Page bucketPage = page.readPage(0, bucketIndex);
            ListRecord bucket = new ListRecord(
                    bucketPage, min(bucketSize, size - bucketIndex * bucketSize));
            return bucket.getEntry(bucketOffset);
        }
    }

    public List<Page> getEntries() {
        return getEntries(0, size);
    }

    public List<Page> getEntries(int index, int count) {
        if (index + count > size) {
            count = size - index;
        }
        if (count == 0) {
            return emptyList();
        } else if (count == 1) {
            return singletonList(getEntry(index));
        } else {
            List<Page> pages = newArrayListWithCapacity(count);
            getEntries(index, count, pages);
            return pages;
        }
    }

    private void getEntries(int index, int count, List<Page> pages) {
        checkPositionIndexes(index, index + count, size);
        if (size == 1) {
            pages.add(page);
        } else if (bucketSize == 1) {
            Reader reader = page.getReader(0, index);
            for (int i = 0; i < count; i++) {
                pages.add(reader.readPage());
            }
        } else {
            while (count > 0) {
                int bucketIndex = index / bucketSize;
                int bucketOffset = index % bucketSize;
                Page buckedPage = page.readPage(0, bucketIndex);
                ListRecord bucket = new ListRecord(
                        buckedPage, min(bucketSize, size - bucketIndex * bucketSize));
                int n = min(bucket.size() - bucketOffset, count);
                bucket.getEntries(bucketOffset, n, pages);
                index += n;
                count -= n;
            }
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else {
            return object instanceof ListRecord &&
                    fastEquals(page, ((ListRecord) object).page);
        }
    }

    @Override
    public int hashCode() {
        return page.hashCode() ^ size;
    }

    @Override
    public String toString() {
        return page.toString();
    }
}
