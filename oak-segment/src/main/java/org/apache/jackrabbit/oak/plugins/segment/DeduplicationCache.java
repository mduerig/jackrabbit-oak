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

package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * michid document
 */
// michid unit test
// michid implement monitoring for this cache
public class DeduplicationCache {
    private static final Logger LOG = LoggerFactory.getLogger(DeduplicationCache.class);

    private final int capacity;
    private final List<Map<String, RecordId>> maps;

    private int size;

    private final Set<Integer> muteDepths = newHashSet();

    DeduplicationCache(int capacity, int maxDepth) {
        checkArgument(capacity > 0);
        checkArgument(maxDepth > 0);
        this.capacity = capacity;
        this.maps = newArrayList();
        for (int k = 0; k < maxDepth; k++) {
            maps.add(new HashMap<String, RecordId>());
        }
    }

    synchronized void put(String key, RecordId value, int depth) {
        // michid validate and optimise eviction strategy
        while (size >= capacity) {
            int d = maps.size() - 1;
            int removed = maps.remove(d).size();
            size -= removed;
            if (removed > 0) {
                LOG.info("Evicted cache at depth {} as size {} reached capacity {}. " +
                    "New size is {}", d, size + removed, capacity, size);
            }
        }

        if (depth < maps.size()) {
            if (maps.get(depth).put(key, value) == null) {
                size++;
            }
        } else {
            if (muteDepths.add(depth)) {
                LOG.info("Not caching {} -> {} as depth {} reaches or exceeds the maximum of {}",
                    key, value, depth, maps.size());
            }
        }
    }

    synchronized RecordId get(String key) {
        for (Map<String, RecordId> map : maps) {
            if (!map.isEmpty()) {
                RecordId recordId = map.get(key);
                if (recordId != null) {
                    return recordId;
                }
            }
        }
        return null;
    }


}
