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

package org.apache.jackrabbit.oak.jcr.observation.queues;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;

import java.util.LinkedList;

import javax.annotation.Nonnull;
import javax.jcr.observation.Event;

/**
 * A {@code Queue} implementation with a fixed capacity.
 * <p>
 * Once the number of elements in this queue reaches the capacity
 * of the queue adding further elements will cause the queue
 * to become full.
 */
public class FixedQueue implements Queue {
    private final LinkedList<Event> queue = newLinkedList();
    private final int capacity;

    /**
     * Create a new instance with the given capacity
     * @param capacity
     */
    public FixedQueue(int capacity) {
        this.capacity = capacity;
    }

    /**
     * @return  the capacity of this queue
     */
    public int getCapacity() {
        return capacity;
    }

    @Override
    public boolean add(@Nonnull Event event) {
        if (queue.size() < capacity) {
            queue.add(checkNotNull(event));
            return true;
        } else {
            return false;
        }
    }

    @Override
    @Nonnull
    public Reader getReader() {
        return new LinkedListReader(queue);
    }

}
