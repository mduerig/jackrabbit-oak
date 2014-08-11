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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.observation.Event;

/**
 * {@code Queue} instances are used by {@link org.apache.jackrabbit.oak.jcr.observation.EventQueue}
 * as backing implementation.
 */
public interface Queue {

    /**
     * Add an event to the end of the queue
     * @param event  the event to add
     * @return  {@code true} on success, {@code false} otherwise.
     */
    boolean add(@Nonnull Event event);

    /**
     * Obtain a reader for this queue.
     * @return  a reader instance or {@code null} if the queue
     * exceeded its capacity.
     */
    @CheckForNull
    Reader getReader();

    /**
     * A reader for reading from a queue.
     */
    interface Reader {

        /**
         * Number of elements in the queue
         * @return  size of the queue
         */
        long size();

        /**
         * @return  {@code true} iff <pre>size() == 0</pre>
         */
        boolean isEmpty();

        /**
         * Removes all elements from the queue.
         */
        void clear();

        /**
         * Removes and returns the first element from the queue.
         * @return the first element from the queue
         * @throws java.util.NoSuchElementException if this queue is empty
         */
        @Nonnull
        Event removeFirst();
        void removeFirst(long num);
    }
}
