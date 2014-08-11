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
package org.apache.jackrabbit.oak.jcr.observation;

import static com.google.common.collect.Lists.newLinkedList;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;

import org.apache.jackrabbit.oak.plugins.observation.EventGenerator;

/**
 * Queue of JCR Events generated from a given content change
 */
class EventQueue {
    private final LinkedList<Event> queue = newLinkedList();
    private final int capacity;

    private boolean full;

    /**
     * Create a new event queue with the given capacity.
     * @param capacity
     */
    EventQueue(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Determine whether the number of items added to the queue exceeded its
     * capacity.
     * @return  {@code true} if full {@code false} otherwise.
     */
    public boolean isFull() {
        return full;
    }

    /**
     * Remove all elements from the queue
     */
    public void clear() {
        queue.clear();
        full = false;
    }

    /**
     * Called by the {@link QueueingHandler} to add new events to the queue.
     */
    public void addEvent(Event event) {
        if (!full) {
            queue.add(event);
            if (queue.size() > capacity) {
                queue.clear();
                full = true;
            }
        }
    }

    //-----------------------------------------------------< EventIterator >--

    /**
     * Create a event iterator for this queue using the passed generator
     * @param generator  the generator for generating the events for this queue
     * @return  a new event iterator or {@code null} if the queue is full.
     */
    public EventIterator getEvents(final EventGenerator generator) {
        return full
            ? null
            : new EventIterator() {
                private long position = 0;

                @Override
                public long getSize() {
                    if (generator.isDone()) {
                        // no more new events will be generated, so count just those
                        // that have already been iterated and those left in the queue
                        return position + queue.size();
                    } else {
                        // the generator is not yet done, so there's no way
                        // to know how many events may still be generated
                        return -1;
                    }
                }

                @Override
                public long getPosition() {
                    return position;
                }

                @Override
                public boolean hasNext() {
                    while (queue.isEmpty()) {
                        if (generator.isDone()) {
                            return false;
                        } else {
                            generator.generate();
                        }
                    }
                    return true;
                }

                @Override
                public void skip(long skipNum) {
                    // generate events until all events to skip have been queued
                    while (skipNum > queue.size()) {
                        // drop all currently queued events as we're skipping them all
                        position += queue.size();
                        skipNum -= queue.size();
                        queue.clear();

                        // generate more events if possible, otherwise fail
                        if (!generator.isDone()) {
                            generator.generate();
                        } else {
                            throw new NoSuchElementException("Not enough events to skip");
                        }
                    }

                    // the remaining events to skip are guaranteed to all be in the
                    // queue, so we can just drop those events and advance the position
                    queue.subList(0, (int) skipNum).clear();
                    position += skipNum;
                }

                @Override
                public Event nextEvent() {
                    if (hasNext()) {
                        position++;
                        return queue.removeFirst();
                    } else {
                        throw new NoSuchElementException();
                    }
                }

                @Override
                public Event next() {
                    return nextEvent();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
        };
    }

}
