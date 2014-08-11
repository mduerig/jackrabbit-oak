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

package org.apache.jackrabbit.oak.plugins.observation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A composite {@code EventHandler} distributing all events
 * to all its composites.
 */
public class CompositeHandler implements EventHandler {
    private final Iterable<EventHandler> handlers;

    private CompositeHandler(Iterable<EventHandler> handlers) {
        this.handlers = handlers;
    }

    /**
     * Create a new composite {@code EventHandler} of the passed event handlers.
     * @param eventHandlers  event handlers
     * @return  a new composite event handler
     */
    @Nonnull
    public static EventHandler create(@Nonnull Iterable<? extends EventHandler> eventHandlers) {
        List<EventHandler> handlers = newArrayList();
        for (EventHandler handler : checkNotNull(eventHandlers)) {
            if (handler instanceof CompositeHandler) {
                addAll(handlers, ((CompositeHandler) handler).handlers);
            } else {
                handlers.add(handler);
            }
        }
        if (handlers.isEmpty()) {
            return new DefaultEventHandler() {
                @Override
                public EventHandler getChildHandler(String name, NodeState before, NodeState after) {
                    return null;
                }
            };
        } else if (handlers.size() == 1) {
            return handlers.get(0);
        } else {
            return new CompositeHandler(handlers);
        }
    }

    @Override
    public void enter(NodeState before, NodeState after) {
        for (EventHandler handler : handlers) {
            handler.enter(before, after);
        }
    }

    @Override
    public void leave(NodeState before, NodeState after) {
        for (EventHandler handler : handlers) {
            handler.leave(before, after);
        }
    }

    @Override
    public EventHandler getChildHandler(String name, NodeState before, NodeState after) {
        List<EventHandler> childHandlers = newArrayList();
        for (EventHandler handler : handlers) {
            EventHandler childHandler = handler.getChildHandler(name, before, after);
            if (childHandler != null) {
                childHandlers.add(childHandler);
            }
        }
        return childHandlers.isEmpty()
                ? null
                : new CompositeHandler(childHandlers);
    }

    @Override
    public void propertyAdded(PropertyState after) {
        for (EventHandler handler : handlers) {
            handler.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        for (EventHandler handler : handlers) {
            handler.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        for (EventHandler handler : handlers) {
            handler.propertyDeleted(before);
        }
    }

    @Override
    public void nodeAdded(String name, NodeState after) {
        for (EventHandler handler : handlers) {
            handler.nodeAdded(name, after);
        }
    }

    @Override
    public void nodeDeleted(String name, NodeState before) {
        for (EventHandler handler : handlers) {
            handler.nodeDeleted(name, before);
        }
    }

    @Override
    public void nodeMoved(String sourcePath, String name, NodeState moved) {
        for (EventHandler handler : handlers) {
            handler.nodeMoved(sourcePath, name, moved);
        }
    }

    @Override
    public void nodeReordered(String destName, String name, NodeState reordered) {
        for (EventHandler handler : handlers) {
            handler.nodeReordered(destName, name, reordered);
        }
    }
}
