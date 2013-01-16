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

package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHandlerWrapper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler.Resolution;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * MergingNodeStateDiff... TODO
 */
class MergingNodeStateDiff implements NodeStateDiff {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(MergingNodeStateDiff.class);

    private final NodeBuilder target;
    private final ConflictHandler conflictHandler;

    private MergingNodeStateDiff(NodeBuilder target, ConflictHandler conflictHandler) {
        this.target = target;
        this.conflictHandler = conflictHandler;
    }

    static void merge(NodeState theirs, NodeState ours, final NodeBuilder target,
            final ConflictHandler conflictHandler) {
        ours.compareAgainstBaseState(theirs, new MergingNodeStateDiff(
                checkNotNull(target), new ChildOrderConflictHandler(conflictHandler)));
    }

    //------------------------------------------------------< NodeStateDiff >---
    @Override
    public void propertyAdded(PropertyState ours) {
        Resolution resolution = conflictHandler.changeDeletedProperty(target, ours);

        switch (resolution) {
            case OURS:
                target.setProperty(ours);
                break;
            case THEIRS:
                target.removeProperty(ours.getName());
                break;
            case MERGED:
                break;
        }
    }

    @Override
    public void propertyChanged(PropertyState theirs, PropertyState ours) {
        checkArgument(theirs.getName().equals(ours.getName()),
                "before and after must have the same name");

        Resolution resolution = conflictHandler.changeChangedProperty(target, ours, theirs);

        switch (resolution) {
            case OURS:
                target.setProperty(ours);
                break;
            case THEIRS:
                target.setProperty(theirs);
                break;
            case MERGED:
                break;
        }
    }

    @Override
    public void propertyDeleted(PropertyState theirs) {
        Resolution resolution = conflictHandler.deleteChangedProperty(target, theirs);

        switch (resolution) {
            case OURS:
                target.removeProperty(theirs.getName());
                break;
            case THEIRS:
                target.setProperty(theirs);
                break;
            case MERGED:
                break;
        }
    }

    @Override
    public void childNodeAdded(String name, NodeState ours) {
        Resolution resolution = conflictHandler.changeDeletedNode(target, name, ours);

        switch (resolution) {
            case OURS:
                addChild(target, name, ours);
                break;
            case THEIRS:
                target.removeNode(name);
                break;
            case MERGED:
                break;
        }
    }

    @Override
    public void childNodeChanged(String name, NodeState before, NodeState after) {
        merge(before, after, target.child(name), conflictHandler);
    }

    @Override
    public void childNodeDeleted(String name, NodeState theirs) {
        ConflictHandler.Resolution resolution = conflictHandler.deleteChangedNode(target, name, theirs);

        switch (resolution) {
            case OURS:
                removeChild(target, name);
                break;
            case THEIRS:
                addChild(target, name, theirs);
                break;
            case MERGED:
                break;
        }
    }

    //-------------------------------------------------------------<private >---

    private static void addChild(NodeBuilder target, String name, NodeState state) {
        target.setNode(name, state);
        PropertyState childOrder = target.getProperty(TreeImpl.OAK_CHILD_ORDER);
        if (childOrder != null) {
            PropertyBuilder<String> builder = MemoryPropertyBuilder.copy(Type.STRING, childOrder);
            builder.addValue(name);
            target.setProperty(builder.getPropertyState());
        }
    }

    private static void removeChild(NodeBuilder target, String name) {
        target.removeNode(name);
        PropertyState childOrder = target.getProperty(TreeImpl.OAK_CHILD_ORDER);
        if (childOrder != null) {
            PropertyBuilder<String> builder = MemoryPropertyBuilder.copy(Type.STRING, childOrder);
            builder.removeValue(name);
            target.setProperty(builder.getPropertyState());
        }
    }

    /**
     * {@code ChildOrderConflictHandler} ignores conflicts on the
     * {@link TreeImpl#OAK_CHILD_ORDER} property. All other conflicts are forwarded
     * to the wrapped handler.
     */
    private static class ChildOrderConflictHandler extends ConflictHandlerWrapper {

        ChildOrderConflictHandler(ConflictHandler delegate) {
            super(delegate);
        }

        @Override
        public Resolution changeDeletedProperty(NodeBuilder parent,
                                                PropertyState ours) {
            if (isChildOrderProperty(ours)) {
                // orderBefore() on trees that were deleted
                return Resolution.THEIRS;
            } else {
                return handler.changeDeletedProperty(parent, ours);
            }
        }

        @Override
        public Resolution changeChangedProperty(NodeBuilder parent,
                                                PropertyState ours,
                                                PropertyState theirs) {
            if (isChildOrderProperty(ours)) {
                // concurrent orderBefore(), other changes win
                return Resolution.THEIRS;
            } else {
                return handler.changeChangedProperty(parent, ours, theirs);
            }
        }

        @Override
        public Resolution deleteChangedProperty(NodeBuilder parent,
                                                PropertyState theirs) {
            if (isChildOrderProperty(theirs)) {
                // remove trees that were reordered by another session
                return Resolution.THEIRS;
            } else {
                return handler.deleteChangedProperty(parent, theirs);
            }
        }

        //----------------------------< internal >----------------------------------

        private static boolean isChildOrderProperty(PropertyState p) {
            return TreeImpl.OAK_CHILD_ORDER.equals(p.getName());
        }
    }
}