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

package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.TreeImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;

import static com.google.common.base.Objects.equal;

/**
 * Jsop operations.
 */
public abstract class JsopOp {
    protected final String path;

    protected JsopOp(String path) {
        this.path = path;
    }

    /**
     * Apply the operation
     * @param base   base base state of the rebase in whose context this operation is applied.
     * @param builder node builder where the operation should be applied to.
     * @param conflictHandler  conflict handler for resolving any conflict which occurs when applying this operation.
     */
    public abstract void apply(NodeState base, NodeBuilder builder, ConflictHandler conflictHandler);

    public static JsopOp add(String path, NodeState nodeState) {
        return new Add(path, nodeState);
    }

    public static JsopOp remove(String path) {
        return new Remove(path);
    }

    public static JsopOp set(String path, PropertyState propertyState) {
        return new Set(path, propertyState);
    }

    public static JsopOp move(String path, String destination) {
        return new Move(path, destination);
    }

    public static JsopOp copy(String path, String destination) {
        return new Copy(path, destination);
    }

    public static class Add extends JsopOp {
        private final NodeState nodeState;

        public Add(String path, NodeState nodeState) {
            super(path);
            this.nodeState = nodeState;
        }

        @Override
        public String toString() {
            return "+\"" + path + "\":" + nodeState;
        }

        @Override
        public void apply(NodeState base, NodeBuilder builder, ConflictHandler conflictHandler) {
            String parentPath = PathUtils.getParentPath(path);
            String name = PathUtils.getName(path);
            NodeBuilder affectedNode = getAffectedNode(builder, parentPath);
            if (affectedNode == null) {
                conflictHandler.parentNotFound(this, base, builder);
                return;
            }
            if (affectedNode.hasChildNode(name)) {
                conflictHandler.nodeExists(this, base, builder);
                return;
            }
            affectedNode.setNode(name, nodeState);
            updateChildOrderNodeAdded(affectedNode, name);
        }
    }

    public static class Remove extends JsopOp {
        public Remove(String path) {
            super(path);
        }

        @Override
        public String toString() {
            return "-\"" + path + '"';
        }

        @Override
        public void apply(NodeState base, NodeBuilder builder, ConflictHandler conflictHandler) {
            String parentPath = PathUtils.getParentPath(path);
            String name = PathUtils.getName(path);
            NodeBuilder affectedNode = getAffectedNode(builder, parentPath);
            if (affectedNode == null || !affectedNode.hasChildNode(name)) {
                conflictHandler.nodeNotFound(this, base, builder);
                return;
            }
            affectedNode.removeNode(name);
            updateChildOrderNodeRemoved(affectedNode, name);
        }
    }

    public static class Set extends JsopOp {
        private final PropertyState propertyState;

        public Set(String path, PropertyState propertyState) {
            super(path);
            this.propertyState = propertyState;
        }

        @Override
        public String toString() {
            return "^\"" + path + "\":" + propertyState;
        }

        @Override
        public void apply(NodeState base, NodeBuilder builder, ConflictHandler conflictHandler) {
            String parentPath = PathUtils.getParentPath(path);
            String name = PathUtils.getName(path);
            NodeBuilder affectedNode = getAffectedNode(builder, parentPath);
            if (affectedNode == null) {
                conflictHandler.parentNotFound(this, base, builder);
                return;
            }

            PropertyState baseProperty = getProperty(base, parentPath, name);
            PropertyState headProperty = affectedNode.getProperty(name);
            boolean theirsChanged = !equal(baseProperty, headProperty);
            if (theirsChanged && !equal(headProperty, propertyState)) {
                conflictHandler.propertyValueConflict(this, base, builder);
                return;
            }

            if (propertyState == null) {
                affectedNode.removeProperty(name);
            }
            else {
                affectedNode.setProperty(propertyState);
            }
        }

        private static PropertyState getProperty(NodeState nodeState, String parentPath, String name) {
            for (String childName : PathUtils.elements(parentPath)) {
                nodeState = nodeState.getChildNode(childName);
                if (nodeState == null) {
                    return null;
                }
            }

            return nodeState.getProperty(name);
        }
    }

    public static class Move extends JsopOp {
        private final String destination;

        public Move(String path, String destination) {
            super(path);
            this.destination = destination;
        }

        @Override
        public String toString() {
            return ">\"" + path + "\":\"" + destination + '"';
        }

        @Override
        public void apply(NodeState base, NodeBuilder builder, ConflictHandler conflictHandler) {
            String parentPath = PathUtils.getParentPath(path);
            String name = PathUtils.getName(path);
            NodeBuilder affectedParent = getAffectedNode(builder, parentPath);
            if (affectedParent == null || !affectedParent.hasChildNode(name)) {
                conflictHandler.sourceNotFound(this, base, builder);
                return;
            }

            String targetParentPath = PathUtils.getParentPath(destination);
            String targetName = PathUtils.getName(destination);
            NodeBuilder targetParent = getAffectedNode(builder, targetParentPath);
            if (targetParent == null) {
                conflictHandler.targetParentNotFound(this, base, builder);
                return;
            }
            if (targetParent.hasChildNode(targetName)) {
                conflictHandler.targetNodeExists(this, base, builder);
                return;
            }

            targetParent.setNode(targetName, affectedParent.child(name).getNodeState());
            affectedParent.removeNode(name);

            updateChildOrderNodeAdded(targetParent, targetName);
            updateChildOrderNodeRemoved(affectedParent, name);
        }
    }

    public static class Copy extends JsopOp {
        private final String destination;

        public Copy(String path, String destination) {
            super(path);
            this.destination = destination;
        }

        @Override
        public String toString() {
            return "*\"" + path + "\":\"" + destination + '"';
        }

        @Override
        public void apply(NodeState base, NodeBuilder builder, ConflictHandler conflictHandler) {
            String parentPath = PathUtils.getParentPath(path);
            String name = PathUtils.getName(path);
            NodeBuilder affectedParent = getAffectedNode(builder, parentPath);
            if (affectedParent == null || !affectedParent.hasChildNode(name)) {
                conflictHandler.sourceNotFound(this, base, builder);
                return;
            }

            String targetParentPath = PathUtils.getParentPath(destination);
            String targetName = PathUtils.getName(destination);
            NodeBuilder targetParent = getAffectedNode(builder, targetParentPath);
            if (targetParent == null) {
                conflictHandler.targetParentNotFound(this, base, builder);
                return;
            }
            if (targetParent.hasChildNode(targetName)) {
                conflictHandler.targetNodeExists(this, base, builder);
                return;
            }

            targetParent.setNode(targetName, affectedParent.child(name).getNodeState());
            updateChildOrderNodeAdded(targetParent, targetName);
        }
    }

    //------------------------------------------------------------< private >---

    private static void updateChildOrderNodeRemoved(NodeBuilder parent, String name) {
        PropertyState childOrder = parent.getProperty(TreeImpl.OAK_CHILD_ORDER);
        if (childOrder != null) {
            PropertyBuilder<String> builder = MemoryPropertyBuilder.copy(Type.STRING, childOrder);
            builder.removeValue(name);
            parent.setProperty(builder.getPropertyState());
        }
    }

    private static void updateChildOrderNodeAdded(NodeBuilder parent, String name) {
        PropertyState childOrder = parent.getProperty(TreeImpl.OAK_CHILD_ORDER);
        if (childOrder != null) {
            PropertyBuilder<String> builder = MemoryPropertyBuilder.copy(Type.STRING, childOrder);
            builder.addValue(name);
            parent.setProperty(builder.getPropertyState());
        }
    }

    private static NodeBuilder getAffectedNode(NodeBuilder builder, String path) {
        for (String name : PathUtils.elements(path)) {
            if (builder.hasChildNode(name)) {
                builder = builder.child(name);
            }
            else {
                return null;
            }
        }
        return builder;
    }
}
