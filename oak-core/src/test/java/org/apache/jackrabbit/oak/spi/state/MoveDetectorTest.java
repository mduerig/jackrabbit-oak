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

package org.apache.jackrabbit.oak.spi.state;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.junit.Before;
import org.junit.Test;

/**
 * TODO document
 */
public class MoveDetectorTest {
    private NodeState root;

    @Before
    public void setup() {
        NodeBuilder rootBuilder = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder test = rootBuilder.child("test");
        test.setProperty("a", 1);
        test.setProperty("b", 2);
        test.setProperty("c", 3);
        test.child("x");
        test.child("y");
        test.child("z");
        root = rootBuilder.getNodeState();
    }

    @Test
    public void simpleMove() {
        NodeState moved = move(root.builder(), "/test/x", "/test/y/xx").getNodeState();

        final Set<String> movedPaths = MoveDetector.findMovedPaths(root, moved);
        class TestDiff implements MoveAwareNodeStateDiff {
            private final String path;

            public TestDiff(String path) {
                this.path = path;
            }

            @Override
            public boolean move(String sourcePath, String destPath, NodeState moved) {
                assertEquals("/test/x", sourcePath);
                assertEquals("/test/y/xx", destPath);
                return true;
            }

            @Override
            public boolean propertyAdded(PropertyState after) {
                return true;
            }

            @Override
            public boolean propertyChanged(PropertyState before, PropertyState after) {
                return true;
            }

            @Override
            public boolean propertyDeleted(PropertyState before) {
                return true;
            }

            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                return true;
            }

            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                String childPath = PathUtils.concat(path, name);
                after.compareAgainstBaseState(before, new MoveDetector(new TestDiff(childPath), childPath, movedPaths));
                return true;
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                return true;
            }
        }

        moved.compareAgainstBaseState(root, new MoveDetector(new TestDiff("/"), "/", movedPaths));
    }

    @Test
    public void moveMoved() {
        NodeBuilder rootBuilder = root.builder();
        move(rootBuilder, "/test/x", "/test/y/xx");
        NodeState moved = move(rootBuilder, "/test/y/xx", "/test/z/xxx").getNodeState();

        final Set<String> movedPaths = MoveDetector.findMovedPaths(root, moved);
        class TestDiff implements MoveAwareNodeStateDiff {
            private final String path;

            public TestDiff(String path) {
                this.path = path;
            }

            @Override
            public boolean move(String sourcePath, String destPath, NodeState moved) {
                assertEquals("/test/x", sourcePath);
                assertEquals("/test/z/xxx", destPath);
                return true;
            }

            @Override
            public boolean propertyAdded(PropertyState after) {
                return true;
            }

            @Override
            public boolean propertyChanged(PropertyState before, PropertyState after) {
                return true;
            }

            @Override
            public boolean propertyDeleted(PropertyState before) {
                return true;
            }

            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                return true;
            }

            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                String childPath = PathUtils.concat(path, name);
                after.compareAgainstBaseState(before, new MoveDetector(new TestDiff(childPath), childPath, movedPaths));
                return true;
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                return true;
            }
        }

        moved.compareAgainstBaseState(root, new MoveDetector(new TestDiff("/"), "/", movedPaths));
    }

    private static NodeBuilder move(NodeBuilder builder, String source, String dest) {
        NodeBuilder sourceBuilder = getBuilder(builder, source);
        NodeBuilder destParentBuilder = getBuilder(builder, PathUtils.getParentPath(dest));
        sourceBuilder.moveTo(destParentBuilder, PathUtils.getName(dest));
        return builder;
    }

    private static NodeBuilder getBuilder(NodeBuilder builder, String path) {
        for (String name : PathUtils.elements(path)) {
            builder = builder.getChildNode(name);
        }
        return builder;
    }
}
