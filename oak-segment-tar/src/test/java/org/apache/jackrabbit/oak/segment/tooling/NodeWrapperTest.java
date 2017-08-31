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
 *
 */

package org.apache.jackrabbit.oak.segment.tooling;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.tooling.filestore.Node;
import org.apache.jackrabbit.oak.tooling.filestore.Property;
import org.junit.Before;
import org.junit.Test;

public class NodeWrapperTest {

    private NodeState node;
    private NodeWrapper wrappedNode;

    @Before
    public void setup() {
        node = TemporaryFileStoreWithToolAPI.addContent(EMPTY_NODE.builder()).getNodeState();
        wrappedNode = new NodeWrapper(node);
    }

    @Test
    public void testGetState() {
        assertTrue(node == wrappedNode.getState());
    }

    @Test
    public void testChildNames() {
        Set<String> expectedChildNames = newHashSet(node.getChildNodeNames());
        Set<String> actualChildNames = newHashSet(wrappedNode.childNames());
        assertEquals(expectedChildNames, actualChildNames);
    }

    @Test
    public void testChildren() {
        Set<Node> expectedChildren = newHashSet(
                transform(node.getChildNodeEntries(), NodeWrapper::new));
        Set<Node> actualChildren = newHashSet(wrappedNode.children());

        assertEquals(expectedChildren, actualChildren);
    }

    @Test
    public void testNode() {
        assertEquals(new NodeWrapper(node.getChildNode("a")), wrappedNode.node("a"));
        assertEquals(new NodeWrapper(node.getChildNode("b")), wrappedNode.node("b"));
        assertEquals(new NodeWrapper(node.getChildNode("c")), wrappedNode.node("c"));
    }

    @Test
    public void testNonExistingNode() {
        assertEquals(Node.NULL_NODE, wrappedNode.node("any"));
    }

    @Test
    public void testNonExistingProperty() {
        assertEquals(PropertyWrapper.NULL_PROPERTY, wrappedNode.property("any"));
    }

    @Test
    public void testProperties() {
        Set<Property> expectedProperties = newHashSet(
                transform(node.getProperties(), PropertyWrapper::new));
        HashSet<Property> actualProperties = newHashSet(wrappedNode.properties());

        assertEquals(expectedProperties, actualProperties);
    }

    @Nonnull
    private static PropertyState getProperty(@Nonnull NodeState node, @Nonnull String name) {
        return checkNotNull(node.getProperty(name));
    }

    @Test
    public void testProperty() {
        assertEquals(new PropertyWrapper(getProperty(node, "u")), wrappedNode.property("u"));
        assertEquals(new PropertyWrapper(getProperty(node, "v")), wrappedNode.property("v"));
        assertEquals(new PropertyWrapper(getProperty(node, "w")), wrappedNode.property("w"));
    }

}
