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
import static java.util.Objects.hash;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.tooling.filestore.Node;
import org.apache.jackrabbit.oak.tooling.filestore.Property;

public class NodeWrapper implements Node {
    @Nonnull
    private final NodeState node;

    public NodeWrapper(@Nonnull NodeState node) {
        this.node = checkNotNull(node);
    }

    public NodeWrapper(@Nonnull ChildNodeEntry nodeEntry) {
        this(nodeEntry.getNodeState());
    }

    @Nonnull
    public NodeState getState() {
        return node;
    }

    @Nonnull
    @Override
    public Iterable<String> childNames() {
        return node.getChildNodeNames();
    }

    @Nonnull
    @Override
    public Iterable<Node> children() {
        return transform(node.getChildNodeEntries(), NodeWrapper::new);
    }

    @Nonnull
    @Override
    public Node node(@Nonnull String name) {
        NodeState childNode = node.getChildNode(name);
        return childNode.exists()
                ? new NodeWrapper(childNode)
                : Node.NULL_NODE;
    }

    @Nonnull
    @Override
    public Iterable<Property> properties() {
        return transform(node.getProperties(), PropertyWrapper::new);
    }

    @Nonnull
    @Override
    public Property property(@Nonnull String name) {
        PropertyState property = node.getProperty(name);
        return property != null
                ? new PropertyWrapper(property)
                : Property.NULL_PROPERTY;
    }

    @Override
    public boolean equals(Object other) {
        if (other == NULL_NODE) {
            return false;
        }
        if (getClass() != other.getClass()) {
            throw new IllegalArgumentException(other.getClass() + " is not comparable with " + getClass());
        }
        if (this == other) {
            return true;
        }
        return node.equals(((NodeWrapper) other).node);
    }

    @Override
    public int hashCode() {
        return hash(node);
    }

    @Override
    public String toString() {
        return "NodeWrapper{node=" + node + '}';
    }
}
