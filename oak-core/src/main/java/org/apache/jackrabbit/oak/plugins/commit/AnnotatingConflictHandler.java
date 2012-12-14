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

package org.apache.jackrabbit.oak.plugins.commit;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.kernel.JsopOp.Add;
import org.apache.jackrabbit.oak.kernel.JsopOp.Copy;
import org.apache.jackrabbit.oak.kernel.JsopOp.Move;
import org.apache.jackrabbit.oak.kernel.JsopOp.Remove;
import org.apache.jackrabbit.oak.kernel.JsopOp.Set;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_REP_MERGE_CONFLICT;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.REP_OURS;

/**
 * This {@link ConflictHandler} implementation resolves by skipping the
 * respective operation and in addition marks nodes where a conflict
 * occurred with the mixin {@code rep:MergeConflict}:
 *
 * <pre>
 * [rep:MergeConflict]
 *   mixin
 *   primaryitem rep:ours
 *   + rep:ours (nt:unstructured) protected IGNORE
 * </pre>
 * *
 * @see ConflictValidator
 *
 * TODO review/define conflict marker structure
 * TODO add conflict marker on the most specific node instead of on the root
 */
public class AnnotatingConflictHandler implements ConflictHandler {

    @Override
    public void parentNotFound(Add add, NodeState base, NodeBuilder rootBuilder) {
        NodeBuilder marker = addConflictMarker(rootBuilder);
        marker.child("parentNotFound").setProperty("op", add.toString());
    }

    @Override
    public void nodeExists(Add add, NodeState base, NodeBuilder rootBuilder) {
        NodeBuilder marker = addConflictMarker(rootBuilder);
        marker.child("nodeExists").setProperty("op", add.toString());
    }

    @Override
    public void nodeNotFound(Remove remove, NodeState base, NodeBuilder rootBuilder) {
        NodeBuilder marker = addConflictMarker(rootBuilder);
        marker.child("nodeNotFound").setProperty("op", remove.toString());
    }

    @Override
    public void parentNotFound(Set set, NodeState base, NodeBuilder rootBuilder) {
        NodeBuilder marker = addConflictMarker(rootBuilder);
        marker.child("parentNotFound").setProperty("op", set.toString());
    }

    @Override
    public void propertyValueConflict(Set set, NodeState base, NodeBuilder rootBuilder) {
        NodeBuilder marker = addConflictMarker(rootBuilder);
        marker.child("propertyValueConflict").setProperty("op", set.toString());
    }

    @Override
    public void sourceNotFound(Move move, NodeState base, NodeBuilder rootBuilder) {
        NodeBuilder marker = addConflictMarker(rootBuilder);
        marker.child("sourceNotFound").setProperty("op", move.toString());
    }

    @Override
    public void targetParentNotFound(Move move, NodeState base, NodeBuilder rootBuilder) {
        NodeBuilder marker = addConflictMarker(rootBuilder);
        marker.child("targetParentNotFound").setProperty("op", move.toString());
    }

    @Override
    public void targetNodeExists(Move move, NodeState base, NodeBuilder rootBuilder) {
        NodeBuilder marker = addConflictMarker(rootBuilder);
        marker.child("targetNodeExists").setProperty("op", move.toString());
    }

    @Override
    public void sourceNotFound(Copy copy, NodeState base, NodeBuilder rootBuilder) {
        NodeBuilder marker = addConflictMarker(rootBuilder);
        marker.child("sourceNotFound").setProperty("op", copy.toString());
    }

    @Override
    public void targetParentNotFound(Copy copy, NodeState base, NodeBuilder rootBuilder) {
        NodeBuilder marker = addConflictMarker(rootBuilder);
        marker.child("targetParentNotFound").setProperty("op", copy.toString());
    }

    @Override
    public void targetNodeExists(Copy copy, NodeState base, NodeBuilder rootBuilder) {
        NodeBuilder marker = addConflictMarker(rootBuilder);
        marker.child("targetNodeExists").setProperty("op", copy.toString());
    }

    //------------------------------------------------------------< private >---

    private static NodeBuilder addConflictMarker(NodeBuilder parent) {
        PropertyState jcrMixin = parent.getProperty(JCR_MIXINTYPES);
        List<String> mixins;
        if (jcrMixin == null) {
            mixins = Lists.newArrayList();
        }
        else {
            mixins = Lists.newArrayList(jcrMixin.getValue(NAMES));
        }
        if (!mixins.contains(MIX_REP_MERGE_CONFLICT)) {
            mixins.add(MIX_REP_MERGE_CONFLICT);
            parent.setProperty(JCR_MIXINTYPES, mixins, NAMES);
        }

        return parent.child(REP_OURS);
    }

}
