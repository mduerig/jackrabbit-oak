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

import org.apache.jackrabbit.oak.kernel.JsopOp.Add;
import org.apache.jackrabbit.oak.kernel.JsopOp.Copy;
import org.apache.jackrabbit.oak.kernel.JsopOp.Move;
import org.apache.jackrabbit.oak.kernel.JsopOp.Remove;
import org.apache.jackrabbit.oak.kernel.JsopOp.Set;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This {@link ConflictHandler} implementation wraps another conflict handler
 * and forwards all calls to the wrapped handler. Sub classes may override
 * methods of this class and intercept calls they are interested in.
 */
public class ConflictHandlerWrapper implements ConflictHandler {
    private final ConflictHandler handler;

    public ConflictHandlerWrapper(ConflictHandler handler) {
        this.handler = handler;
    }

    @Override
    public void parentNotFound(Add add, NodeState base, NodeBuilder rootBuilder) {
        handler.parentNotFound(add, base, rootBuilder);
    }

    @Override
    public void nodeExists(Add add, NodeState base, NodeBuilder rootBuilder) {
        handler.nodeExists(add, base, rootBuilder);
    }

    @Override
    public void nodeNotFound(Remove remove, NodeState base, NodeBuilder rootBuilder) {
        handler.nodeNotFound(remove, base, rootBuilder);
    }

    @Override
    public void parentNotFound(Set set, NodeState base, NodeBuilder rootBuilder) {
        handler.parentNotFound(set, base, rootBuilder);
    }

    @Override
    public void propertyValueConflict(Set set, NodeState base, NodeBuilder rootBuilder) {
        handler.propertyValueConflict(set, base, rootBuilder);
    }

    @Override
    public void sourceNotFound(Move move, NodeState base, NodeBuilder rootBuilder) {
        handler.sourceNotFound(move, base, rootBuilder);
    }

    @Override
    public void targetParentNotFound(Move move, NodeState base, NodeBuilder rootBuilder) {
        handler.targetParentNotFound(move, base, rootBuilder);
    }

    @Override
    public void targetNodeExists(Move move, NodeState base, NodeBuilder rootBuilder) {
        handler.targetNodeExists(move, base, rootBuilder);
    }

    @Override
    public void sourceNotFound(Copy copy, NodeState base, NodeBuilder rootBuilder) {
        handler.sourceNotFound(copy, base, rootBuilder);
    }

    @Override
    public void targetParentNotFound(Copy copy, NodeState base, NodeBuilder rootBuilder) {
        handler.targetParentNotFound(copy, base, rootBuilder);
    }

    @Override
    public void targetNodeExists(Copy copy, NodeState base, NodeBuilder rootBuilder) {
        handler.targetNodeExists(copy, base, rootBuilder);
    }
}
