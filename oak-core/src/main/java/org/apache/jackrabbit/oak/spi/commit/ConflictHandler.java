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

package org.apache.jackrabbit.oak.spi.commit;

import org.apache.jackrabbit.oak.kernel.JsopOp.Add;
import org.apache.jackrabbit.oak.kernel.JsopOp.Copy;
import org.apache.jackrabbit.oak.kernel.JsopOp.Move;
import org.apache.jackrabbit.oak.kernel.JsopOp.Remove;
import org.apache.jackrabbit.oak.kernel.JsopOp.Set;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A {@code ConflictHandler} is responsible for handling conflicts which happen on
 * {@link org.apache.jackrabbit.oak.spi.state.NodeStoreBranch#rebase(org.apache.jackrabbit.oak.spi.commit.ConflictHandler)}}.
 *
 * This interface contains one method per type of conflict which might occur.
 */
public interface ConflictHandler {
    void parentNotFound(Add add, NodeState base, NodeBuilder rootBuilder);
    void nodeExists(Add add, NodeState base, NodeBuilder rootBuilder);

    void nodeNotFound(Remove remove, NodeState base, NodeBuilder rootBuilder);

    void parentNotFound(Set set, NodeState base, NodeBuilder rootBuilder);
    void propertyValueConflict(Set set, NodeState base, NodeBuilder rootBuilder);

    void sourceNotFound(Move move, NodeState base, NodeBuilder rootBuilder);
    void targetParentNotFound(Move move, NodeState base, NodeBuilder rootBuilder);
    void targetNodeExists(Move move, NodeState base, NodeBuilder rootBuilder);

    void sourceNotFound(Copy copy, NodeState base, NodeBuilder rootBuilder);
    void targetParentNotFound(Copy copy, NodeState base, NodeBuilder rootBuilder);
    void targetNodeExists(Copy copy, NodeState base, NodeBuilder rootBuilder);
}
