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

package org.apache.jackrabbit.oak.plugins.segment;

import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

/**
 * michid document
 */
public final class CachedPropertyState extends SegmentPropertyState {
    private final PropertyState cached;

    public CachedPropertyState(RecordId id, PropertyTemplate template) {
        super(id, template);
        cached = createProperty(getName(), super.getValue(getType()), getType());
    }

    @Override
    public int count() {
        return cached.count();
    }

    @Nonnull
    @Override
    public <T> T getValue(Type<T> type) {
        return cached.getValue(type);
    }

    @Override
    public long size() {
        return cached.size();
    }

    @Nonnull
    @Override
    public <T> T getValue(Type<T> type, int index) {
        return cached.getValue(type, index);
    }

    @Override
    public long size(int index) {
        return cached.size(index);
    }
}
