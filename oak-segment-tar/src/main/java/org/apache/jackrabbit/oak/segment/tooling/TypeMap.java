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

import static com.google.common.base.Preconditions.checkState;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.tooling.filestore.Property;
import org.apache.jackrabbit.oak.tooling.filestore.Record;

public final class TypeMap {
    private static final Map<Type<?>, Property.Type<?>> TO_TOOL_PROPERTY_TYPE =
        ImmutableMap.<Type<?>, Property.Type<?>>builder()
            .put(Type.STRING, Property.Type.STRING)
            .put(Type.BINARY, Property.Type.BINARY)
            .put(Type.LONG, Property.Type.LONG)
            .put(Type.DOUBLE, Property.Type.DOUBLE)
            .put(Type.DATE, Property.Type.DATE)
            .put(Type.BOOLEAN, Property.Type.BOOLEAN)
            .put(Type.NAME, Property.Type.NAME)
            .put(Type.PATH, Property.Type.PATH)
            .put(Type.REFERENCE, Property.Type.REFERENCE)
            .put(Type.WEAKREFERENCE, Property.Type.WEAKREFERENCE)
            .put(Type.URI, Property.Type.URI)
            .put(Type.DECIMAL, Property.Type.DECIMAL)
        .build();

    private static final Map<Property.Type<?>, Type<?>> TO_OAK_PROPERTY_TYPE =
        ImmutableMap.<Property.Type<?>, Type<?>>builder()
            .put(Property.Type.STRING, Type.STRING)
            .put(Property.Type.BINARY, Type.BINARY)
            .put(Property.Type.LONG, Type.LONG)
            .put(Property.Type.DOUBLE, Type.DOUBLE)
            .put(Property.Type.DATE, Type.DATE)
            .put(Property.Type.BOOLEAN, Type.BOOLEAN)
            .put(Property.Type.NAME, Type.NAME)
            .put(Property.Type.PATH, Type.PATH)
            .put(Property.Type.REFERENCE, Type.REFERENCE)
            .put(Property.Type.WEAKREFERENCE, Type.WEAKREFERENCE)
            .put(Property.Type.URI, Type.URI)
            .put(Property.Type.DECIMAL, Type.DECIMAL)
        .build();

    private static final Map<RecordType, Record.Type> TO_TOOL_RECORD_TYPE =
        ImmutableMap.<RecordType, Record.Type>builder()
            .put(RecordType.LEAF, Record.Type.LEAF)
            .put(RecordType.BRANCH, Record.Type.BRANCH)
            .put(RecordType.BUCKET, Record.Type.BUCKET)
            .put(RecordType.LIST, Record.Type.LIST)
            .put(RecordType.VALUE, Record.Type.VALUE)
            .put(RecordType.BLOCK, Record.Type.BLOCK)
            .put(RecordType.TEMPLATE, Record.Type.TEMPLATE)
            .put(RecordType.NODE, Record.Type.NODE)
            .put(RecordType.BLOB_ID, Record.Type.BLOB_ID)
        .build();

    private static final Map<Record.Type, RecordType> TO_OAK_RECORD_TYPE =
        ImmutableMap.<Record.Type, RecordType>builder()
            .put(Record.Type.LEAF, RecordType.LEAF)
            .put(Record.Type.BRANCH, RecordType.BRANCH)
            .put(Record.Type.BUCKET, RecordType.BUCKET)
            .put(Record.Type.LIST, RecordType.LIST)
            .put(Record.Type.VALUE, RecordType.VALUE)
            .put(Record.Type.BLOCK, RecordType.BLOCK)
            .put(Record.Type.TEMPLATE, RecordType.TEMPLATE)
            .put(Record.Type.NODE, RecordType.NODE)
            .put(Record.Type.BLOB_ID, RecordType.BLOB_ID)
        .build();

    private TypeMap(){}

    @Nonnull
    public static Property.Type<?> toToolType(@Nonnull Type<?> type) {
        if (type.isArray()) {
            type = type.getBaseType();
        }
        checkState(TO_TOOL_PROPERTY_TYPE.containsKey(type), "No conversion for " + type);
        return TO_TOOL_PROPERTY_TYPE.get(type);
    }

    @Nonnull
    public static Type<?> toOakType(@Nonnull Property.Type<?> type) {
        checkState(TO_OAK_PROPERTY_TYPE.containsKey(type), "No conversion for " + type);
        return TO_OAK_PROPERTY_TYPE.get(type);
    }

    @Nonnull
    public static Record.Type toToolType(@Nonnull RecordType type) {
        checkState(TO_TOOL_RECORD_TYPE.containsKey(type), "No conversion for " + type);
        return TO_TOOL_RECORD_TYPE.get(type);
    }

    @Nonnull
    public static RecordType toOakType(@Nonnull Record.Type type) {
        checkState(TO_OAK_RECORD_TYPE.containsKey(type), "No conversion for " + type);
        return TO_OAK_RECORD_TYPE.get(type);
    }
}
