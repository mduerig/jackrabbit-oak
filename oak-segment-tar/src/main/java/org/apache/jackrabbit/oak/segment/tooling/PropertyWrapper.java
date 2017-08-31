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
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Objects.hash;
import static org.apache.jackrabbit.oak.segment.tooling.TypeMap.toOakType;
import static org.apache.jackrabbit.oak.segment.tooling.TypeMap.toToolType;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.tooling.filestore.Property;

public class PropertyWrapper implements Property {
    @Nonnull
    private final PropertyState property;

    public PropertyWrapper(@Nonnull PropertyState property) {
        this.property = checkNotNull(property);
    }

    @Nonnull
    public PropertyState getState() {
        return property;
    }

    @Nonnull
    @Override
    public String getName() {
        return property.getName();
    }

    @Nonnull
    @Override
    public Type<?> type() {
        return toToolType(property.getType());
    }

    @Override
    public int cardinality() {
        return property.count();
    }

    @SuppressWarnings("unchecked")
    private static <T> T convertValue(@Nonnull Type<T> type, @Nonnull Object value) {
        if (type == Type.BINARY) {
            return (T) new BlobWrapper((Blob) value);
        } else {
            return (T) value;
        }
    }

    @Nonnull
    @Override
    public <T> T value(Type<T> type, int index) {
        return convertValue(type, property.getValue(toOakType(type), index));
    }

    @Nonnull
    @Override
    public <T> Iterable<T> values(Type<T> type) {
        List<T> values = newArrayList();
        for (int k = 0; k < property.count(); k++) {
            values.add(convertValue(type, property.getValue(toOakType(type), k)));
        }
        return values;
    }

    @Override
    public boolean equals(Object other) {
        if (other == NULL_PROPERTY) {
            return false;
        }
        if (getClass() != other.getClass()) {
            throw new IllegalArgumentException(other.getClass() + " is not comparable with " + getClass());
        }
        if (this == other) {
            return true;
        }
        return property.equals(((PropertyWrapper) other).property);
    }

    @Override
    public int hashCode() {
        return hash(property);
    }

    @Override
    public String toString() {
        return "PropertyWrapper{property=" + property + '}';
    }
}
