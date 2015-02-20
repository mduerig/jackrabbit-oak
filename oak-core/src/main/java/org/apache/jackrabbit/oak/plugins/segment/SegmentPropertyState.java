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
 */
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.DATE;
import static org.apache.jackrabbit.oak.api.Type.DECIMAL;
import static org.apache.jackrabbit.oak.api.Type.DOUBLE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.PATH;
import static org.apache.jackrabbit.oak.api.Type.REFERENCE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.URI;
import static org.apache.jackrabbit.oak.api.Type.WEAKREFERENCE;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.AbstractPropertyState;
import org.apache.jackrabbit.oak.plugins.segment.Segment.Reader;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

/**
 * A property, which can read a value or list record from a segment. It
 * currently doesn't cache data.
 * <p>
 * Depending on the property type, this is a record of type "VALUE" or a record
 * of type "LIST" (for arrays).
 */
public class SegmentPropertyState implements PropertyState {
    private final Page page;
    private final PropertyTemplate template;

    public SegmentPropertyState(@Nonnull Page page, @Nonnull PropertyTemplate template) {
        this.page = checkNotNull(page);
        this.template = checkNotNull(template);
    }

    public Page getPage() {
        return page;
    }

    private ListRecord getValueList() {
        Page listPage = page;
        int size = 1;
        if (isArray()) {
            Reader reader = page.getReader();
            size = reader.readInt();
            if (size > 0) {
                listPage = reader.readPage();
            }
        }
        return new ListRecord(listPage, size);
    }

    Map<String, Page> getValueRecords() {
        if (getType().tag() == PropertyType.BINARY) {
            return emptyMap();
        }

        Map<String, Page> map = newHashMap();
        ListRecord values = getValueList();
        for (int i = 0; i < values.size(); i++) {
            Page valuePage = values.getEntry(i);
            String value = valuePage.readString(0);
            map.put(value, valuePage);
        }
        return map;
    }

    @Override @Nonnull
    public String getName() {
        return template.getName();
    }

    @Override
    public Type<?> getType() {
        return template.getType();
    }

    @Override
    public boolean isArray() {
        return getType().isArray();
    }

    @Override
    public int count() {
        if (isArray()) {
            return page.readInt(0);
        } else {
            return 1;
        }
    }

    @Override @Nonnull @SuppressWarnings("unchecked")
    public <T> T getValue(Type<T> type) {
        if (isArray()) {
            checkState(type.isArray());
            ListRecord values = getValueList();
            if (values.size() == 0) {
                return (T) emptyList();
            } else if (values.size() == 1) {
                return (T) singletonList(getValue(values.getEntry(0), type.getBaseType()));
            } else {
                Type<?> base = type.getBaseType();
                List<Object> list = newArrayListWithCapacity(values.size());
                for (Page valuePage : values.getEntries()) {
                    list.add(getValue(valuePage, base));
                }
                return (T) list;
            }
        } else {
            if (type.isArray()) {
                return (T) singletonList(getValue(page, type.getBaseType()));
            } else {
                return getValue(page, type);
            }
        }
    }

    @Override
    public long size() {
        return size(0);
    }

    @Override @Nonnull
    public <T> T getValue(Type<T> type, int index) {
        checkNotNull(type);
        checkArgument(!type.isArray(), "Type must not be an array type");

        ListRecord values = getValueList();
        checkElementIndex(index, values.size());
        return getValue(values.getEntry(index), type);
    }

    @SuppressWarnings("unchecked")
    private <T> T getValue(Page page, Type<T> type) {
        if (type == BINARY) {
            return (T) new SegmentBlob(page); // load binaries lazily
        }

        String value = page.readString(0);
        if (type == STRING || type == URI || type == DATE
                || type == NAME || type == PATH
                || type == REFERENCE || type == WEAKREFERENCE) {
            return (T) value; // no conversion needed for string types
        }

        Type<?> base = getType();
        if (base.isArray()) {
            base = base.getBaseType();
        }
        Converter converter = Conversions.convert(value, base);
        if (type == BOOLEAN) {
            return (T) Boolean.valueOf(converter.toBoolean());
        } else if (type == DECIMAL) {
            return (T) converter.toDecimal();
        } else if (type == DOUBLE) {
            return (T) Double.valueOf(converter.toDouble());
        } else if (type == LONG) {
            return (T) Long.valueOf(converter.toLong());
        } else {
            throw new UnsupportedOperationException(
                    "Unknown type: " + type);
        }
    }

    @Override
    public long size(int index) {
        ListRecord values = getValueList();
        checkElementIndex(index, values.size());
        return values.getEntry(0).readLength(0);
    }


    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        // optimize for common cases
        if (this == object) { // don't use fastEquals here due to value sharing
            return true;
        } else if (object instanceof SegmentPropertyState) {
            SegmentPropertyState that = (SegmentPropertyState) object;
            if (!template.equals(that.template)) {
                return false;
            } else if (page.equals(that.page)) {
                return true;
            }
        }
        // fall back to default equality check in AbstractPropertyState
        return object instanceof PropertyState
                && AbstractPropertyState.equal(this, (PropertyState) object);
    }

    @Override
    public int hashCode() {
        return AbstractPropertyState.hashCode(this);
    }

    @Override
    public String toString() {
        return AbstractPropertyState.toString(this);
    }

}
