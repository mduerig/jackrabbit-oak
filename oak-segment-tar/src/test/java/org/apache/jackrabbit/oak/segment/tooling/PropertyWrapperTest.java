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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.DECIMAL;
import static org.apache.jackrabbit.oak.api.Type.DOUBLE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.segment.tooling.TypeMap.toToolType;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.io.ByteSource;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.tooling.filestore.Binary;
import org.apache.jackrabbit.oak.tooling.filestore.Property.Type;
import org.junit.Test;

public class PropertyWrapperTest {
    private final Blob blob = new ArrayBasedBlob(new byte[]{0,1,2,3});

    private final PropertyState stringProperty = createProperty("string", "string", STRING);
    private final PropertyState binaryProperty = createProperty("binary", blob, BINARY);
    private final PropertyState longProperty = createProperty("long", 42L, LONG);
    private final PropertyState doubleProperty = createProperty("double", 0.42, DOUBLE);
    private final PropertyState booleanProperty = createProperty("boolean", true, BOOLEAN);
    private final PropertyState decimalProperty = createProperty("decimal", BigDecimal.valueOf(42), DECIMAL);

    private final PropertyWrapper wrappedStringProperty = new PropertyWrapper(stringProperty);
    private final PropertyWrapper wrappedBinaryProperty = new PropertyWrapper(binaryProperty);
    private final PropertyWrapper wrappedLongProperty = new PropertyWrapper(longProperty);
    private final PropertyWrapper wrappedDoubleProperty = new PropertyWrapper(doubleProperty);
    private final PropertyWrapper wrappedBooleanProperty = new PropertyWrapper(booleanProperty);
    private final PropertyWrapper wrappedDecimalProperty = new PropertyWrapper(decimalProperty);

    @Test
    public void testName() {
        assertEquals(stringProperty.getName(), wrappedStringProperty.getName());
    }

    @Test
    public void testType() {
        assertEquals(toToolType(stringProperty.getType()), wrappedStringProperty.type());
        assertEquals(toToolType(binaryProperty.getType()), wrappedBinaryProperty.type());
        assertEquals(toToolType(longProperty.getType()), wrappedLongProperty.type());
        assertEquals(toToolType(doubleProperty.getType()), wrappedDoubleProperty.type());
        assertEquals(toToolType(booleanProperty.getType()), wrappedBooleanProperty.type());
        assertEquals(toToolType(decimalProperty.getType()), wrappedDecimalProperty.type());
    }

    @Test
    public void testCardinality() {
        assertEquals(1, wrappedStringProperty.cardinality());
        assertEquals(1, wrappedBinaryProperty.cardinality());
        assertEquals(1, wrappedLongProperty.cardinality());
        assertEquals(1, wrappedDoubleProperty.cardinality());
        assertEquals(1, wrappedBooleanProperty.cardinality());
        assertEquals(1, wrappedDecimalProperty.cardinality());
    }

    abstract static class EquableByteSource extends ByteSource {
        @Nonnull
        private static ByteSource newByteSource(@Nonnull Blob blob) {
            return new EquableByteSource() {
                @Override
                public InputStream openStream() {
                    return blob.getNewStream();
                }
            };
        }

        @Nonnull
        private static ByteSource newByteSource(@Nonnull Binary binary) {
            return new EquableByteSource() {
                @Override
                public InputStream openStream() {
                    return binary.bytes();
                }
            };
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof EquableByteSource)) {
                return false;
            }
            EquableByteSource that = (EquableByteSource) other;
            try {
                return this.contentEquals(that);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    @Test
    public void testValue() throws IOException {
        String string = wrappedStringProperty.value(Type.STRING, 0);
        assertEquals(stringProperty.getValue(STRING), string);

        Blob expectedBinary = binaryProperty.getValue(BINARY);
        Binary actualBinary = wrappedBinaryProperty.value(Type.BINARY, 0);
        assertEquals(expectedBinary.length(), actualBinary.size());
        assertEquals(
                EquableByteSource.newByteSource(expectedBinary),
                EquableByteSource.newByteSource(actualBinary));

        Long long_ = wrappedLongProperty.value(Type.LONG, 0);
        assertEquals(longProperty.getValue(LONG), long_);

        Double double_ = wrappedDoubleProperty.value(Type.DOUBLE, 0);
        assertEquals(doubleProperty.getValue(DOUBLE), double_);

        Boolean boolean_ = wrappedBooleanProperty.value(Type.BOOLEAN, 0);
        assertEquals(booleanProperty.getValue(BOOLEAN), boolean_);

        BigDecimal decimal = wrappedDecimalProperty.value(Type.DECIMAL, 0);
        assertEquals(decimalProperty.getValue(DECIMAL), decimal);
    }

    @Nonnull
    private static <T> List<T> getValues(
            @Nonnull PropertyState property,
            @Nonnull org.apache.jackrabbit.oak.api.Type<T> type) {
        List<T> result = newArrayList();
        for (int k = 0; k < property.count(); k++) {
            result.add(property.getValue(type, k));
        }
        return result;
    }

    @Test
    public void testValues() {
        List<String> expectedStrings = getValues(stringProperty, STRING);
        List<String> actualStrings = newArrayList(wrappedStringProperty.values(Type.STRING));
        assertEquals(expectedStrings, actualStrings);

        List<Blob> expectedBinaries = getValues(binaryProperty, BINARY);
        List<Binary> actualBinaries = newArrayList(wrappedBinaryProperty.values(Type.BINARY));

        List<ByteSource> expectedBytes = transform(expectedBinaries, EquableByteSource::newByteSource);
        List<ByteSource> actualBytes = transform(actualBinaries, EquableByteSource::newByteSource);
        assertEquals(expectedBytes, actualBytes);

        List<Long> expectedSizes = transform(expectedBinaries, Blob::length);
        List<Long> actualSizes = transform(actualBinaries, Binary::size);
        assertEquals(expectedSizes, actualSizes);

        List<Long> expectedLongs = getValues(longProperty, LONG);
        List<Long> actualLongs = newArrayList(wrappedLongProperty.values(Type.LONG));
        assertEquals(expectedLongs, actualLongs);

        List<Double> expectedDoubles = getValues(doubleProperty, DOUBLE);
        List<Double> actualDoubles = newArrayList(wrappedDoubleProperty.values(Type.DOUBLE));
        assertEquals(expectedDoubles, actualDoubles);

        List<Boolean> expectedBooleans = getValues(booleanProperty, BOOLEAN);
        List<Boolean> actualBooleans = newArrayList(wrappedBooleanProperty.values(Type.BOOLEAN));
        assertEquals(expectedBooleans, actualBooleans);

        List<BigDecimal> expectedDecimals = getValues(decimalProperty, DECIMAL);
        List<BigDecimal> actualDecimals = newArrayList(wrappedDecimalProperty.values(Type.DECIMAL));
        assertEquals(expectedDecimals, actualDecimals);
    }

}
