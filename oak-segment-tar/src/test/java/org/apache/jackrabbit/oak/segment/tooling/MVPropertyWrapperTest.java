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
import static java.math.BigDecimal.valueOf;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.BOOLEANS;
import static org.apache.jackrabbit.oak.api.Type.DECIMAL;
import static org.apache.jackrabbit.oak.api.Type.DECIMALS;
import static org.apache.jackrabbit.oak.api.Type.DOUBLE;
import static org.apache.jackrabbit.oak.api.Type.DOUBLES;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
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

public class MVPropertyWrapperTest {
    private final Blob blob = new ArrayBasedBlob(new byte[]{0,1,2,3});

    private final PropertyState stringProperty = createProperty("string",
            newArrayList("string1", "string2", "string3"), STRINGS);

    private final PropertyState binaryProperty = createProperty("binary",
            newArrayList(blob, blob, blob), BINARIES);

    private final PropertyState longProperty = createProperty("long",
            newArrayList(42L, 43L, 44L), LONGS);

    private final PropertyState doubleProperty = createProperty("double",
            newArrayList(0.42, 0.43, 0.44), DOUBLES);

    private final PropertyState booleanProperty = createProperty("boolean",
            newArrayList(true, true, false), BOOLEANS);

    private final PropertyState decimalProperty = createProperty("decimal",
            newArrayList(valueOf(42), valueOf(43), valueOf(44)), DECIMALS);

    private final PropertyState emptyProperty = createProperty("empty", newArrayList(), STRINGS);

    private final PropertyWrapper wrappedStringProperty = new PropertyWrapper(stringProperty);
    private final PropertyWrapper wrappedBinaryProperty = new PropertyWrapper(binaryProperty);
    private final PropertyWrapper wrappedLongProperty = new PropertyWrapper(longProperty);
    private final PropertyWrapper wrappedDoubleProperty = new PropertyWrapper(doubleProperty);
    private final PropertyWrapper wrappedBooleanProperty = new PropertyWrapper(booleanProperty);
    private final PropertyWrapper wrappedDecimalProperty = new PropertyWrapper(decimalProperty);
    private final PropertyWrapper wrappedEmptyProperty = new PropertyWrapper(emptyProperty);

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
        assertEquals(3, wrappedStringProperty.cardinality());
        assertEquals(3, wrappedBinaryProperty.cardinality());
        assertEquals(3, wrappedLongProperty.cardinality());
        assertEquals(3, wrappedDoubleProperty.cardinality());
        assertEquals(3, wrappedBooleanProperty.cardinality());
        assertEquals(3, wrappedDecimalProperty.cardinality());
        assertEquals(0, wrappedEmptyProperty.cardinality());
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
        for (int k = 0; k < 3; k++) {
            String string = wrappedStringProperty.value(Type.STRING, k);
            assertEquals(stringProperty.getValue(STRING, k), string);

            Blob expectedBinary = binaryProperty.getValue(BINARY, k);
            Binary actualBinary = wrappedBinaryProperty.value(Type.BINARY, k);
            assertEquals(expectedBinary.length(), actualBinary.size());
            assertEquals(
                    EquableByteSource.newByteSource(expectedBinary),
                    EquableByteSource.newByteSource(actualBinary));

            Long long_ = wrappedLongProperty.value(Type.LONG, k);
            assertEquals(longProperty.getValue(LONG, k), long_);

            Double double_ = wrappedDoubleProperty.value(Type.DOUBLE, k);
            assertEquals(doubleProperty.getValue(DOUBLE, k), double_);

            Boolean boolean_ = wrappedBooleanProperty.value(Type.BOOLEAN, k);
            assertEquals(booleanProperty.getValue(BOOLEAN, k), boolean_);

            BigDecimal decimal = wrappedDecimalProperty.value(Type.DECIMAL, k);
            assertEquals(decimalProperty.getValue(DECIMAL, k), decimal);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testNegativeIndex() {
        wrappedStringProperty.value(Type.STRING, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testOOBIndex() {
        wrappedStringProperty.value(Type.STRING, 4);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testValueOnEmptyProperty() {
        wrappedEmptyProperty.value(Type.STRING, 0);
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

        List<String> expectedEmpty = getValues(emptyProperty, STRING);
        List<String> actualEmpty = newArrayList(wrappedEmptyProperty.values(Type.STRING));
        assertEquals(expectedEmpty, actualEmpty);
    }

}
