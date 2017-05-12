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

package org.apache.jackrabbit.oak.segment.io.raw;

public class RawTemplate {

    private final RawRecordId primaryType;

    private final RawRecordId[] mixins;

    private final boolean manyChildNodes;

    private final boolean zeroChildNodes;

    private final RawRecordId childNodeName;

    private final RawRecordId propertyNames;

    private final byte[] propertyTypes;

    RawTemplate(RawRecordId primaryType, RawRecordId[] mixins, boolean manyChildNodes, boolean zeroChildNodes, RawRecordId childNodeName, RawRecordId propertyNames, byte[] propertyTypes) {
        this.primaryType = primaryType;
        this.mixins = mixins;
        this.manyChildNodes = manyChildNodes;
        this.zeroChildNodes = zeroChildNodes;
        this.childNodeName = childNodeName;
        this.propertyNames = propertyNames;
        this.propertyTypes = propertyTypes;
    }

    public RawRecordId getPrimaryType() {
        return primaryType;
    }

    public RawRecordId[] getMixins() {
        return mixins;
    }

    public boolean hasManyChildNodes() {
        return manyChildNodes;
    }

    public boolean hasZeroChildNodes() {
        return zeroChildNodes;
    }

    public RawRecordId getChildNodeName() {
        return childNodeName;
    }

    public RawRecordId getPropertyNames() {
        return propertyNames;
    }

    public byte[] getPropertyTypes() {
        return propertyTypes;
    }

}
