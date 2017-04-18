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
package org.apache.jackrabbit.oak.plugins.value;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: document
 */
public interface OakValue {

    Blob getBlob() throws RepositoryException;

    /**
     * Same as {@link #getString()} unless that names and paths are returned in their
     * Oak representation instead of being mapped to their JCR representation.
     * @return  A String representation of the value of this property.
     */
    String getOakString() throws RepositoryException;
}