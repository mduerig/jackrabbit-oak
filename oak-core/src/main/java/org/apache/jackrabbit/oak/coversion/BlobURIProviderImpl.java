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

package org.apache.jackrabbit.oak.coversion;

import java.net.URI;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.conversion.BlobURIProvider;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.osgi.framework.BundleContext;

/**
 * Exposed OSGi service implementing the BlobURIProvider.
 * This is the service that components outside Oak should
 */
@Component( immediate = true)
@Service(BlobURIProvider.class)
public class BlobURIProviderImpl implements BlobURIProvider {

    private Whiteboard whiteboard;

    public BlobURIProviderImpl() {}

    @Activate
    public void activate(BundleContext bundleContext) {
        whiteboard = new OsgiWhiteboard(bundleContext);
    }

    public BlobURIProviderImpl(@Nonnull Whiteboard whiteboard) {
        this.whiteboard = whiteboard;
    }

    @Override
    public URI getURI(Blob blob) {
        List<BlobURIProvider> converters = WhiteboardUtils.getServices(whiteboard, BlobURIProvider.class);
        for (BlobURIProvider converter : converters) {
            URI result = converter.getURI(blob);
            if (result != null) {
                return result;
            }
        }
        return null;
    }
}
