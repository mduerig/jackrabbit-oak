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
package org.apache.jackrabbit.mongomk.impl.blob;

import org.apache.jackrabbit.mk.blobs.AbstractBlobStore;
import org.apache.jackrabbit.mk.util.StringUtils;
import org.apache.jackrabbit.mongomk.impl.model.MongoBlob;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * FIXME:
 * -Use level
 * -Create commands
 * -Implement GC
 */
public class MongoBlobStore extends AbstractBlobStore {

    public static final String COLLECTION_BLOBS = "blobs";

    private final DB db;

    public MongoBlobStore(DB db) {
        this.db = db;
        initBlobCollection();
    }

    @Override
    protected void storeBlock(byte[] digest, int level, byte[] data) throws Exception {
        String id = StringUtils.convertBytesToHex(digest);
        MongoBlob mongoBlob = new MongoBlob();
        mongoBlob.setId(id);
        mongoBlob.setData(data);
        getBlobCollection().insert(mongoBlob);
    }

    @Override
    protected byte[] readBlockFromBackend(BlockId blockId) throws Exception {
        String id = StringUtils.convertBytesToHex(blockId.getDigest());
        DBCollection blobCollection = getBlobCollection();
        QueryBuilder queryBuilder = QueryBuilder.start(MongoBlob.KEY_ID).is(id);
        DBObject query = queryBuilder.get();
        MongoBlob blobMongo = (MongoBlob)blobCollection.findOne(query);
        return blobMongo.getData();
    }

    @Override
    public void startMark() throws Exception {
    }

    @Override
    public int sweep() throws Exception {
        return 0;
    }

    @Override
    protected boolean isMarkEnabled() {
        return false;
    }

    @Override
    protected void mark(BlockId id) throws Exception {
    }

    private DBCollection getBlobCollection() {
        DBCollection collection = db.getCollection(COLLECTION_BLOBS);
        collection.setObjectClass(MongoBlob.class);
        return collection;
    }

    private void initBlobCollection() {
        if (db.collectionExists(COLLECTION_BLOBS)) {
            return;
        }
        DBCollection collection = getBlobCollection();
        DBObject index = new BasicDBObject();
        index.put(MongoBlob.KEY_ID, 1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.TRUE);
        collection.ensureIndex(index, options);
    }
}