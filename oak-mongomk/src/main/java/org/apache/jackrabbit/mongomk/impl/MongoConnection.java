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
package org.apache.jackrabbit.mongomk.impl;

import java.util.Arrays;

import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.SyncMongo;
import org.apache.jackrabbit.mongomk.model.NodeMongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;

/**
 * The {@code MongoConnection} contains connection properties for the {@code MongoDB}.
 */
public class MongoConnection {

    public static final String INITIAL_COMMIT_MESSAGE = "This is an autogenerated initial commit";
    public static final String INITIAL_COMMIT_PATH = "";
    public static final String INITIAL_COMMIT_DIFF = "+\"/\" : {}";

    private static final String COLLECTION_COMMITS = "commits";
    private static final String COLLECTION_NODES = "nodes";
    private static final String COLLECTION_SYNC = "sync";

    private final DB db;
    private final GridFS gridFS;
    private final Mongo mongo;

    /**
     * Constructs a new {@code MongoConnection}.
     *
     * @param host The host address.
     * @param port The port.
     * @param database The database name.
     * @throws Exception If an error occurred while trying to connect.
     */
    public MongoConnection(String host, int port, String database) throws Exception {
        mongo = new Mongo(host, port);
        db = mongo.getDB(database);
        gridFS = new GridFS(db);
    }

    /**
     * Initializes the underlying DB.
     *
     * @param force If true, clears the DB before initializing the collections.
     * Use with caution.
     */
    public void initializeDB(boolean force) {
        if (force) {
            clearDB();
        }
        initCommitCollection(force);
        initNodeCollection(force);
        initSyncCollection(force);
    }

    /**
     * Drops the collections of the underlying DB.
     */
    public void clearDB() {
        getNodeCollection().drop();
        getCommitCollection().drop();
        getSyncCollection().drop();
    }

    /**
     * Closes the underlying Mongo instance
     */
    public void close(){
        if (mongo != null){
            mongo.close();
        }
    }

    /**
     * Returns the commit {@link DBCollection}.
     *
     * @return The commit {@link DBCollection}.
     */
    public DBCollection getCommitCollection() {
        DBCollection commitCollection = db.getCollection(COLLECTION_COMMITS);
        commitCollection.setObjectClass(CommitMongo.class);
        return commitCollection;
    }

    /**
     * Returns the {@link DB}.
     *
     * @return The {@link DB}.
     */
    public DB getDB() {
        return db;
    }

    /**
     * Returns the {@link GridFS}.
     *
     * @return The {@link GridFS}.
     */
    public GridFS getGridFS() {
        return gridFS;
    }

    /**
     * Returns the sync {@link DBCollection}.
     *
     * @return The sync {@link DBCollection}.
     */
    public DBCollection getSyncCollection() {
        DBCollection syncCollection = db.getCollection(COLLECTION_SYNC);
        syncCollection.setObjectClass(SyncMongo.class);
        return syncCollection;
    }

    /**
     * Returns the node {@link DBCollection}.
     *
     * @return The node {@link DBCollection}.
     */
    public DBCollection getNodeCollection() {
        DBCollection nodeCollection = db.getCollection(COLLECTION_NODES);
        nodeCollection.setObjectClass(NodeMongo.class);
        return nodeCollection;
    }

    private void initCommitCollection(boolean force) {
        if (!force && db.collectionExists(MongoConnection.COLLECTION_COMMITS)){
            return;
        }
        DBCollection commitCollection = getCommitCollection();
        DBObject index = new BasicDBObject();
        index.put(CommitMongo.KEY_REVISION_ID, 1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.TRUE);
        commitCollection.ensureIndex(index, options);
        CommitMongo commit = new CommitMongo();
        commit.setAffectedPaths(Arrays.asList(new String[] { "/" }));
        commit.setBaseRevId(0L);
        commit.setDiff(INITIAL_COMMIT_DIFF);
        commit.setMessage(INITIAL_COMMIT_MESSAGE);
        commit.setPath(INITIAL_COMMIT_PATH);
        commit.setRevisionId(0L);
        commitCollection.insert(commit);
    }

    private void initNodeCollection(boolean force) {
        if (!force && db.collectionExists(MongoConnection.COLLECTION_NODES)){
            return;
        }
        DBCollection nodeCollection = getNodeCollection();
        DBObject index = new BasicDBObject();
        index.put(NodeMongo.KEY_PATH, 1L);
        index.put(NodeMongo.KEY_REVISION_ID, 1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.TRUE);
        nodeCollection.ensureIndex(index, options);
        NodeMongo root = new NodeMongo();
        root.setRevisionId(0L);
        root.setPath("/");
        nodeCollection.insert(root);
    }

    private void initSyncCollection(boolean force) {
        if (!force && db.collectionExists(MongoConnection.COLLECTION_SYNC)){
            return;
        }
        DBCollection headCollection = getSyncCollection();
        SyncMongo headMongo = new SyncMongo();
        headMongo.setHeadRevisionId(0L);
        headMongo.setNextRevisionId(1L);
        headCollection.insert(headMongo);
    }

}