package com.github.akka.couchbase;

import com.couchbase.client.java.*;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Arrays;
import java.util.List;

/**
 * @author Yousef Fadila
 */

public enum CouchbaseAccessLayer {
    INSTANCE();

    private final Logger log = LoggerFactory.getLogger(CouchbaseAccessLayer.class);

    private List<String> nodes;
    private String bucketName;
    private String bucketPassword;

    private Bucket bucket;
    private Cluster cluster;
    private ReplicateTo replicateTo;
    private PersistTo persistTo;

    private CouchbaseAccessLayer() {

        loadConfig();
        try {
            connect();
        } catch (Exception e) {
            log.error("failed to connect to couchbase cluster, will retry again in the first DB access", e);
        }
    }

    private void loadConfig() {
        log.debug("CouchbaseAccessLayer loadConfig");
        try {
            ConfigManager configManager = ConfigManager.INSTANCE;

            nodes = Arrays.asList(configManager.getString("couchbase-persistence-v2.couchBase.servers").split(","));
            bucketName = configManager.getString("couchbase-persistence-v2.couchBase.bucketName");
            bucketPassword = configManager.getString("couchbase-persistence-v2.couchBase.pass");
            persistTo = PersistTo.valueOf(configManager.getString("couchbase-persistence-v2.couchBase.persistTo"));
            replicateTo = ReplicateTo.valueOf(configManager.getString("couchbase-persistence-v2.couchBase.replicateTo"));

        } catch (Exception e) {
            log.error("failed to loadConfig to couchbase cluster", e);
            throw new RuntimeException("failed to loadConfig to couchbase cluster " , e );
        }
    }

    private synchronized void connect() {

        if (bucket != null)
            return;

        try {
            //connect to the cluster and open the configured bucket
            this.cluster = CouchbaseCluster.create(nodes);
            this.bucket = cluster.openBucket(bucketName, bucketPassword);
        } catch (Exception e) {
            log.error("failed to connect to couchbase cluster", e);
            throw new RuntimeException("failed to connect to couchbase cluster " , e );
        }
    }
    // TODO, when to call this!? now we don't use spring
    public void preDestroy() {
        if (this.cluster != null) {
            this.cluster.disconnect();
        }
    }

    public Observable<BinaryDocument> getBinaryDocumentById(String documentId){
        if (bucket == null) {
            connect();
        }
        return  bucket.async().get(documentId, BinaryDocument.class);
    }

    public Observable<RawJsonDocument> getRawJsonDocumentByIdAsync(String documentId){
        if (bucket == null) {
            connect();
        }
        return  bucket.async().get(documentId, RawJsonDocument.class);
    }

    public RawJsonDocument getRawJsonDocumentById(String documentId){
        if (bucket == null) {
            connect();
        }

        return  bucket.get(documentId, RawJsonDocument.class);
    }

    public <T extends Document<?>> Observable<T> upsertDocument(T doc ) {

        if (bucket == null) {
            connect();
        }
        return bucket.async().upsert(doc, persistTo, replicateTo);
    }

    public <T extends Document<?>> Observable<T> insertDocument(T doc ) {

        if (bucket == null) {
            connect();
        }
        System.out.println("insertDocument");
        return bucket.async().insert(doc, persistTo, replicateTo);
    }

    public <T extends Document<?>> Observable<T> replaceDocument(T doc ) {
        if (bucket == null) {
            connect();
        }
        System.out.println("replaceDocument");
        return bucket.async().replace(doc, persistTo, replicateTo);
    }

    public <T extends Document<?>> Observable<T> deleteDocument(T doc)
    {
        if (bucket == null) {
            connect();
        }
        return bucket.async().remove(doc, persistTo, replicateTo);
    }

    public Observable<BinaryDocument> deleteBinaryDocumentById(String id) {
        if (bucket == null) {
            connect();
        }
        return bucket.async().remove(id, persistTo, replicateTo, BinaryDocument.class);
    }

}