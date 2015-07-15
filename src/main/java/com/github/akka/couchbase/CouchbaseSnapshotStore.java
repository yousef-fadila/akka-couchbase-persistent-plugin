package com.github.akka.couchbase;

import akka.dispatch.Futures;
import akka.japi.Option;
import akka.persistence.SelectedSnapshot;
import akka.persistence.SnapshotMetadata;
import akka.persistence.SnapshotSelectionCriteria;
import akka.persistence.snapshot.japi.SnapshotStore;
import akka.serialization.SerializationExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;
import scala.concurrent.Promise;

/**
 * Implementation of SnapshotStore for Couchbase
 * @author Yousef Fadila
 */
public class CouchbaseSnapshotStore extends SnapshotStore {
    private static final Logger log = LoggerFactory.getLogger(CouchbaseSnapshotStore.class);
    AkkaPersistenceImpl akkaPersistenceImpl;

    public CouchbaseSnapshotStore() {
        this.akkaPersistenceImpl = AkkaPersistenceImpl.INSTANCE;
    }

    @Override
    public Future<Option<SelectedSnapshot>> doLoadAsync(String persistenceId, SnapshotSelectionCriteria snapshotSelectionCriteria) {

        Promise<Option<SelectedSnapshot>> promise = Futures.promise();
        akkaPersistenceImpl.findYoungestSnapshotByMaxSequence(persistenceId, snapshotSelectionCriteria, SerializationExtension.get(context().system()))
                .subscribe(SelectedSnapshotOption -> promise.success(SelectedSnapshotOption),
                        e -> {
                            log.error("findYoungestSnapshotByMaxSequence persistenceId {} failed {} ", persistenceId, e);
                            promise.failure((Throwable) e);
                        });
        return promise.future();
    }

    @Override
    public Future<Void> doSaveAsync(SnapshotMetadata snapshotMetadata, Object snapshotData) {

        Promise<Void> promise = Futures.promise();
        akkaPersistenceImpl.saveSnapshot(snapshotMetadata, snapshotData,  SerializationExtension.get(context().system()))
                .subscribe(n -> promise.success(null),
                        e -> {
                            log.error("doSaveAsync snapshotMetadata {} failed {} ", snapshotMetadata, e);
                            promise.failure((Throwable) e);
                        });
        return promise.future();
    }

    @Override
    public void onSaved(SnapshotMetadata snapshotMetadata) throws Exception {
        log.debug("onSaved is called snapshot {} ", snapshotMetadata);
    }

    @Override
    public void doDelete(SnapshotMetadata snapshotMetadata) {
        log.debug("doDelete is called snapshotMetadata {}", snapshotMetadata);
        try {
            akkaPersistenceImpl.deleteSnapshot(snapshotMetadata);
        } catch (Exception e) {
            log.error("exception in doDelete ", e);
        }
    }

    @Override
    public void doDelete(String persistenceId, SnapshotSelectionCriteria snapshotSelectionCriteria) {
        log.debug("doDelete is called persistenceId{} SnapshotSelectionCriteria {}", persistenceId, snapshotSelectionCriteria);
        try {
            akkaPersistenceImpl.deleteSnapshot(persistenceId, snapshotSelectionCriteria);
        } catch (Exception e) {
            log.error("exception in doDelete ", e);
        }
    }
}
