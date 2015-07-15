package com.github.akka.couchbase;

import akka.dispatch.Futures;
import akka.japi.Procedure;
import akka.persistence.PersistentConfirmation;
import akka.persistence.PersistentId;
import akka.persistence.PersistentRepr;
import akka.persistence.journal.japi.AsyncWriteJournal;
import akka.serialization.SerializationExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;
import scala.concurrent.Promise;

/**
 * Implementation of AsyncWriteJournal for Couchbase
 * @author Yousef Fadila
 */
public class CouchbaseAsyncWriteJournal extends AsyncWriteJournal {
    private static final Logger log = LoggerFactory.getLogger(CouchbaseAsyncWriteJournal.class);

    AkkaPersistenceImpl akkaPersistenceImpl;

    public CouchbaseAsyncWriteJournal() {
        this.akkaPersistenceImpl = AkkaPersistenceImpl.INSTANCE;
    }

    @Override
    public Future<Void> doAsyncWriteMessages(Iterable<PersistentRepr> persistentReprs) {
        Promise<Void> promise = Futures.promise();
        akkaPersistenceImpl.doWriteJournal(persistentReprs, SerializationExtension.get(context().system()))
                .subscribe(n -> {},
                        e -> {
                            log.error("doAsyncWriteMessages failed " + persistentReprs, e);
                            promise.failure((Throwable) e);
                        },
                        () -> {
                            log.debug("doAsyncWriteMessages succeed {}", persistentReprs);
                            promise.success(null);});
        return promise.future();
    }

    @Override
    public Future<Long> doAsyncReadHighestSequenceNr(String persistenceId, long fromSequenceNr) {
        Promise<Long> promise = Futures.promise();
        akkaPersistenceImpl.doAsyncReadHighestSequenceNr(persistenceId, fromSequenceNr)
                .subscribe(highestSequenceNr -> promise.success(highestSequenceNr),
                        e -> {
                            log.error("doAsyncReadHighestSequenceNr persistenceId {} failed {} ",persistenceId, e);
                            promise.failure((Throwable) e);
                        });
        return promise.future();
    }

    @Override
    public Future<Void> doAsyncReplayMessages(String persistenceId, long fromSequenceNr, long toSequenceNr, long max, Procedure<PersistentRepr> replayCallback) {
        Promise<Void> promise = Futures.promise();
        akkaPersistenceImpl.doAsyncReplyMessages(persistenceId, fromSequenceNr, toSequenceNr, max, replayCallback, context().system())
                .subscribe(n -> {},
                        e -> {
                            log.error("doAsyncReplayMessages persistenceId {} failed {} ",persistenceId, e);
                            promise.failure((Throwable) e);
                        },
                        () -> promise.success(null));
        return promise.future();
    }

    @Override
    public Future<Void> doAsyncDeleteMessagesTo(String persistenceId, long toSequenceNr, boolean permanent) {
        Promise<Void> promise = Futures.promise();
        akkaPersistenceImpl.doDeleteMessage(persistenceId, toSequenceNr, permanent)
                .subscribe(n -> {},
                        e -> {
                            log.error("doAsyncDeleteMessagesTo failed persistenceId {} {} ", persistenceId, e);
                            promise.failure((Throwable) e);
                        }, () -> promise.success(null));
        return promise.future();
    }

    @Override
    public Future<Void> doAsyncWriteConfirmations(Iterable<PersistentConfirmation> persistentConfirmations) {
        //This method is deprecated, implemented only to pass AKKA tests suits.
        Promise<Void> promise = Futures.promise();
        akkaPersistenceImpl.doWriteConfirmation(persistentConfirmations)
                .subscribe(n -> {},
                        e -> {
                            log.error("doAsyncWriteConfirmations failed ", (Throwable)e);
                            promise.failure((Throwable) e);
                        },
                        () -> promise.success(null));
        return promise.future();
    }

    @Override
    public Future<Void> doAsyncDeleteMessages(Iterable<PersistentId> persistentIds, boolean permanent) {

    //    This method is deprecated, implemented only to pass AKKA tests suits.
        Promise<Void> promise = Futures.promise();
        akkaPersistenceImpl.doDeleteMessage(persistentIds, permanent)
                .subscribe(n -> {},
                        e -> {
                            log.error("doAsyncDeleteMessages failed ", (Throwable)e);
                            promise.failure((Throwable) e);
                        },
                        () -> promise.success(null));
        return promise.future();
    }
}
