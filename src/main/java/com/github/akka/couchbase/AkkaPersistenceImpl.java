package com.github.akka.couchbase;

import akka.actor.ActorSystem;
import akka.japi.Option;
import akka.japi.Pair;
import akka.japi.Procedure;
import akka.persistence.*;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.util.Blocking;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func2;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;
import com.github.akka.couchbase.entities.*;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import rx.Observable;
/**
 * Implementation of AKKA persistence of Journal.
 * @author Yousef Fadila
 */
public enum AkkaPersistenceImpl {
    INSTANCE();
    private final Logger log = LoggerFactory.getLogger(AkkaPersistenceImpl.class);

    CouchbaseAccessLayer couchbaseAccessLayer;
    int ttl;
    int operationTimeout;
    Gson gson;
    private String idPrefix;
    private int maxRetries;

    private int getExpiryInUnixTime(int ttl) {
        if (ttl == 0)
            return 0;
        int currentUnixTime = (int) (System.currentTimeMillis() / 1000L);
        return currentUnixTime + ttl;
    }

    private AkkaPersistenceImpl() {
        this.gson = new Gson();
        couchbaseAccessLayer = CouchbaseAccessLayer.INSTANCE;
        ttl = ConfigManager.INSTANCE.getInt("couchbase-persistence-v2.couchBase.expiryInSeconds", 0);
        operationTimeout = ConfigManager.INSTANCE.getInt("couchbase-persistence-v2.couchBase.operationTimeout", 5000);
        maxRetries = ConfigManager.INSTANCE.getInt("couchbase-persistence-v2.couchBase.maxRetries", 5);
        idPrefix = ConfigManager.INSTANCE.getString("couchbase-persistence-v2.couchBase.idPrefix");
        if (idPrefix == null) {
            idPrefix = "";
        }
        log.info("AkkaPersistenceImpl is ready, ttl {}, operationTimeout {}, idPrefix {} ",ttl, operationTimeout, idPrefix);
    }

    public Observable<Long> doAsyncReadHighestSequenceNr(String persistenceId, long fromSequenceNr) {

        return couchbaseAccessLayer.getRawJsonDocumentByIdAsync(JournalContainer.generateId(idPrefix, persistenceId))
                .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                .map(document-> {
                    JournalContainer journalContainer = gson.fromJson(document.content(), JournalContainer.class);
                    if(journalContainer.getJournals().size()==0)
                        return 0L;
                    return journalContainer.getJournals().lastKey();
                }).singleOrDefault(0L);
    }

    public  Observable<Boolean> doAsyncReplyMessages(String persistenceId, long fromSequenceNr, long toSequenceNr, long max, Procedure<PersistentRepr> replayCallback, ActorSystem system){

        return couchbaseAccessLayer.getRawJsonDocumentByIdAsync(JournalContainer.generateId(idPrefix, persistenceId))
                .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                .map(document -> {
                    JournalContainer journalContainer = gson.fromJson(document.content(), JournalContainer.class);
                    log.debug("doAsyncReplyMessages to persistenceId {} from {} to {}, journalContainer {} ", persistenceId, fromSequenceNr, toSequenceNr, journalContainer);

                    int replayed = 0;

                    for (Journal journal : journalContainer.getJournals().values()) {
                        if (replayed >= max) {
                            break;
                        }
                        if (journal.getSequenceNr() >= fromSequenceNr && journal.getSequenceNr() <= toSequenceNr) {

                            byte[] persistentReprBytes = Base64.getDecoder().decode(journal.getPersistentRepr().getBytes());
                            PersistentRepr impl = SerializationExtension.get(system).deserialize(persistentReprBytes, PersistentRepr.class).get();

                            Buffer buf = JavaConversions.asScalaBuffer(journal.getConfirmationIds());

                            PersistentRepr persistentRepr = impl.update(impl.sequenceNr(), impl.persistenceId(), journal.isDeleted(), impl.redeliveries(), buf.toList(), impl.confirmMessage(), impl.confirmTarget(), impl.sender());
                            log.debug("doAsyncReplyMessages going to reply to persistenceId {} with payload [{}]-{}", impl.persistenceId(), impl.payload().getClass(), impl.payload());
                            try {
                                replayCallback.apply(persistentRepr);
                            } catch (Exception e) {
                                log.error("Exception on replayCallback:", e);
                                throw new RuntimeException(e);
                            }

                            replayed++;
                        }
                    }
                    return Boolean.TRUE;
                });
    }


    public Observable doDeleteMessage(String persistentId, long toSequenceNr, boolean permanent) {

        return Observable
                .defer(() -> couchbaseAccessLayer.getRawJsonDocumentByIdAsync(JournalContainer.generateId(idPrefix, persistentId)))
                .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                .flatMap(document -> {
                    JournalContainer journalContainer = gson.fromJson(document.content(), JournalContainer.class);
					
                    long maxSeqNumber =  journalContainer.getJournals().lastKey() > toSequenceNr ? toSequenceNr : journalContainer.getJournals().lastKey();
					
                    for (long seq = journalContainer.getJournals().firstKey(); seq <= maxSeqNumber; seq++) {
                        if (permanent) {
                            journalContainer.getJournals().remove(seq);
                        } else {
                            Journal journal = journalContainer.getJournals().get(seq);
                            if (journal != null) {
                                journal.setDeleted(true);
                            }
                        }
                    }

                    if (journalContainer.getJournals().size() > 0) {
                        RawJsonDocument updatedDoc = RawJsonDocument.create(document.id(), gson.toJson(journalContainer), document.cas());
                        return couchbaseAccessLayer.replaceDocument(updatedDoc);
                    } else {
                        return couchbaseAccessLayer.deleteDocument(document);
                    }
                })
                .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                .retry(retryPredicate());
    }

    public Observable saveSnapshot(SnapshotMetadata snapshotMetadata, Object snapshotData, Serialization serialization) {

        log.debug("saveSnapshot started snapshotMetadata: {} ", snapshotMetadata);

        return  Observable.defer(()->{
            ByteBuf binaryData = Unpooled.wrappedBuffer(serialization.serialize(snapshotData).get());
            BinaryDocument doc = BinaryDocument.create(SnapshotMetadataEntity.generateId(idPrefix, snapshotMetadata.persistenceId(), snapshotMetadata.sequenceNr()), getExpiryInUnixTime(ttl), binaryData, 1L);
            return couchbaseAccessLayer.upsertDocument(doc);
        }).timeout(operationTimeout, TimeUnit.MILLISECONDS)
                .flatMap(document -> couchbaseAccessLayer.getRawJsonDocumentByIdAsync(SnapshotContainer.generateId(idPrefix, snapshotMetadata.persistenceId())))
                .singleOrDefault(null)
                .flatMap(document -> {
                    SnapshotContainer snapshotContainer;
                    if (document == null) {
                        log.debug("create snapshotContainer to PersistenceId {} ", snapshotMetadata.persistenceId());
                        snapshotContainer = new SnapshotContainer();
                    } else {
                        log.debug("read snapshotContainer of PersistenceId {} from couchbase ",  snapshotMetadata.persistenceId());
                        snapshotContainer = gson.fromJson(document.content(), SnapshotContainer.class);
                    }

                    SnapshotMetadataEntity snapshotMetadataEntity = new SnapshotMetadataEntity();
                    snapshotMetadataEntity.setPersistenceId(snapshotMetadata.persistenceId());
                    snapshotMetadataEntity.setTimestamp(snapshotMetadata.timestamp());
                    snapshotMetadataEntity.setSequenceNr(snapshotMetadata.sequenceNr());
                    snapshotMetadataEntity.setSnapshotMetadata(snapshotMetadata);
                    snapshotMetadataEntity.setPayloadClassName(snapshotData.getClass().getName());

                    log.debug("save snapshot seqNumber{} of PersistenceId {} to snapshotContainer ", snapshotMetadataEntity.getSequenceNr(),  snapshotMetadata.persistenceId());
                    snapshotContainer.getSnapshots().put(snapshotMetadataEntity.getSequenceNr(), snapshotMetadataEntity);
                    // always update the expiry time to TTL + now
                    RawJsonDocument updatedDoc = RawJsonDocument.create(SnapshotContainer.generateId(idPrefix, snapshotMetadataEntity.getPersistenceId()), getExpiryInUnixTime(ttl), gson.toJson(snapshotContainer), document == null ? 1 : document.cas());
                    if (document == null) {
                        return couchbaseAccessLayer.insertDocument(updatedDoc);
                    } else {
                        return couchbaseAccessLayer.replaceDocument(updatedDoc);
                    }
                })
                .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                .retry(retryPredicate());
    }

    public Observable<Option<SelectedSnapshot>> findYoungestSnapshotByMaxSequence(String persistenceId, SnapshotSelectionCriteria snapshotSelectionCriteria, Serialization serialization) {
        log.debug("findYoungestSnapshotByMaxSequence is called with persistenceId {}, snapshotSelectionCriteria {} ", persistenceId, snapshotSelectionCriteria);
        return  Observable
                .defer(() -> couchbaseAccessLayer.getRawJsonDocumentByIdAsync(SnapshotContainer.generateId(idPrefix, persistenceId)))
                .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                .singleOrDefault(null)
                .flatMap(document -> {
                    if (document == null) {
                        log.debug("SnapshotContainer {} is not found", SnapshotContainer.generateId(idPrefix, persistenceId));
                        return Observable.just(new Pair<SnapshotMetadataEntity, BinaryDocument>(null, null));
                    }

                    SnapshotContainer snapshotContainer = gson.fromJson(document.content(), SnapshotContainer.class);
                    long sequenceNr = 0;
                    SnapshotMetadataEntity maxSnapshot = null;
                    for (SnapshotMetadataEntity snapshotMetadataEntity : snapshotContainer.getSnapshots().values()) {
                        log.trace("Checking  snapshot {} ", snapshotMetadataEntity);
                        if (snapshotSelectionCriteria.matches(snapshotMetadataEntity.getSnapshotMetadata())) {
                            log.trace("snapshot {} fit the match ", snapshotMetadataEntity);
                            if (sequenceNr <= snapshotMetadataEntity.getSequenceNr()) {
                                sequenceNr = snapshotMetadataEntity.getSequenceNr();
                                maxSnapshot = snapshotMetadataEntity;
                            }
                        } else {
                            log.trace("Checking snapshot {} did not match ", snapshotMetadataEntity);
                        }
                    }
                    if (maxSnapshot != null) {
                        final SnapshotMetadataEntity matchedSnapshot = maxSnapshot;
                        return couchbaseAccessLayer.getBinaryDocumentById(SnapshotMetadataEntity.generateId(idPrefix, maxSnapshot.getPersistenceId(), maxSnapshot.getSequenceNr()))
                                .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                                .map(d -> new Pair<SnapshotMetadataEntity, BinaryDocument>(matchedSnapshot, d))
                                .singleOrDefault(new Pair<SnapshotMetadataEntity, BinaryDocument>(maxSnapshot, null));
                    } else {
                        return Observable.just(new Pair<SnapshotMetadataEntity, BinaryDocument>(null, null));
                    }
                }).map(pair -> {
                            SnapshotMetadataEntity matchedSnapshot = pair.first();
                            BinaryDocument snapshotData = pair.second();

                            if (matchedSnapshot != null && snapshotData != null) {
                                Class<?> payloadClass;
                                try {
                                    payloadClass = Class.forName(matchedSnapshot.getPayloadClassName());
                                } catch (Exception e) {
                                    log.warn("fail to find class, use Serializable ", e);
                                    payloadClass = Serializable.class;
                                }

                                byte[] bufArr = new byte[snapshotData.content().capacity()];
                                snapshotData.content().readBytes(bufArr);
                                SelectedSnapshot selectedSnapshot = SelectedSnapshot.create(matchedSnapshot.getSnapshotMetadata(), serialization.deserialize(bufArr, payloadClass).get());
                                log.debug("find matched snapshot {} - in container {}", selectedSnapshot, SnapshotContainer.generateId(idPrefix, persistenceId));
                                return Option.option(selectedSnapshot);
                            } else {
                                log.debug("no snapshot in the container{} fit match the criteria {} ", SnapshotContainer.generateId(idPrefix, persistenceId), snapshotSelectionCriteria);
                                return Option.<SelectedSnapshot>none();
                            }
                        }
                )
                .timeout(operationTimeout, TimeUnit.MILLISECONDS);
    };

    public Observable doWriteJournal(Iterable<PersistentRepr> persistentReprs, Serialization serialization) {

        log.debug("doWriteJournal {} started ", persistentReprs);

        return Observable.defer(()-> {

            Map<String, List<Journal>> journalsMap = new HashMap<>();

                for (PersistentRepr message : persistentReprs) {
                    log.debug("doWriteJournal persistenceId [{}] Message [{}]-{} using serializer{}", message.persistenceId(), message.payload().getClass(), message.payload(), serialization.serializerFor(message.payload().getClass()));
                    if (!journalsMap.containsKey(message.persistenceId())) {
                        journalsMap.put(message.persistenceId(), new ArrayList<>());
                    }
                    List<Journal> journals = journalsMap.get(message.persistenceId());
                    Journal journal = new Journal(message.persistenceId(), message.sequenceNr());
                    journal.setDeleted(message.deleted());
                    String serialized = Base64.getEncoder().encodeToString(serialization.serialize(message).get());

                    journal.setPersistentRepr(serialized);
                    journals.add(journal);
                }
                log.debug("doWriteJournal {} finish creating journals map ", persistentReprs);

            return Observable.from(journalsMap.entrySet());})
                .flatMap(e -> couchbaseAccessLayer.getRawJsonDocumentByIdAsync(JournalContainer.generateId(idPrefix, e.getKey()))
                                .singleOrDefault(null)
                                .map(d -> new Pair<>(e, d)))
                .flatMap(p -> {
                    JournalContainer journalContainer;
                    Map.Entry<String, List<Journal>> entrySet = (Map.Entry<String, List<Journal>>) p.first();
                    String persistenceId = entrySet.getKey();
                    RawJsonDocument document = p.second();
                    if (document == null) {
                        journalContainer = new JournalContainer();
                        journalContainer.setPersistenceId(persistenceId);
                    } else {
                        journalContainer = gson.fromJson(document.content(), JournalContainer.class);
                    }
                    entrySet.getValue().forEach((Journal journal) -> journalContainer.getJournals().put(journal.getSequenceNr(), journal));
                    int expiryInUnixTime = getExpiryInUnixTime(ttl);
                    RawJsonDocument updatedDoc = RawJsonDocument.create(JournalContainer.generateId(idPrefix, persistenceId), expiryInUnixTime, gson.toJson(journalContainer), document == null ? 1 : document.cas());
                    return  document == null ? couchbaseAccessLayer.insertDocument(updatedDoc): couchbaseAccessLayer.replaceDocument(updatedDoc);
                })
                .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                .retry(retryPredicate());
    }

    public void deleteSnapshot(SnapshotMetadata snapshotMetadata) {
        Blocking.blockForSingle(Observable
                .defer(() -> couchbaseAccessLayer.getRawJsonDocumentByIdAsync(SnapshotContainer.generateId(idPrefix, snapshotMetadata.persistenceId())))
                .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                .flatMap(document -> {
                    SnapshotContainer snapshotContainer = gson.fromJson(document.content(), SnapshotContainer.class);

                    snapshotContainer.getSnapshots().remove(snapshotMetadata.sequenceNr());

                    RawJsonDocument updatedDoc = RawJsonDocument.create(document.id(), gson.toJson(snapshotContainer), document.cas());
                    if (snapshotContainer.getSnapshots().size() == 0) {
                        return couchbaseAccessLayer.deleteDocument(updatedDoc);
                    } else {
                        return couchbaseAccessLayer.replaceDocument(updatedDoc);
                    }

                })
                .retry(retryPredicate()), operationTimeout, TimeUnit.MILLISECONDS);

        // Asynchronously remove the binary document, ignore the result
        couchbaseAccessLayer.deleteBinaryDocumentById(SnapshotMetadataEntity.generateId(idPrefix, snapshotMetadata.persistenceId(), snapshotMetadata.sequenceNr()))
                .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                .subscribe((n)->  log.debug("delete snapshot success"),
                        (e)-> log.warn("exception in delete snapshot", e));
        return;
    }

    public void deleteSnapshot(String persistenceId, SnapshotSelectionCriteria snapshotSelectionCriteria) {
        Blocking.blockForSingle(Observable
                .defer(() -> couchbaseAccessLayer.getRawJsonDocumentByIdAsync(SnapshotContainer.generateId(idPrefix, persistenceId)))
                .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                .flatMap(document -> {
                    SnapshotContainer snapshotContainer = gson.fromJson(document.content(), SnapshotContainer.class);
                    final List<Long> toDelete = snapshotContainer.getSnapshots().values().stream()
                            .filter(snapshot -> snapshotSelectionCriteria.matches(snapshot.getSnapshotMetadata()))
                            .map(SnapshotMetadataEntity::getSequenceNr)
                            .collect(Collectors.toList());

                    for (Long sequenceNr : toDelete) {
                        snapshotContainer.getSnapshots().remove(sequenceNr);
                    }

                    // Asynchronously remove the binary document, ignore the result
                    Observable.from(toDelete)
                            .map(sequenceNr -> couchbaseAccessLayer.deleteBinaryDocumentById(SnapshotMetadataEntity.generateId(idPrefix, persistenceId, sequenceNr)))
                            .timeout(operationTimeout, TimeUnit.MILLISECONDS)
                            .subscribe((n)->  log.debug("delete snapshot success"),
                                    (e)-> log.warn("exception in delete snapshot", e));

                    RawJsonDocument updatedDoc = RawJsonDocument.create(document.id(), gson.toJson(snapshotContainer), document.cas());
                    if (snapshotContainer.getSnapshots().size() == 0) {
                        return couchbaseAccessLayer.deleteDocument(updatedDoc);
                    } else {
                        return couchbaseAccessLayer.replaceDocument(updatedDoc);
                    }
                })
                .retry(retryPredicate()), operationTimeout, TimeUnit.MILLISECONDS);

        return;
    }

    @Deprecated
    public Observable doDeleteMessage(Iterable<PersistentId> persistentIds, boolean permanent) {
        return Observable
                .from(persistentIds)
                .flatMap(persistentIdObj -> {
                    String persistentId = persistentIdObj.persistenceId();
                    long sequenceNr = persistentIdObj.sequenceNr();
                    // blocking call! not optimistic but the function is anyway deprecated
                    RawJsonDocument document = couchbaseAccessLayer.getRawJsonDocumentById(JournalContainer.generateId(idPrefix, persistentId));
                    if (document != null) {
                        JournalContainer journalContainer = gson.fromJson(document.content(), JournalContainer.class);
                        if (permanent) {
                            journalContainer.getJournals().remove(sequenceNr);
                        } else {
                            Journal journal = journalContainer.getJournals().get(sequenceNr);
                            if (journal != null) {
                                journal.setDeleted(true);
                            }
                        }
                        if (journalContainer.getJournals().size() > 0) {
                            RawJsonDocument updatedDoc = RawJsonDocument.create(document.id(), gson.toJson(journalContainer), document.cas());
                            return couchbaseAccessLayer.replaceDocument(updatedDoc);
                        } else {
                            return couchbaseAccessLayer.deleteDocument(document);
                        }
                    }
                    return null;
                }) .retry(retryPredicate());
    }

    @Deprecated
    public Observable doWriteConfirmation(Iterable<PersistentConfirmation> persistentConfirmations) {
        return  Observable
                .defer(()-> Observable.from(persistentConfirmations))
                .flatMap(persistentConfirmation -> {
                    // blocking call! not optimistic but the function is anyway deprecated
                    RawJsonDocument document = couchbaseAccessLayer.getRawJsonDocumentById(JournalContainer.generateId(idPrefix, persistentConfirmation.persistenceId()));
                    if (document != null) {
                        JournalContainer journalContainer = gson.fromJson(document.content(), JournalContainer.class);

                        Journal journal = journalContainer.getJournals().get(persistentConfirmation.sequenceNr());
                        if (journal != null) {

                            journal.getConfirmationIds().add(persistentConfirmation.channelId());
                            RawJsonDocument updatedDoc = RawJsonDocument.create(document.id(), gson.toJson(journalContainer), document.cas());
                            return couchbaseAccessLayer.replaceDocument(updatedDoc);
                        }
                    }
                    return null;
                })
                .retry(retryPredicate());
    }

    /**
     * this predicate checks if there is a need to retry
     * the exceptions in here are not recoverable and there is no need to retry when getting them
     * @return predicate
     */
    private Func2<Integer, Throwable, Boolean> retryPredicate() {
        return (count, throwable) -> {
            boolean doRetry =  !(throwable instanceof NoSuchElementException) &&
                    !(throwable instanceof DocumentAlreadyExistsException) &&
                    !(throwable instanceof DocumentDoesNotExistException) &&
                    !(throwable instanceof TimeoutException) &&
                    count < maxRetries;

            log.debug("retryPredicate called: count {} -  max {} doRetry {} - throwable {}", count, maxRetries, doRetry, throwable);
            return doRetry;
        };
    }

}