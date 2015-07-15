package com.github.akka.couchbase.entities;

import akka.persistence.SnapshotMetadata;

public class SnapshotMetadataEntity {
    private String persistenceId;
    private long sequenceNr;
    private SnapshotMetadata snapshotMetadata;
    private long timestamp;

    private String payloadClassName;
    private String toString;

    public SnapshotMetadata getSnapshotMetadata() {
        return snapshotMetadata;
    }

    public void setSnapshotMetadata(SnapshotMetadata snapshotMetadata) {
        this.snapshotMetadata = snapshotMetadata;
    }

    public String getPersistenceId() {
        return persistenceId;
    }

    public void setPersistenceId(String persistenceId) {
        this.persistenceId = persistenceId;
    }

    public long getSequenceNr() {
        return sequenceNr;
    }

    public void setSequenceNr(long sequenceNr) {
        this.sequenceNr = sequenceNr;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public static String generateId(String prefix, String persistenceId, long sequenceNr) { return prefix + "SnapshotMetadataEntity."+persistenceId+"."+sequenceNr; }

    public void setToString(String toString) {
        this.toString = toString;
    }
    public String getPayloadClassName() {
        return payloadClassName;
    }

    public void setPayloadClassName(String payloadClassName) {
        this.payloadClassName = payloadClassName;
    }
}
