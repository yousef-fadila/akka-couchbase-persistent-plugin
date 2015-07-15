package com.github.akka.couchbase.entities;

import java.util.ArrayList;
import java.util.List;

public class Journal {
    private String id;
    private String persistenceId;
    private long sequenceNr;
    private String persistentRepr;

    private List<String> confirmationIds;
    private boolean deleted;

    private String toString;

    public Journal(String persistenceId, long sequenceNr) {
        this.sequenceNr = sequenceNr;
        this.persistenceId = persistenceId;
        confirmationIds = new ArrayList<>();
        id = "Journal." + persistenceId + "." + sequenceNr;
    }

    public Journal() {
    }

    public List<String> getConfirmationIds() {
        return confirmationIds;
    }

    public String getPersistenceId() {
        return persistenceId;
    }

    public String getId() {
        return id;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public long getSequenceNr() {
        return sequenceNr;
    }

    public String getPersistentRepr() {
        return persistentRepr;
    }

    public void setPersistentRepr(String persistentRepr) {
        this.persistentRepr = persistentRepr;
    }

    public void setToString(String toString) {
        this.toString = toString;
    }

    public String getToString() {
        return toString;
    }
}