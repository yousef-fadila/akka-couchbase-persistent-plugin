package com.github.akka.couchbase.entities;

import java.util.SortedMap;
import java.util.TreeMap;

public class JournalContainer {
    private String persistenceId;

    private TreeMap<Long,Journal> journals = new TreeMap<>();

    public String getPersistenceIdId() {
        return persistenceId;
    }

    public void setPersistenceId(String persistenceId) {
        this.persistenceId = persistenceId;
    }

    public SortedMap<Long, Journal> getJournals() {
        return journals;
    }

    public void setJournals(TreeMap<Long, Journal> journals) {
        this.journals = journals;
    }

    public static String generateId(String idPrefix, String persistenceId) {

        return idPrefix + "JournalContainer."+persistenceId;
    }
}
