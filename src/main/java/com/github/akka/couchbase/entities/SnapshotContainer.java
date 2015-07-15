package com.github.akka.couchbase.entities;

import java.util.HashMap;
import java.util.Map;


public class SnapshotContainer {
    private String id;

    private Map<Long, SnapshotMetadataEntity> snapshots = new HashMap<>();

    public Map<Long, SnapshotMetadataEntity> getSnapshots() {
        return snapshots;
    }

    public void setSnapshots(Map<Long, SnapshotMetadataEntity> snapshots) {
        this.snapshots = snapshots;
    }

    public static String generateId(String prefix, String persistenceId) {
        return prefix +"SnapshotContainer."+persistenceId;
    }
}
