package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Collection;
import java.util.UUID;

public class CollectionSplit<T> implements SourceSplit, Serializable {

    private final Collection<T> collection;
    private final String splitId;

    public CollectionSplit(Collection<T> collection) {
        this.collection = collection;
        this.splitId = UUID.randomUUID().toString();
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public Collection<T> getCollection() {
        return collection;
    }
}
