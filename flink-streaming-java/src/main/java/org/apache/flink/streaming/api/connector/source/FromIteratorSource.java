package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.connector.source.Source;

import java.util.Collection;

public class FromIteratorSource implements Source<T, ElementsSplit<T>, Collection<ElementsSplit<T>>> {
}
