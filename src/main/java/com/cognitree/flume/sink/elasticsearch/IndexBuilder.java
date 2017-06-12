package com.cognitree.flume.sink.elasticsearch;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

/**
 * Created by prashant
 */
public interface IndexBuilder extends Configurable {

    /**
     * Returns index name
     */
    String getIndexName(Event event);

    /**
     * Return Index Type
     */
    String getIndexType(Event event);

    /**
     * Returns Index Id
     */
    String getIndexId(Event event);
}
