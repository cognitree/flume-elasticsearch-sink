package com.cognitree.flume.sink.elasticsearch;

/**
 * Created by prashant
 */
import org.apache.flume.Event;

public interface IndexBuilder {

    /**
     * Returns index name
     *
     * @param event
     * @return
     */
    String getIndexName(Event event);

    /**
     * Return Index Type
     *
     * @param event
     * @return
     */
    String getIndexType(Event event);

    /**
     * Returns Index Id
     *
     * @param event
     * @return
     */
    String getIndexId(Event event);
}
