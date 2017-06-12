package com.cognitree.flume.sink.elasticsearch;

import org.apache.flume.Event;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Created by prashant
 */
public interface SerializerBuilder {

    /**
     * Converts the body of the event to the XContentBuilder format from the given format
     *
     * @param event
     * @return
     */
    XContentBuilder serialize(Event event);
}
