package com.cognitree.flume.sink.elasticsearch;

import org.apache.flume.Event;

import java.util.Map;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * Created by prashant
 * <p>
 * This class create the index type and Id based on header
 */
public class HeaderBasedIndexBuilder implements IndexBuilder {

    /**
     * Returns the index name from the headers
     */
    @Override
    public String getIndexName(Event event) {
        Map<String, String> headers = event.getHeaders();
        String index = DEFAULT_ES_INDEX_NAME;
        if (headers.get(INDEX) != null) {
            index = headers.get(INDEX);
        }
        return index;
    }

    /**
     * Returns the index type from the headers
     */
    @Override
    public String getIndexType(Event event) {
        Map<String, String> headers = event.getHeaders();
        String indexType = DEFAULT_ES_INDEX_TYPE;
        if (headers.get(TYPE) != null) {
            indexType = headers.get(TYPE);
        }
        return indexType;
    }

    /**
     * Returns the index Id from the headers.
     */
    @Override
    public String getIndexId(Event event) {
        Map<String, String> headers = event.getHeaders();
        return headers.get(ID);
    }
}
