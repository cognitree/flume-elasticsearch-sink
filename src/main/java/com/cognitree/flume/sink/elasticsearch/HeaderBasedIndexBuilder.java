package com.cognitree.flume.sink.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * Created by prashant
 * <p>
 * This class create the index type and Id based on header
 */
public class HeaderBasedIndexBuilder implements IndexBuilder {

    private static final Logger logger = LoggerFactory.getLogger(HeaderBasedIndexBuilder.class);

    private String indexName;

    private String indexType;

    private String indexId;


    /**
     * Returns the index name from the headers
     */
    @Override
    public String getIndexName(Event event) {
        Map<String, String> headers = event.getHeaders();
        String index;
        if (headers.get(INDEX) != null) {
            index = headers.get(INDEX);
        } else if (this.indexName != null) {
            index = indexName;
        } else {
            index = DEFAULT_ES_INDEX_NAME;
        }
        return index;
    }

    /**
     * Returns the index type from the headers
     */
    @Override
    public String getIndexType(Event event) {
        Map<String, String> headers = event.getHeaders();
        String indexType;
        if (headers.get(TYPE) != null) {
            indexType = headers.get(TYPE);
        } else if (this.indexType != null) {
            indexType = this.indexType;
        } else {
            indexType = DEFAULT_ES_INDEX_TYPE;
        }
        return indexType;
    }

    /**
     * Returns the index Id from the headers.
     */
    @Override
    public String getIndexId(Event event) {
        Map<String, String> headers = event.getHeaders();
        String indexId = null;
        if (headers.get(ID) != null) {
            indexId = headers.get(ID);
        } else if (this.indexId != null) {
            indexId = this.indexId;
        }
        return indexId;
    }

    @Override
    public void configure(Context context) {
        indexName = Util.getContextValue(context, ES_INDEX_NAME);
        indexType = Util.getContextValue(context, ES_INDEX_TYPE);
        indexId = Util.getContextValue(context, ES_INDEX_ID);
        logger.trace("Header based Index builder, indexName [{}] indextype [{}] indexID [{}]",
                new Object[]{indexName, indexType, indexId});
    }
}
