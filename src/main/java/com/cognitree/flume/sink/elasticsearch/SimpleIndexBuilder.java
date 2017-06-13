package com.cognitree.flume.sink.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * Created by prashant
 */
public class SimpleIndexBuilder implements IndexBuilder {

    private static final Logger logger = LoggerFactory.getLogger(SimpleIndexBuilder.class);

    private String indexName;

    private String indexType;

    private String indexId;


    @Override
    public String getIndexName(Event event) {
        String index;
        if (this.indexName != null) {
            index = indexName;
        } else {
            index = DEFAULT_ES_INDEX_NAME;
        }
        return index;
    }

    @Override
    public String getIndexType(Event event) {
        String indexType;
        if (this.indexType != null) {
            indexType = this.indexType;
        } else {
            indexType = DEFAULT_ES_INDEX_TYPE;
        }
        return indexType;
    }

    @Override
    public String getIndexId(Event event) {
        String indexId = null;
        if (this.indexId != null) {
            indexId = this.indexId;
        }
        return indexId;
    }

    @Override
    public void configure(Context context) {
        indexName = Util.getContextValue(context, ES_INDEX_NAME);
        indexType = Util.getContextValue(context, ES_INDEX_TYPE);
        indexId = Util.getContextValue(context, ES_INDEX_ID);
        logger.info("Simple Index builder, indexName [{}] indextype [{}] indexID [{}]",
                new Object[]{indexName, indexType, indexId});

    }
}
