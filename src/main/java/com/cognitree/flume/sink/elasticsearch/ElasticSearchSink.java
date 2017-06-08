package com.cognitree.flume.sink.elasticsearch;

import com.cognitree.flume.sink.elasticsearch.client.BulkProcessorBuilder;
import com.cognitree.flume.sink.elasticsearch.client.ElasticSearchClient;
import static com.cognitree.flume.sink.elasticsearch.Constants.*;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.netty.util.internal.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

/**
 * This sink will read the events from a channel and add them to the bulk processor.
 * Depends upon the configuration it will write bunch of documents in elastic search.
 * <p>
 * This sink must be configured with with mandatory parameters detailed in
 * {@link Constants}
 */
public class ElasticSearchSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSink.class);

    private TransportClient client;

    private BulkProcessor bulkProcessor;

    private String indexName;

    private String indexType;

    private String indexId;

    @Override
    public void configure(Context context) {
        try {
            client = new ElasticSearchClient().generateTransportClient(context);
            indexName = context.getString(ES_INDEX_NAME,
                    DEFAULT_ES_INDEX_NAME);
            indexType = context.getString(ES_INDEX_TYPE,
                    DEFAULT_ES_INDEX_TYPE);
            if (StringUtils.isNotBlank(context.getString(ES_INDEX_ID))) {
                indexId = context.getString(ES_INDEX_ID);
            }
            bulkProcessor = new BulkProcessorBuilder().buildBulkProcessor(context, client);
        } catch (Exception e) {
            logger.error("Could not instantiate event serializer.", e);
            Throwables.propagate(e);
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        txn.begin();
        try {
            Event event = channel.take();
            if (null != event) {
                String body = new String(event.getBody(), Charsets.UTF_8);
                if (!Strings.isNullOrEmpty(body)) {
                    logger.info("start to sink event [{}].", body);
                    if (!StringUtil.isNullOrEmpty(indexId)) {
                        bulkProcessor.add(new IndexRequest(indexName, indexType, indexId)
                                .source(body, XContentType.JSON));
                    } else {
                        bulkProcessor.add(new IndexRequest(indexName, indexType)
                                .source(body, XContentType.JSON));
                    }
                }
                logger.info("sink event [{}] successfully.", body);
            }
            txn.commit();
            return Status.READY;
        } catch (Throwable tx) {
            try {
                txn.rollback();
            } catch (Exception ex) {
                logger.error("exception in rollback.", ex);
            }
            logger.error("transaction rolled back.", tx);
            return Status.BACKOFF;
        } finally {
            txn.close();
        }
    }

    @Override
    public void stop() {
        bulkProcessor.close();
    }
}
