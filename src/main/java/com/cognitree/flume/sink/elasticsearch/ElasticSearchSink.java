package com.cognitree.flume.sink.elasticsearch;

import com.cognitree.flume.sink.elasticsearch.client.BulkProcessorBuilder;
import com.cognitree.flume.sink.elasticsearch.client.ElasticSearchClient;
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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * This sink will read the events from a channel and add them to the bulk processor.
 * Depends upon the configuration it will write bunch of documents in elastic search.
 * <p>
 * This sink must be configured with with mandatory parameters detailed in
 * {@link Constants}
 */
public class ElasticSearchSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSink.class);

    private BulkProcessor bulkProcessor;

    private String indexName;

    private String indexType;

    private String indexId;

    private IndexBuilder indexBuilder;

    private SerializerBuilder serializerBuilder;

    @Override
    public void configure(Context context) {
        try {
            TransportClient client = new ElasticSearchClient().generateTransportClient(context);
            if (StringUtils.isNotBlank(context.getString(ES_INDEX_NAME))) {
                indexName = context.getString(ES_INDEX_NAME,
                        DEFAULT_ES_INDEX_NAME);
            }
            if (StringUtils.isNotBlank(context.getString(ES_INDEX_TYPE))) {
                indexType = context.getString(ES_INDEX_TYPE,
                        DEFAULT_ES_INDEX_TYPE);
            }
            if (StringUtils.isNotBlank(context.getString(ES_INDEX_ID))) {
                indexId = context.getString(ES_INDEX_ID);
            }
            buildIndexBuilder(context);
            buildSerializerBuilder(context);
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
                    if (indexBuilder != null) {
                        indexName = indexBuilder.getIndexName(event);
                        indexType = indexBuilder.getIndexType(event);
                        indexId = indexBuilder.getIndexId(event);
                    }
                    if (!StringUtil.isNullOrEmpty(indexId)) {
                        bulkProcessor.add(new IndexRequest(indexName, indexType, indexId)
                                .source(serializerBuilder.serialize(event)));
                    } else {
                        bulkProcessor.add(new IndexRequest(indexName, indexType)
                                .source(serializerBuilder.serialize(event)));
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

    /**
     *
     * builds the Index builder
     *
     * @param context
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private void buildIndexBuilder(Context context) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        if (StringUtils.isNotBlank(context.getString(ES_INDEX_BUILDER))) {
            String indexBuilderClass = context.getString(ES_INDEX_BUILDER);
            @SuppressWarnings("unchecked")
            Class<? extends IndexBuilder> aClass
                    = (Class<? extends IndexBuilder>) Class
                    .forName(indexBuilderClass);
            indexBuilder = aClass.newInstance();
        }
    }

    /**
     *
     * builds Serializer builder based on the value "es.serializer.builder"
     *
     * @param context
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private void buildSerializerBuilder(Context context) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        String serializerBuilderClass = DEFAULT_ES_SERIALIZER_BUILDER;
        if (StringUtils.isNotEmpty(context.getString(ES_SERIALIZER_BUILDER))) {
            serializerBuilderClass = context.getString(ES_SERIALIZER_BUILDER);
        }
        @SuppressWarnings("unchecked")
        Class<? extends SerializerBuilder> clazz
                = (Class<? extends SerializerBuilder>) Class
                .forName(serializerBuilderClass);
        serializerBuilder = clazz.newInstance();
    }

}
