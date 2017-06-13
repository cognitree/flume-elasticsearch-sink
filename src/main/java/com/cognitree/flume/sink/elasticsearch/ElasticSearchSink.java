package com.cognitree.flume.sink.elasticsearch;

import com.cognitree.flume.sink.elasticsearch.client.BulkProcessorBuilder;
import com.cognitree.flume.sink.elasticsearch.client.ElasticsearchClientBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * This sink will read the events from a channel and add them to the bulk processor.
 *
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

    private Serializer serializer;

    @Override
    public void configure(Context context) {
        try {
            TransportClient client = new ElasticsearchClientBuilder(context.getString(PREFIX + ES_CLUSTER_NAME),
                    context.getString(ES_HOST_NAME),
                    context.getInteger(ES_PORT))
                    .setTransportSniff(context.getBoolean(
                            PREFIX + ES_TRANSPORT_SNIFF, false))
                    .setIgnoreClusterName(context.getBoolean(
                            PREFIX + ES_IGNORE_CLUSTER_NAME, false))
                    .setTransportPingTimeout(Util.getTimeValue(context.getString(
                            PREFIX + ES_TRANSPORT_PING_TIMEOUT), DEFAULT_ES_TIME))
                    .setNodeSamplerInterval(Util.getTimeValue(context.getString(
                            PREFIX + ES_TRANSPORT_NODE_SAMPLER_INTERVAL), DEFAULT_ES_TIME))
                    .build();
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
            buildSerializer(context);
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
                                .source(serializer.serialize(event)));
                    } else {
                        bulkProcessor.add(new IndexRequest(indexName, indexType)
                                .source(serializer.serialize(event)));
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
     */
    private void buildIndexBuilder(Context context) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        String indexBuilderClass = DEFAULT_ES_INDEX_BUILDER;
        if (StringUtils.isNotBlank(context.getString(ES_INDEX_BUILDER))) {
            indexBuilderClass = context.getString(ES_INDEX_BUILDER);
        }
            @SuppressWarnings("unchecked")
            Class<? extends IndexBuilder> aClass
                    = (Class<? extends IndexBuilder>) Class
                    .forName(indexBuilderClass);
            indexBuilder = aClass.newInstance();
            indexBuilder.configure(context);
    }

    /**
     *
     * builds Serializer builder based on the value "es.serializer.builder"
     */
    private void buildSerializer(Context context) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        String serializerBuilderClass = DEFAULT_ES_SERIALIZER_BUILDER;
        if (StringUtils.isNotEmpty(context.getString(ES_SERIALIZER_BUILDER))) {
            serializerBuilderClass = context.getString(ES_SERIALIZER_BUILDER);
        }
        @SuppressWarnings("unchecked")
        Class<? extends Serializer> clazz
                = (Class<? extends Serializer>) Class
                .forName(serializerBuilderClass);
        serializer = clazz.newInstance();
    }

}
