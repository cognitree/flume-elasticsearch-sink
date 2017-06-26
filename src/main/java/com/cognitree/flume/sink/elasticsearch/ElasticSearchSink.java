/*
 * Copyright 2017 Cognitree Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.cognitree.flume.sink.elasticsearch;

import com.cognitree.flume.sink.elasticsearch.client.BulkProcessorBuilder;
import com.cognitree.flume.sink.elasticsearch.client.ElasticsearchClientBuilder;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.netty.util.internal.StringUtil;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * This sink will read the events from a channel and add them to the bulk processor.
 * <p>
 * This sink must be configured with with mandatory parameters detailed in
 * {@link Constants}
 */
public class ElasticSearchSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSink.class);

    private BulkProcessor bulkProcessor;

    private IndexBuilder indexBuilder;

    private Serializer serializer;

    @Override
    public void configure(Context context) {
        String[] hosts = getHosts(context);
        if(ArrayUtils.isNotEmpty(hosts)) {
            TransportClient client = new ElasticsearchClientBuilder(
                    context.getString(PREFIX + ES_CLUSTER_NAME, DEFAULT_ES_CLUSTER_NAME), hosts)
                    .setTransportSniff(context.getBoolean(
                            PREFIX + ES_TRANSPORT_SNIFF, false))
                    .setIgnoreClusterName(context.getBoolean(
                            PREFIX + ES_IGNORE_CLUSTER_NAME, false))
                    .setTransportPingTimeout(Util.getTimeValue(context.getString(
                            PREFIX + ES_TRANSPORT_PING_TIMEOUT), DEFAULT_ES_TIME))
                    .setNodeSamplerInterval(Util.getTimeValue(context.getString(
                            PREFIX + ES_TRANSPORT_NODE_SAMPLER_INTERVAL), DEFAULT_ES_TIME))
                    .build();
            buildIndexBuilder(context);
            buildSerializer(context);
            bulkProcessor = new BulkProcessorBuilder().buildBulkProcessor(context, client);
        } else {
            logger.error("Could not create transport client, No host exist");
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        txn.begin();
        try {
            Event event = channel.take();
            if (event != null) {
                String body = new String(event.getBody(), Charsets.UTF_8);
                if (!Strings.isNullOrEmpty(body)) {
                    logger.debug("start to sink event [{}].", body);
                    String index = indexBuilder.getIndex(event);
                    String type = indexBuilder.getType(event);
                    String id = indexBuilder.getId(event);
                    XContentBuilder xContentBuilder = serializer.serialize(event);
                    if(xContentBuilder != null) {
                        if (!StringUtil.isNullOrEmpty(id)) {
                            bulkProcessor.add(new IndexRequest(index, type, id)
                                    .source(xContentBuilder));
                        } else {
                            bulkProcessor.add(new IndexRequest(index, type)
                                    .source(xContentBuilder));
                        }
                    } else {
                        logger.error("Could not serialize the event body [{}] for index [{}], type[{}] and id [{}] ",
                                new Object[]{body, index, type, id});
                    }
                }
                logger.debug("sink event [{}] successfully.", body);
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
        if (bulkProcessor != null) {
            bulkProcessor.close();
        }
    }

    /**
     * builds Index builder
     */
    private void buildIndexBuilder(Context context) {
        String indexBuilderClass = DEFAULT_ES_INDEX_BUILDER;
        if (StringUtils.isNotBlank(context.getString(ES_INDEX_BUILDER))) {
            indexBuilderClass = context.getString(ES_INDEX_BUILDER);
        }
        this.indexBuilder = instantiateClass(indexBuilderClass);
        if (this.indexBuilder != null) {
            this.indexBuilder.configure(context);
        }
    }

    /**
     * builds Serializer
     */
    private void buildSerializer(Context context) {
        String serializerClass = DEFAULT_ES_SERIALIZER;
        if (StringUtils.isNotEmpty(context.getString(ES_SERIALIZER))) {
            serializerClass = context.getString(ES_SERIALIZER);
        }
        this.serializer = instantiateClass(serializerClass);
        if(this.serializer != null) {
            this.serializer.configure(context);
        }
    }

    private <T> T instantiateClass(String className) {
        try {
            @SuppressWarnings("unchecked")
            Class<T> aClass = (Class<T>) Class.forName(className);
            return aClass.newInstance();
        } catch (Exception e) {
            logger.error("Could not instantiate class " + className, e);
            Throwables.propagate(e);
            return null;
        }
    }

    /**
     * returns hosts
     */
    private String[] getHosts(Context context) {
        String[] hosts = null;
        if (StringUtils.isNotBlank(context.getString(ES_HOSTS))) {
            hosts = context.getString(ES_HOSTS).split(",");
        }
        return hosts;
    }
}
