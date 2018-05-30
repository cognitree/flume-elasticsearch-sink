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
package com.cognitree.flume.sink.elasticsearch.client;

import com.google.common.base.Throwables;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * Created by prashant
 */
public class ElasticsearchClientBuilder {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchClientBuilder.class);

    private String clusterName;

    private boolean transportSniff;
    private boolean ignoreClusterName;
    private TimeValue transportPingTimeout;
    private TimeValue nodeSamplerInterval;

    private List<TransportAddress> transportAddresses;

    public ElasticsearchClientBuilder(String clusterName, String[] hostnames) {
        this.clusterName = clusterName;
        setTransportAddresses(hostnames);
    }


    public ElasticsearchClientBuilder setTransportSniff(boolean transportSniff) {
        this.transportSniff = transportSniff;
        return this;
    }

    public ElasticsearchClientBuilder setIgnoreClusterName(boolean ignoreClusterName) {
        this.ignoreClusterName = ignoreClusterName;
        return this;
    }

    public ElasticsearchClientBuilder setTransportPingTimeout(TimeValue transportPingTimeout) {
        this.transportPingTimeout = transportPingTimeout;
        return this;
    }

    public ElasticsearchClientBuilder setNodeSamplerInterval(TimeValue nodeSamplerInterval) {
        this.nodeSamplerInterval = nodeSamplerInterval;
        return this;
    }

    public TransportClient build() {
        TransportClient client;
        logger.trace("Cluster Name: [{}], Transport Sniff: [{}]" +
                        ", Ignore Cluster Name: [{}], Transport Ping TimeOut: [{}],  " +
                        "Node Sampler Interval: [{}], HostName: [{}], Port: [{}] ",
                new Object[]{clusterName, transportSniff,
                        ignoreClusterName, transportPingTimeout,
                        nodeSamplerInterval, transportAddresses});
        Settings settings = Settings.builder()
                .put(ES_CLUSTER_NAME,
                        clusterName)
                .put(ES_TRANSPORT_SNIFF,
                        transportSniff)
                .put(ES_IGNORE_CLUSTER_NAME,
                        ignoreClusterName)
                .put(ES_TRANSPORT_PING_TIMEOUT,
                        transportPingTimeout)
                .put(ES_TRANSPORT_NODE_SAMPLER_INTERVAL,
                        nodeSamplerInterval)
                .build();
        client = new PreBuiltTransportClient(settings);
        for (TransportAddress inetSocketTransportAddress : transportAddresses) {
            client.addTransportAddress(inetSocketTransportAddress);
        }
        return client;
    }

    private void setTransportAddresses(String[] transportAddresses) {
        try {
            this.transportAddresses = new ArrayList<TransportAddress>(transportAddresses.length);
            for (String transportAddress : transportAddresses) {
                String hostName = transportAddress.split(":")[0];
                Integer port = transportAddress.split(":").length > 1 ?
                        Integer.parseInt(transportAddress.split(":")[1]) : DEFAULT_ES_PORT;
                this.transportAddresses.add(new TransportAddress(InetAddress.getByName(hostName), port));
            }
        } catch (Exception e) {
            logger.error("Error in creating the TransportAddress for elasticsearch " + e.getMessage(), e);
            Throwables.propagate(e);
        }
    }
}
