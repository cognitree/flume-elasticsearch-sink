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
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.transport.TransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import static com.cognitree.flume.sink.elasticsearch.Constants.DEFAULT_ES_PORT;

/**
 * This class creates  an instance of the {@link RestHighLevelClient}
 * Sets the number of hosts for the client
 */
public class ElasticSearchClientBuilder {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClientBuilder.class);

    private String clusterName;

    private List<TransportAddress> transportAddresses;

    public ElasticSearchClientBuilder(String clusterName, String[] hostnames) {
        this.clusterName = clusterName;
        setTransportAddresses(hostnames);
    }

    public RestHighLevelClient build() {
        RestHighLevelClient client;
        HttpHost[] hosts = new HttpHost[transportAddresses.size()];
        int i = 0;
        logger.trace("Cluster Name: [{}], HostName: [{}], Port: [{}] ",
                new Object[]{clusterName, transportAddresses});
        for (TransportAddress transportAddress : transportAddresses) {
            hosts[i++] = new HttpHost(transportAddress.address().getAddress(),
                    transportAddress.address().getPort(), "http");
        }
        client = new RestHighLevelClient(RestClient.builder(hosts));
        return client;
    }

    private void setTransportAddresses(String[] transportAddresses) {
        try {
            this.transportAddresses = new ArrayList<>(transportAddresses.length);
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