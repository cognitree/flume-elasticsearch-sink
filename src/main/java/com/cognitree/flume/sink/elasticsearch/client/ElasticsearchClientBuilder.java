package com.cognitree.flume.sink.elasticsearch.client;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * Created by prashant
 */
public class ElasticsearchClientBuilder {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchClientBuilder.class);

    private String clusterName;
    private String hostName;
    private Integer port;

    private boolean transportSniff;
    private boolean ignoreClusterName;
    private TimeValue transportPingTimeout;
    private TimeValue nodeSamplerInterval;


    public ElasticsearchClientBuilder(String clusterName, String hostName, Integer port) {
        this.clusterName = clusterName;
        this.hostName = hostName;
        this.port = port;
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

    public TransportClient build(){
        TransportClient client = null;
        try {
            logger.trace("Cluster Name: [{}], Transport Sniff: [{}]" +
                            ", Ignore Cluster Name: [{}], Transport Ping TimeOut: [{}],  " +
                            "Node Sampler Interval: [{}], HostName: [{}], Port: [{}] ",
                    new Object[]{clusterName, transportSniff,
                            ignoreClusterName, transportPingTimeout,
                            nodeSamplerInterval, hostName, port});
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
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), port));
        } catch (UnknownHostException e) {
            logger.error("Error in creating the transport client for elastic search " + e.getMessage(), e);
        }
        return client;
    }
}
