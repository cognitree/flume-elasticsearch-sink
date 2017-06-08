package com.cognitree.flume.sink.elasticsearch.client;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;
import com.cognitree.flume.sink.elasticsearch.Util;
import org.apache.flume.Context;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by prashant
 */
public class ElasticSearchClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);

    private String clusterName;

    private boolean transportSniff;

    private boolean ignoreClusterName;

    private TimeValue transportPingTimeout;

    private TimeValue nodeSamplerInterval;

    private String hostName;

    private Integer port;

    public TransportClient generateTransportClient(Context context) {
        clusterName = context.getString(PREFIX + ES_CLUSTER_NAME);
        transportSniff = context.getBoolean(
                PREFIX + ES_TRANSPORT_SNIFF, false);
        ignoreClusterName = context.getBoolean(
                PREFIX + ES_IGNORE_CLUSTER_NAME, false);
        transportPingTimeout = Util.getTimeValue(context.getString(
                PREFIX + ES_TRANSPORT_PING_TIMEOUT), DEFAULT_ES_TIME);
        nodeSamplerInterval = Util.getTimeValue(context.getString(
                PREFIX + ES_TRANSPORT_NODE_SAMPLER_INTERVAL), DEFAULT_ES_TIME);
        hostName = context.getString(ES_HOST_NAME);
        port = context.getInteger(ES_PORT);
        return generateTransportClient();
    }

    private TransportClient generateTransportClient() {
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
