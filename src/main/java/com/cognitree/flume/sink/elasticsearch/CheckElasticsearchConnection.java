package com.cognitree.flume.sink.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class CheckElasticsearchConnection extends TimerTask {

    private TransportClient client;
    AtomicBoolean backOffPolicy;

    @Override
    public void run() {
        checkConnection();
    }

    public CheckElasticsearchConnection(TransportClient client, AtomicBoolean backOffPolicy){
        this.client=client;
        this.backOffPolicy=backOffPolicy;
    }

    private void checkConnection(){
        if(!client.connectedNodes().isEmpty()){
            backOffPolicy.set(false);
        }
    }
}
