package com.cognitree.flume.sink.elasticsearch.client;

import com.cognitree.flume.sink.elasticsearch.Util;
import org.apache.flume.Context;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * This class creates  an instance of the {@link BulkProcessor}
 * Sets the configuration for teh BulkProcessor through {@link Context} object
 */
public class BulkProcessorBulider {

    private static final Logger logger = LoggerFactory.getLogger(BulkProcessorBulider.class);

    private String bulkProcessorName;

    private ByteSizeValue bulkSize;

    private Integer bulkActions;

    private Integer concurrentRequest;

    private TimeValue flushIntervalTime;

    private String backoffPolicyTimeInterval;

    private Integer backoffPolicyRetries;

    public BulkProcessor buildBulkProcessor(Context context, RestHighLevelClient client) {
        bulkActions = context.getInteger(ES_BULK_ACTIONS,
                DEFAULT_ES_BULK_ACTIONS);
        bulkProcessorName = context.getString(ES_BULK_PROCESSOR_NAME,
                DEFAULT_ES_BULK_PROCESSOR_NAME);
        bulkSize = Util.getByteSizeValue(context.getInteger(ES_BULK_SIZE),
                context.getString(ES_BULK_SIZE_UNIT));
        concurrentRequest = context.getInteger(ES_CONCURRENT_REQUEST,
                DEFAULT_ES_CONCURRENT_REQUEST);
        flushIntervalTime = Util.getTimeValue(context.getString(ES_FLUSH_INTERVAL_TIME),
                DEFAULT_ES_FLUSH_INTERVAL_TIME);
        backoffPolicyTimeInterval = context.getString(ES_BACKOFF_POLICY_TIME_INTERVAL,
                DEFAULT_ES_BACKOFF_POLICY_START_DELAY);
        backoffPolicyRetries = context.getInteger(ES_BACKOFF_POLICY_RETRIES,
                DEFAULT_ES_BACKOFF_POLICY_RETRIES);
        return build(client);
    }

    private BulkProcessor build(final RestHighLevelClient client) {
        logger.trace("Bulk processor name: [{}]  bulkActions: [{}], bulkSize: [{}], flush interval time: [{}]," +
                        " concurrent Request: [{}], backoffPolicyTimeInterval: [{}], backoffPolicyRetries: [{}] ",
                new Object[]{bulkProcessorName, bulkActions, bulkSize, flushIntervalTime,
                        concurrentRequest, backoffPolicyTimeInterval, backoffPolicyRetries});
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) -> client
                        .bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        return BulkProcessor.builder(bulkConsumer, getListener())
                .setBulkActions(bulkActions)
                .setBulkSize(bulkSize)
                .setFlushInterval(flushIntervalTime)
                .setConcurrentRequests(concurrentRequest)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(
                        Util.getTimeValue(backoffPolicyTimeInterval,
                                DEFAULT_ES_BACKOFF_POLICY_START_DELAY),
                        backoffPolicyRetries))
                .build();
    }

    private BulkProcessor.Listener getListener() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId,
                                   BulkRequest request) {
                logger.trace("Bulk Execution [" + executionId + "]\n" +
                        "No of actions " + request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId,
                                  BulkRequest request,
                                  BulkResponse response) {
                logger.trace("Bulk execution completed [" + executionId + "]\n" +
                        "Took (ms): " + response.getTook().getMillis() + "\n" +
                        "Failures: " + response.hasFailures() + "\n" +
                        "Failures Message: " + response.buildFailureMessage() + "\n" +
                        "Count: " + response.getItems().length);
            }

            @Override
            public void afterBulk(long executionId,
                                  BulkRequest request,
                                  Throwable failure) {
                logger.error("Bulk execution failed [" + executionId + "]" +
                        failure.toString());
            }
        };
    }
}