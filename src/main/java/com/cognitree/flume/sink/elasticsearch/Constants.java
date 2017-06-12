package com.cognitree.flume.sink.elasticsearch;

/**
 * Created by prashant
 */
public class Constants {

    public static final String PREFIX = "es.";

    public static final String INDEX = "index";
    public static final String TYPE = "type";
    public static final String ID = "id";

    public static final String ES_BULK_ACTIONS = "es.bulkActions";
    public static final Integer DEFAULT_ES_BULK_ACTIONS = 1000;

    public static final String ES_BULK_SIZE = "es.bulkSize";
    public static final String ES_BULK_SIZE_UNIT = "es.bulkSize.unit";
    public static final Integer DEFAULT_ES_BULK_SIZE = 5;

    public static final String ES_BULK_PROCESSOR_NAME = "es.bulkProcessor.name";

    public static final String ES_CONCURRENT_REQUEST = "es.concurrent.request";
    public static final Integer DEFAULT_ES_CONCURRENT_REQUEST = 1;

    public static final String ES_FLUSH_INTERVAL_TIME = "es.flush.interval.time";
    public static final String DEFAULT_ES_TIME = "5s";

    public static final String ES_BACKOFF_POLICY_TIME_INTERVAL = "es.backoff.policy.time.interval";
    public static final String DEFAULT_ES_BACKOFF_POLICY_START_DELAY = "50M";

    public static final String ES_BACKOFF_POLICY_RETRIES = "es.backoff.policy.retries";
    public static final Integer DEFAULT_ES_BACKOFF_POLICY_RETRIES = 8;

    public static final String ES_INDEX_NAME = "es.index.name";
    public static final String DEFAULT_ES_INDEX_NAME = "defaultindex";

    public static final String ES_INDEX_TYPE = "es.index.type";
    public static final String DEFAULT_ES_INDEX_TYPE = "defaulttype";

    public static final String ES_INDEX_ID = "es.index.id";

    public static final String ES_INDEX_BUILDER = "es.index.builder";

    public static final String ES_SERIALIZER_BUILDER = "es.serializer.builder";
    public static final String DEFAULT_ES_SERIALIZER_BUILDER = "com.cognitree.flume.sink.elasticsearch.SimpleSerializerBuilder";

    // Mandatory Properties
    public static final String ES_CLUSTER_NAME = "cluster.name";
    public static final String ES_HOST_NAME = "es.client.hostName";
    public static final String ES_PORT = "es.client.port";

    public static final String ES_TRANSPORT_SNIFF = "client.transport.sniff";
    public static final String ES_IGNORE_CLUSTER_NAME = "client.transport.ignore_cluster_name";
    public static final String ES_TRANSPORT_PING_TIMEOUT = "client.transport.ping_timeout";
    public static final String ES_TRANSPORT_NODE_SAMPLER_INTERVAL = "client.transport.nodes_sampler_interval";

    /**
     * This enum is used for the time unit
     *
     * Time unit can be in Second, Minute or Mili second
     */
    public enum UnitEnum {
        SECOND("s"),
        MINUTE("m"),
        MILI_SECOND("M"),
        UNKNOWN("unknown");

        private String unit;

        UnitEnum(String unit) {
            this.unit = unit;
        }

        @Override
        public String toString() {
            return unit;
        }

        public static UnitEnum fromString(String unit) {
            for (UnitEnum unitEnum : UnitEnum.values()) {
                if (unitEnum.unit.equals(unit)) {
                    return unitEnum;
                }
            }
            return UNKNOWN;
        }
    }

    /**
     *
     * This enum is used for unit of size of data
     *
     * Unit can be in Mega byte or kilo byte
     */
    public enum ByteSizeEnum {
        MB("mb"),
        KB("kb");

        private String byteSizeUnit;

        ByteSizeEnum(String byteSizeUnit) {
            this.byteSizeUnit = byteSizeUnit;
        }

        @Override
        public String toString() {
            return byteSizeUnit;
        }
    }
}
