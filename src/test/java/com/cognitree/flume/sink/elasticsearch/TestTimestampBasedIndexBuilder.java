package com.cognitree.flume.sink.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;
import static org.junit.Assert.*;

public class TestTimestampBasedIndexBuilder {

    private TimestampBasedIndexBuilder timestampBasedIndexBuilder;
    private String index = "es-index";
    private String type = "es-type";
    private String dateFormat = "yyyy-MM-dd-HH";


    @Before
    public void init() throws Exception {
        timestampBasedIndexBuilder = new TimestampBasedIndexBuilder();
    }

    @Test
    public void testDefaultIndex() {
        Event event = new SimpleEvent();
        assertEquals(DEFAULT_ES_INDEX, timestampBasedIndexBuilder.getIndex(event));
        assertEquals(DEFAULT_ES_TYPE, timestampBasedIndexBuilder.getType(event));
    }

    @Test
    public void testTimestampedIndex() {
        Event event = new SimpleEvent();

        Context context = new Context();

        context.put(ES_INDEX, index);
        context.put(ES_TYPE, type);
        context.put(ES_INDEX_BUILDER_DATE_FORMAT, dateFormat);

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("timestamp", "1546350162000"); // 2019-01-01 13:42:42.000 UTC
        event.setHeaders(headers);

        timestampBasedIndexBuilder.configure(context);

        String expectedIndexName = new StringBuilder(index).append('-').append("2019-01-01-13").toString();

        assertEquals(expectedIndexName, timestampBasedIndexBuilder.getIndex(event));
    }

    @Test
    public void testTimestampedWithTZIndex() {
        Event event = new SimpleEvent();

        Context context = new Context();

        context.put(ES_INDEX, index);
        context.put(ES_TYPE, type);
        context.put(ES_INDEX_BUILDER_DATE_FORMAT, dateFormat);
        context.put(ES_INDEX_BUILDER_DATE_TIME_ZONE, "Europe/Moscow"); // UTC+3

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("@timestamp", "1546350162000"); // 2019-01-01 13:42:42.000 UTC
        event.setHeaders(headers);

        timestampBasedIndexBuilder.configure(context);

        String expectedIndexName = new StringBuilder(index).append('-').append("2019-01-01-16").toString();

        assertEquals(expectedIndexName, timestampBasedIndexBuilder.getIndex(event));
    }

    @Test
    public void testConfigurationIndex() {
        Event event = new SimpleEvent();
        Context context = new Context();

        context.put(ES_INDEX, index);
        context.put(ES_TYPE, type);

        timestampBasedIndexBuilder.configure(context);

        assertEquals(index, timestampBasedIndexBuilder.getIndex(event));
        assertEquals(type, timestampBasedIndexBuilder.getType(event));
    }
}