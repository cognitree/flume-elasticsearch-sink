package com.cognitree.flume.sink.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by prashant
 */
public class TestHeaderIndexBuilder {

    private HeaderBasedIndexBuilder headerBasedIndexBuilder;

    private String index = "es-index";

    private String type = "es-type";

    private String id = "es-id";

    @Before
    public void init() throws Exception {
        headerBasedIndexBuilder = new HeaderBasedIndexBuilder();
    }

    /**
     * tests header based index, type and id
     */
    @Test
    public void testHeaderIndex() {
        Event event = new SimpleEvent();
        Map<String, String> headers = new HashMap<String, String>();
        headers.put(INDEX, index);
        headers.put(TYPE, type);
        headers.put(ID, id);
        event.setHeaders(headers);
        assertEquals(index, headerBasedIndexBuilder.getIndex(event));
        assertEquals(type, headerBasedIndexBuilder.getType(event));
        assertEquals(id, headerBasedIndexBuilder.getId(event));
    }

    /**
     * tests configuration based index and type
     */
    @Test
    public void testConfigurationIndex() {
        Event event = new SimpleEvent();
        Context context = new Context();
        context.put(ES_INDEX, index);
        context.put(ES_TYPE, type);
        headerBasedIndexBuilder.configure(context);
        assertEquals(index, headerBasedIndexBuilder.getIndex(event));
        assertEquals(type, headerBasedIndexBuilder.getType(event));
    }

    /**
     * tests Default index and type
     */
    @Test
    public void testDefaultIndex() {
        Event event = new SimpleEvent();
        assertEquals(DEFAULT_ES_INDEX, headerBasedIndexBuilder.getIndex(event));
        assertEquals(DEFAULT_ES_TYPE, headerBasedIndexBuilder.getType(event));
    }
}
