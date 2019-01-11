package com.cognitree.flume.sink.elasticsearch;

import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.*;

public class TestHeaderBasedSerializer {
    private HeaderBasedSerializer headerBasedSerializer;

    private static final Charset charset = Charset.defaultCharset();

    private String message = "Lorem ipsum dolor sit amet";

    @Before
    public void init() throws Exception {
      headerBasedSerializer = new HeaderBasedSerializer();
    }

    @Test
    public void testSerializer() throws IOException {
        Context context = new Context();
        context.put("es.serializer.json.bodyFieldName", "message");

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("id", "1");
        headers.put("name", "test");

        Event event = EventBuilder.withBody(message, charset, headers);

        headerBasedSerializer.configure(context);

        XContentBuilder expected = generateContentBuilder();
        XContentBuilder actual = headerBasedSerializer.serialize(event);

        JsonParser parser = new JsonParser();
        assertEquals(parser.parse(expected.string()), parser.parse(actual.string()));
    }

    private XContentBuilder generateContentBuilder() throws IOException {
        XContentBuilder expected = jsonBuilder().startObject();
        expected.field("id", "1");
        expected.field("name", "test");
        expected.field("message", message);
        expected.endObject();
        return expected;
    }
}