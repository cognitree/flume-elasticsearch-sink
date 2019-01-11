package com.cognitree.flume.sink.elasticsearch;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class HeaderBasedSerializer implements Serializer {
    private static final Logger logger = LoggerFactory.getLogger(HeaderBasedSerializer.class);
    public static final String BODY_FIELD_NAME = "es.serializer.json.bodyFieldName";
    public static final String BODY_FIELD_NAME_VALUE = "message";

    private String bodyFieldName;

    public XContentBuilder serialize(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody(), Charsets.UTF_8);

        XContentBuilder xContentBuilder = null;

        try {
            xContentBuilder = jsonBuilder().startObject();

            for (Map.Entry<String, String> entry: headers.entrySet()) {
                xContentBuilder.field(entry.getKey(), entry.getValue());
            }

            xContentBuilder.field(bodyFieldName, body);

            xContentBuilder.endObject();
        } catch (IOException e) {
            logger.error("Error in converting the event to the json format " + e.getMessage(), e);
        }

        return xContentBuilder;
    }

    public void configure(Context context) {
        bodyFieldName = context.getString(BODY_FIELD_NAME, BODY_FIELD_NAME_VALUE);
    }

}
