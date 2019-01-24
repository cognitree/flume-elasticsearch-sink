package com.cognitree.flume.sink.elasticsearch;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static com.cognitree.flume.sink.elasticsearch.Constants.*;

public class HeaderBasedSerializer implements Serializer {
    private static final Logger logger = LoggerFactory.getLogger(HeaderBasedSerializer.class);

    private final List<String> names = new ArrayList<String>();

    private final List<String> types = new ArrayList<String>();

    private String bodyFieldName;

    public XContentBuilder serialize(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody(), Charsets.UTF_8);

        XContentBuilder xContentBuilder = null;

        try {
            if (!names.isEmpty() && !types.isEmpty()) {
                xContentBuilder = jsonBuilder().startObject();

                for (int i = 0; i < names.size(); i++) {
                    String name = names.get(i);
                    Util.addField(xContentBuilder, name, headers.get(name), types.get(i));
                }

                Util.addField(xContentBuilder, bodyFieldName, body, DEFAULT_ES_HEADERBASED_BODY_FIELD_TYPE);

                xContentBuilder.endObject();
            } else {
                logger.error("Fields for headers based serializer are not configured, " +
                        "please configured the property " + ES_HEADERBASED_FIELDS);
            }
        } catch (IOException e) {
            logger.error("Error in converting the event to the json format " + e.getMessage(), e);
        }

        return xContentBuilder;
    }

    public void configure(Context context) {
        bodyFieldName = context.getString(ES_HEADERBASED_BODY_FIELD_NAME, DEFAULT_ES_HEADERBASED_BODY_FIELD_NAME);

        String fields = context.getString(ES_HEADERBASED_FIELDS);
        if(fields == null) {
            Throwables.propagate(new Exception("Fields for headers based serializer are not configured," +
                    " please configured the property " + ES_HEADERBASED_FIELDS));
        }
        try {
            String[] fieldTypes = fields.split(COMMA);
            for (String fieldType : fieldTypes) {
                names.add(getValue(fieldType, 0));
                types.add(getValue(fieldType, 1));
            }
        } catch(Exception e) {
            Throwables.propagate(e);
        }
    }

    private String getValue(String fieldType, Integer index) {
        String value = "";
        if (fieldType.length() > index) {
            value = fieldType.split(COLONS)[index];
        }
        return value;
    }

}
