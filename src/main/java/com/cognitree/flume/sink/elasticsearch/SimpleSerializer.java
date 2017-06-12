package com.cognitree.flume.sink.elasticsearch;

import com.google.common.base.Charsets;
import org.apache.flume.Event;
import org.elasticsearch.common.xcontent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by prashant
 *
 * This Serializer assumes the event body to be in JSON format
 * Validate the json and copy the same structure in the parser
 * return the XContentBuilder
 */
public class SimpleSerializer implements Serializer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleSerializer.class);

    public XContentBuilder serialize(Event event) {
        XContentBuilder builder = null;
        try {
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY, new String(event.getBody(), Charsets.UTF_8));
            builder = jsonBuilder().copyCurrentStructure(parser);
            parser.close();
        } catch (Exception e) {
            logger.error("Error in Converting the body to json field " + e.getMessage(), e);
        }
        return builder;
    }
}
