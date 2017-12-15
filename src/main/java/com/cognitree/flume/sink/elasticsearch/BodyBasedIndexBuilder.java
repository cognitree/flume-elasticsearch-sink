package com.cognitree.flume.sink.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

public class BodyBasedIndexBuilder implements IndexBuilder {

    private static final Logger logger = LoggerFactory.getLogger(BodyBasedIndexBuilder.class);

    private static final ObjectMapper objectMapper= new ObjectMapper();


    private String eventIndexIdentifier;
    private String eventTypeIdentifier;
    private String eventIdIdentifier;

    @Override
    public String getIndex(Event event) {
        try {
            JsonNode dataNode = objectMapper.readTree(new String(event.getBody()));
            JsonNode eventIndexNode = dataNode.get(eventIndexIdentifier);
            if(eventIndexNode != null) {
                return eventIndexNode.asText();
            }
        } catch (IOException e) {
            logger.error("Error parsing logger body", e);
        }
        return DEFAULT_ES_INDEX;
    }

    @Override
    public String getType(Event event) {
        try {
            JsonNode dataNode = objectMapper.readTree(new String(event.getBody()));
            JsonNode eventTypeNode = dataNode.get(eventTypeIdentifier);
            if(eventTypeNode != null) {
                return eventTypeNode.asText();
            }
        } catch (IOException e) {
            logger.error("Error parsing logger body", e);
        }
        return DEFAULT_ES_TYPE;
    }

    @Override
    public String getId(Event event) {
        try {
            JsonNode dataNode = objectMapper.readTree(new String(event.getBody()));
            JsonNode eventIdNode = dataNode.get(eventIdIdentifier);
            if(eventIdNode != null) {
                return eventIdNode.asText();
            }
        } catch (IOException e) {
            logger.error("Error parsing logger body", e);
        }
        return event.getHeaders().get(ID);
    }

    @Override
    public void configure(Context context) {
        this.eventIndexIdentifier = Util.getContextValue(context, ES_INDEX);
        this.eventTypeIdentifier = Util.getContextValue(context, ES_TYPE);
        this.eventIdIdentifier = Util.getContextValue(context, ES_ID);
        logger.info("Simple Index builder, name [{}] typeIdentifier [{}] id [[]]",
                new Object[]{this.eventIndexIdentifier, this.eventTypeIdentifier, this.eventIdIdentifier});

    }
}