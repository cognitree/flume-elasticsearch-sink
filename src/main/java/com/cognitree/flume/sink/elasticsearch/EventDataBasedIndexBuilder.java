/*
 * Copyright 2017 Cognitree Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.cognitree.flume.sink.elasticsearch;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * Creates Index, type and id based on the event data.
 */
public class EventDataBasedIndexBuilder implements IndexBuilder {

    private static final Logger logger = LoggerFactory.getLogger(EventDataBasedIndexBuilder.class);

    private static final ObjectMapper objectMapper= new ObjectMapper();

    private String indexField;
    private String typeField;
    private String idField;

    private AvroSerializer avroSerializer;

    /**
     * Get the field identified by indexField and returns its value as index name. If the field is absent
     * then returns default index value.
     */
    @Override
    public String getIndex(Event event) {
        try {
            Decoder decoder = new DecoderFactory().binaryDecoder(event.getBody(), null);
            DatumReader<GenericRecord> datumReader = avroSerializer.getDatumReader(event);
            GenericRecord data = datumReader.read(null, decoder);

            if(data.get(indexField) != null) {
                return data.get(indexField).toString();
            }
        } catch (IOException e) {
            logger.error("Error parsing logger body", e);
        }
        return DEFAULT_ES_INDEX;
    }

    /**
     * Get the field identified by typeField and returns its value as type name. If the field is absent
     * then returns default type value.
     */
    @Override
    public String getType(Event event) {
        try {
            Decoder decoder = new DecoderFactory().binaryDecoder(event.getBody(), null);
            DatumReader<GenericRecord> datumReader = avroSerializer.getDatumReader(event);
            GenericRecord data = datumReader.read(null, decoder);

            if(data.get(typeField) != null) {
                return data.get(typeField).toString();
            }
        } catch (IOException e) {
            logger.error("Error parsing logger body", e);
        }
        return DEFAULT_ES_TYPE;
    }

    /**
     * Get the field identified by idField and returns its value as type name. If the field is absent
     * then returns value obtained from header for ID.
     */
    @Override
    public String getId(Event event) {
        try {
            Decoder decoder = new DecoderFactory().binaryDecoder(event.getBody(), null);
            DatumReader<GenericRecord> datumReader = avroSerializer.getDatumReader(event);
            GenericRecord data = datumReader.read(null, decoder);

            if(data.get(idField) != null) {
                return data.get(idField).toString();
            }
        } catch (IOException e) {
            logger.error("Error parsing logger body", e);
        }
        return event.getHeaders().get(ID);
    }

    @Override
    public void configure(Context context) {
        this.indexField = Util.getContextValue(context, ES_INDEX);
        this.typeField = Util.getContextValue(context, ES_TYPE);
        this.idField = Util.getContextValue(context, ES_ID);
        logger.info("Simple Index builder, name [{}] typeIdentifier [{}] id [[]]",
                new Object[]{this.indexField, this.typeField, this.idField});

        avroSerializer = new AvroSerializer();
        avroSerializer.configure(context);
    }
}