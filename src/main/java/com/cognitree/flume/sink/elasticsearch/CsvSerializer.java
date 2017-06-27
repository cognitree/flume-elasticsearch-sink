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

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by prashant
 *
 * This Serializer assumes the event body to be in CSV format
 * with custom delimiter specified.
 */
public class CsvSerializer implements Serializer {

    private static final Logger logger = LoggerFactory.getLogger(CsvSerializer.class);

    private final List<String> names = new ArrayList<String>();

    private final List<String> types = new ArrayList<String>();

    private String delimiter;

    /**
     *
     * Converts the csv data to the json format
     */
    @Override
    public XContentBuilder serialize(Event event) {
        XContentBuilder xContentBuilder = null;
        String body = new String(event.getBody(), Charsets.UTF_8);
        try {
            if (!names.isEmpty() && !types.isEmpty()) {
                xContentBuilder = jsonBuilder().startObject();
                List<String> values = Arrays.asList(body.split(delimiter));
                for (int i = 0; i < names.size(); i++) {
                    Util.addField(xContentBuilder, names.get(i), values.get(i), types.get(i));
                }
                xContentBuilder.endObject();
            } else {
                logger.error("Fields for csv files are not configured, " +
                        "please configured the property " + ES_CSV_FIELDS);
            }
        } catch (Exception e) {
            logger.error("Error in converting the body to the json format " + e.getMessage(), e);
        }
        return xContentBuilder;
    }

    /**
     *
     * Returns name and value based on the index
     *
     */
    private String getValue(String fieldType, Integer index) {
        String value = "";
        if (fieldType.length() > index) {
            value = fieldType.split(COLONS)[index];
        }
        return value;
    }

    /**
     *
     * Configure the field and its type with the custom delimiter
     */
    @Override
    public void configure(Context context) {
        String fields = context.getString(ES_CSV_FIELDS);
        if(fields == null) {
            Throwables.propagate(new Exception("Fields for csv files are not configured," +
                    " please configured the property " + ES_CSV_FIELDS));
        }
        try {
            delimiter = context.getString(ES_CSV_DELIMITER, DEFAULT_ES_CSV_DELIMITER);
            String[] fieldTypes = fields.split(COMMA);
            for (String fieldType : fieldTypes) {
                names.add(getValue(fieldType, 0));
                types.add(getValue(fieldType, 1));
            }
        } catch(Exception e) {
            Throwables.propagate(e);
        }
    }
}
