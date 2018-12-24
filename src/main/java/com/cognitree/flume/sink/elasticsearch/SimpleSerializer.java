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
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.common.xcontent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by prashant
 * <p>
 * This Serializer assumes the event body to be in JSON format
 * Validate the json and copy the same structure in the parser
 * returns XContentBuilder
 */
public class SimpleSerializer implements Serializer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleSerializer.class);

    public XContentBuilder serialize(Event event) {
        XContentBuilder builder = null;
        try {
            XContentParser parser = XContentFactory
                    .xContent(XContentType.JSON)
                    .createParser(NamedXContentRegistry.EMPTY,
                            new String(event.getBody(), Charsets.UTF_8));
            builder = jsonBuilder().copyCurrentStructure(parser);
            parser.close();
        } catch (Exception e) {
            logger.error("Error in Converting the body to json field " + e.getMessage(), e);
        }
        return builder;
    }

    @Override
    public void configure(Context context) {
        // No parameters needed from the configurations
    }
}
