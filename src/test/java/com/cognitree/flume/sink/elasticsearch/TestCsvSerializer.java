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

import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;

import static com.cognitree.flume.sink.elasticsearch.Constants.ES_CSV_FIELDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

/**
 * Created by prashant
 */
public class TestCsvSerializer {

    private CsvSerializer csvSerializer;

    private static final Charset charset = Charset.defaultCharset();

    @Before
    public void init() throws Exception {
        csvSerializer = new CsvSerializer();
    }

    /**
     * tests csv serializer
     */
    @Test
    public void testSerializer() throws Exception {
        Context context = new Context();
        String type = "id:int,name:string,currentEmployee:boolean,leaves:float";
        context.put(ES_CSV_FIELDS, type);
        csvSerializer.configure(context);
        String message = "1,test,true,11.5";
        Event event = EventBuilder.withBody(message.getBytes(charset));
        XContentBuilder expected = generateContentBuilder();
        XContentBuilder actual = csvSerializer.serialize(event);
        JsonParser parser = new JsonParser();
        assertEquals(parser.parse(expected.string()), parser.parse(actual.string()));
    }

    private XContentBuilder generateContentBuilder() throws IOException {
        XContentBuilder expected = jsonBuilder().startObject();
        expected.field("id", 1);
        expected.field("name", "test");
        expected.field("currentEmployee", true);
        expected.field("leaves", 11.5);
        expected.endObject();
        return expected;
    }
}
