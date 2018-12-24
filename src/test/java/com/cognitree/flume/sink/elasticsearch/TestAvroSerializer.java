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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static com.cognitree.flume.sink.elasticsearch.Constants.ES_AVRO_SCHEMA_FILE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

/**
 * Created by prashant
 */
@RunWith(Parameterized.class)
public class TestAvroSerializer {

    private AvroSerializer avroSerializer;

    private static final String ID = "id";

    private static final String NAME = "name";

    private static final String GENDER = "gender";

    private Integer id;
    private String name;
    private String gender;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { 1,"user1", "Male" },
                { 2, "user2", "Female" },
                { 3, "user3", "Male" }
        });
    }
    public TestAvroSerializer(Integer id, String name, String gender) {
        this.id = id;
        this.name = name;
        this.gender = gender;
    }

    @Before
    public void init() throws Exception {
        avroSerializer = new AvroSerializer();
    }

    /**
     * tests Avro Serializer
     */
    @Test
    public void testSerializer() throws Exception {
        Context context = new Context();
        String schemaFile = getClass().getResource("/schema.avsc").getFile();
        context.put(ES_AVRO_SCHEMA_FILE, schemaFile);
        avroSerializer.configure(context);
        Schema schema = new Schema.Parser().parse(new File(schemaFile));
        GenericRecord user = generateGenericRecord(schema);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = new EncoderFactory().binaryEncoder(outputStream, null);
        datumWriter.write(user, encoder);
        encoder.flush();
        Event event = EventBuilder.withBody(outputStream.toByteArray());
        XContentBuilder expected = generateContentBuilder();
        XContentBuilder actual = avroSerializer.serialize(event);
        JsonParser parser = new JsonParser();
        assertEquals(parser.parse(expected.string()), parser.parse(actual.string()));
    }

    private GenericRecord generateGenericRecord(Schema schema) {
        GenericRecord user = new GenericData.Record(schema);
        user.put(ID, id);
        user.put(NAME, name);
        user.put(GENDER, gender);
        return user;
    }

    private XContentBuilder generateContentBuilder() throws IOException {
        XContentBuilder expected = jsonBuilder().startObject();
        expected.field(ID, id);
        expected.field(NAME, name);
        expected.field(GENDER, gender);
        expected.endObject();
        return expected;
    }
}
