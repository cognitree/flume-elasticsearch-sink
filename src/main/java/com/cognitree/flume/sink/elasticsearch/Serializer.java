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

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * A serializer to convert the given Flume Event into a json document that will be indexed into Elasticsearch.
 * A single instance of the class is created when the Sink initializes and is destroyed when the Sink is stopped.
 *
 */
public interface Serializer extends Configurable {

    /**
     * Serialize the body of the event to
     * XContentBuilder format
     */
    XContentBuilder serialize(Event event);
}
