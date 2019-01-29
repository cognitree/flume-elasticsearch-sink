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

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.joda.time.DateTimeUtils;

import java.util.Map;

final public class TimestampedEvent extends SimpleEvent {
    private final long timestamp;

    TimestampedEvent(Event base) {
        setBody(base.getBody());
        Map<String, String> headers = Maps.newHashMap(base.getHeaders());
        String timestampString = headers.get("timestamp");
        if (StringUtils.isBlank(timestampString)) {
            timestampString = headers.get("@timestamp");
        }
        if (StringUtils.isBlank(timestampString)) {
            this.timestamp = DateTimeUtils.currentTimeMillis();
            headers.put("timestamp", String.valueOf(timestamp ));
        } else {
            this.timestamp = Long.valueOf(timestampString);
        }
        setHeaders(headers);
    }

    long getTimestamp() {
        return timestamp;
    }
}
