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

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.formatter.output.BucketPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

public class TimestampBasedIndexBuilder implements IndexBuilder {

    private static final Logger logger = LoggerFactory.getLogger(TimestampBasedIndexBuilder.class);

    private String index;
    private String type;
    private String dateFormat;
    private String dateTimeZone;

    private FastDateFormat fastDateFormat = null;

    @Override
    public String getIndex(Event event) {
        TimestampedEvent timestampedEvent = new TimestampedEvent(event);
        long timestamp = timestampedEvent.getTimestamp();

        String indexName = index;
        if (indexName == null) {
            indexName = DEFAULT_ES_INDEX;
        }
        indexName = BucketPath.escapeString(indexName, event.getHeaders());

        String timestampSuffix = "";
        if (fastDateFormat != null) {
            timestampSuffix = new StringBuilder(fastDateFormat.format(timestamp)).insert(0, '-').toString();
        }

        return new StringBuilder(indexName).append(timestampSuffix).toString();
    }

    @Override
    public String getType(Event event) {
        String type;
        if (this.type != null) {
            type = this.type;
        } else {
            type = DEFAULT_ES_TYPE;
        }
        return type;
    }

    @Override
    public String getId(Event event) {
        return null;
    }

    @Override
    public void configure(Context context) {
        this.index = Util.getContextValue(context, ES_INDEX);
        if (this.index == null) {
            this.index = DEFAULT_ES_INDEX;
        }
        this.type = Util.getContextValue(context, ES_TYPE);
        this.dateFormat = Util.getContextValue(context, ES_INDEX_BUILDER_DATE_FORMAT);
        this.dateTimeZone = Util.getContextValue(context, ES_INDEX_BUILDER_DATE_TIME_ZONE);

        if (this.dateFormat != null) {
            if (this.dateTimeZone == null) {
                this.dateTimeZone = DEFAULT_ES_INDEX_BUILDER_DATE_TIME_ZONE;
            }

            fastDateFormat = FastDateFormat.getInstance(dateFormat, TimeZone.getTimeZone(dateTimeZone));
        }
        logger.info("Simple Index builder, name [{}] type [{}] date format [{}] date time zone [{}] ",
                new Object[]{this.index, this.type, this.dateFormat, this.dateTimeZone});
    }
}
