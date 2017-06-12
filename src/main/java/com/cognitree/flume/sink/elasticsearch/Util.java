package com.cognitree.flume.sink.elasticsearch;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * Created by prashant
 */
public class Util {

    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    /**
     * Returns TimeValue based on the given interval
     * Interval can be in minutes, hours, seconds, mili seconds or nano seconds
     */
    public static TimeValue getTimeValue(String interval, String defaultValue) {
        TimeValue timeValue = null;
        String timeInterval = interval != null ? interval : defaultValue;
        logger.trace("Time interval is [{}] ", timeInterval);
        if (timeInterval != null) {
            Integer time = Integer.valueOf(timeInterval.substring(0, timeInterval.length() - 1));
            String unit = timeInterval.substring(timeInterval.length() - 1);
            UnitEnum unitEnum = UnitEnum.fromString(unit);
            switch (unitEnum) {
                case MINUTE:
                    timeValue = TimeValue.timeValueMinutes(time);
                    break;
                case SECOND:
                    timeValue = TimeValue.timeValueSeconds(time);
                    break;
                case MILI_SECOND:
                    timeValue = TimeValue.timeValueMillis(time);
                    break;
                default:
                    logger.error("Unit is incorrect, please check the Time Value unit: " + unit);
            }
        }
        return timeValue;
    }

    /**
     * Returns ByteSizeValue of the given byteSize and unit
     * byteSizeUnit can be in Peta bytes, Giga bytes, Tera bytes, Mega bytes, Kilo Bytes
     */
    public static ByteSizeValue getByteSizeValue(Integer byteSize, String unit) {
        ByteSizeValue byteSizeValue = new ByteSizeValue(DEFAULT_ES_BULK_SIZE, ByteSizeUnit.MB);
        logger.trace("Byte size value is [{}] ", byteSizeValue);
        if (byteSize != null) {
            ByteSizeEnum byteSizeEnum = ByteSizeEnum.valueOf(unit.toUpperCase());
            switch (byteSizeEnum) {
                case MB:
                    byteSizeValue = new ByteSizeValue(byteSize, ByteSizeUnit.MB);
                    break;
                case KB:
                    byteSizeValue = new ByteSizeValue(byteSize, ByteSizeUnit.KB);
                    break;
                default:
                    logger.error("Unit is incorrect, please check the Byte Size unit: " + unit);
            }
        }
        return byteSizeValue;
    }

    /**
     * Returns the context value for the contextId
     */
    public static String getContextValue(Context context, String contextId) {
        String contextValue = null;
        if (StringUtils.isNotBlank(context.getString(contextId))) {
            contextValue = context.getString(contextId);
        }
        return contextValue;
    }
}
