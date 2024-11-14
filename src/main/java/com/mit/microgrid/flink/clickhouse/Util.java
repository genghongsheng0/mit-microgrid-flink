package com.mit.microgrid.flink.clickhouse;

import java.util.concurrent.TimeUnit;

public class Util {

    // 计算耗时并格式化为时分秒
    public static String formatDuration(long startTimeMillis, long endTimeMillis) {
        long durationMillis = endTimeMillis - startTimeMillis;

        // 计算时、分、秒
        long hours = TimeUnit.MILLISECONDS.toHours(durationMillis);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(durationMillis) - TimeUnit.HOURS.toMinutes(hours);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(durationMillis) - TimeUnit.MINUTES.toSeconds(minutes) - TimeUnit.HOURS.toSeconds(hours);
        long milliseconds = durationMillis % 1000;

        StringBuilder formattedDuration = new StringBuilder();

        // 根据条件添加时、分、秒
        if (hours > 0) {
            formattedDuration.append(hours).append("小时");
        }
        if (minutes > 0) {
            formattedDuration.append(minutes).append("分钟");
        }
        if (seconds > 0 || formattedDuration.length() == 0) { // 如果没有小时和分钟，确保至少显示秒
            formattedDuration.append(seconds).append("秒");
        }
        formattedDuration.append(milliseconds).append("ms");

        return formattedDuration.toString();
    }
}
