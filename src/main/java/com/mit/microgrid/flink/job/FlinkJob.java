package com.mit.microgrid.flink.job;

import com.mit.microgrid.flink.clickhouse.Util;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.CollectionUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlinkJob {

    public static JobGraph createJobGraph(String sqls) {
        long startTime = System.currentTimeMillis();
        if (sqls == null || sqls.isEmpty()) return null;
        List<String> sqlList = Arrays.asList(sqls.split(";"));
        System.out.println("执行多条sql: " + sqlList);

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        ArrayList<TableResult> tableResults = new ArrayList<>();
        if (CollectionUtil.isNullOrEmpty(sqlList)) return null;
        for (String s : sqlList) {
            TableResult tableResult = tableEnv.executeSql(s);
            tableResults.add(tableResult);
        }

        // 返回 JobGraph
        String s = Util.formatDuration(startTime, System.currentTimeMillis());
        System.out.println("execFlinkSqls耗时：" + s);
        return env.getStreamGraph().getJobGraph();
    }


}
