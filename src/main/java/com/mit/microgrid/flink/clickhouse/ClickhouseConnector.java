package com.mit.microgrid.flink.clickhouse;

import com.mit.microgrid.common.bean.domain.computation.dto.ComputationTaskConfigDto;
import com.mit.microgrid.common.bean.domain.computation.po.ComputationTaskHistoryPo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.catalog.ClickHouseCatalog;
import org.apache.flink.connector.clickhouse.config.ClickHouseConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.util.CollectionUtil;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * clickhouse连接器
 *
 * @Description 提供sql客户端功能
 */
@Slf4j
public class ClickhouseConnector {

    /**
     * sql client to clickhouse
     *
     * @param sql
     */
    public static TableResult flinkSqlClient(String sql) {
        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(setting);
        log.info("执行flink sql: " + sql);
        TableResult sqlResult = tEnv.executeSql(sql);
        return sqlResult;
    }

    /**
     * multiple sql execute
     *
     * @param sqlList
     */
    public static ArrayList<TableResult> flinkSqlClientMultiple(List<String> sqlList) throws ExecutionException, InterruptedException {
//        Configuration configuration = new Configuration();
//        configuration.setInteger("web.port", 8081);
//        EnvironmentSettings setting = EnvironmentSettings.newInstance()
//                .inBatchMode()
//                .build();
//        TableEnvironment tEnv = TableEnvironment.create(setting);
//        tEnv.getConfig().setRootConfiguration(configuration);
//        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);
        ArrayList<TableResult> tableResults = new ArrayList<>();
        if (CollectionUtil.isNullOrEmpty(sqlList)) return tableResults;
        for (String s : sqlList) {
            TableResult tableResult = tEnv.executeSql(s);
            Optional<JobClient> jobClientOptional = tableResult.getJobClient();
            if (jobClientOptional.isPresent()) {
                JobClient jobClient = jobClientOptional.get();
                String jobId = jobClient.getJobID().toString();
                log.info("jobId: " + jobId);
                String currentJobStatus = jobClient.getJobStatus().get().toString();
                log.info("currentJobStatus: " + currentJobStatus);
                jobClient.getJobExecutionResult().thenAccept(executionResult -> {
                    log.info("job completed successfully!");
                    JobExecutionResult sucExecutionResult = executionResult;
                    log.info(sucExecutionResult.toString());
                }).exceptionally(ex -> {
                    log.error("job failed" , ex);
                    return null;
                });
            }
            tableResults.add(tableResult);
        }
        return tableResults;
    }

    /**
     * multiple sql execute
     *
     * @param sqlList
     */
    public static JobClient flinkSqlJobClientMultiple(List<String> sqlList) {
        log.info("参数sqlList: {}", sqlList);
//        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);
        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(setting);
        if (CollectionUtil.isNullOrEmpty(sqlList)) {
            log.warn("sqlList参数为空");
            return null;
        }
        for (String s : sqlList) {
            TableResult tableResult = tEnv.executeSql(s);
            Optional<JobClient> jobClientOptional = tableResult.getJobClient();
            if (jobClientOptional.isPresent()) {
                JobClient jobClient = jobClientOptional.get();
                log.info("jobClient: " + jobClient);
                return jobClient;
            }
        }
        log.error("没有可执行的job");
        return null;
    }

    /**
     * clickhouse catalog
     *
     * @param catalog
     */
    public static void flinkSqlCatalog(String database, String url, String username,
                                       String password, String catalog, String sql) {
        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(setting);
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseConfig.DATABASE_NAME, database);
        props.put(ClickHouseConfig.URL, url);
        props.put(ClickHouseConfig.USERNAME, username);
        props.put(ClickHouseConfig.PASSWORD, password);
        props.put(ClickHouseConfig.SINK_FLUSH_INTERVAL, "30s");
        Catalog cHcatalog = new ClickHouseCatalog(catalog, props);
        tEnv.registerCatalog(catalog, cHcatalog);
        tEnv.useCatalog(catalog);

        TableResult tableResult = tEnv.executeSql(sql);
        log.info(tableResult.toString());
    }

    /**
     * sql
     */
    private static void sql(TableEnvironment tEnv) {
        String tableRegisterSql = "CREATE TABLE t_user (\n" +
                "    `user_id` BIGINT,\n" +
                "    `user_type` INTEGER,\n" +
                "    `language` STRING,\n" +
                "    `country` STRING,\n" +
                "    `gender` STRING,\n" +
                "    `score` DOUBLE,\n" +
                "    `list` ARRAY<STRING>,\n" +
                "    `map` Map<STRING, BIGINT>,\n" +
                "    PRIMARY KEY (`user_id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'clickhouse',\n" +
                "    'url' = 'clickhouse://192.168.2.179:8123',\n" +
                "    'database-name' = 'mit_microgrid_history',\n" +
                "    'table-name' = 'users',\n" +
                "    'sink.batch-size' = '500',\n" +
                "    'sink.flush-interval' = '1000',\n" +
                "    'sink.max-retries' = '3',\n" +
                "    'username' = 'default',\n" +
                "    'password' = '123456'\n" +
                ");";
        TableResult registerResult = tEnv.executeSql(tableRegisterSql);
        log.info(registerResult.toString());

        String sinkSql = "CREATE TABLE t_user2 (\n" +
                "    `user_id` BIGINT,\n" +
                "    `user_type` INTEGER,\n" +
                "    `language` STRING,\n" +
                "    `country` STRING,\n" +
                "    `gender` STRING,\n" +
                "    `score` DOUBLE,\n" +
                "    `list` ARRAY<STRING>,\n" +
                "    `map` Map<STRING, BIGINT>,\n" +
                "    PRIMARY KEY (`user_id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'clickhouse',\n" +
                "    'url' = 'clickhouse://192.168.2.179:8123',\n" +
                "    'database-name' = 'mit_microgrid_history',\n" +
                "    'table-name' = 'users2',\n" +
                "    'sink.batch-size' = '500',\n" +
                "    'sink.flush-interval' = '1000',\n" +
                "    'sink.max-retries' = '3',\n" +
                "    'username' = 'default',\n" +
                "    'password' = '123456'\n" +
                ");";
        TableResult querySqlResult = tEnv.executeSql(sinkSql);
        log.info(querySqlResult.toString());

        String outSql = "INSERT INTO t_user select * from t_user2";
        TableResult outResult = tEnv.executeSql(outSql);
        outResult.print();

    }

    /**
     * create and use ClickHouseCatalog
     */
    private static void catalog(TableEnvironment tEnv) {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseConfig.DATABASE_NAME, "mit_microgrid_history");
        props.put(ClickHouseConfig.URL, "clickhouse://192.168.2.179:8123");
        props.put(ClickHouseConfig.USERNAME, "default");
        props.put(ClickHouseConfig.PASSWORD, "123456");
        props.put(ClickHouseConfig.SINK_FLUSH_INTERVAL, "30s");
        Catalog cHcatalog = new ClickHouseCatalog("clickhouse", props);
        tEnv.registerCatalog("clickhouse", cHcatalog);
        tEnv.useCatalog("clickhouse");

        tEnv.executeSql("insert into `clickhouse`.`mit_microgrid_history`.`users` select * from `clickhouse`.`mit_microgrid_history`.`users2`");
    }

    /**
     * 远程提交任务执行sql
     *
     * @param sqlList 多条sql
     * @return
     */
    public static ArrayList<TableResult> remoteExecuteSql(List<String> sqlList) {
        Configuration config = new Configuration();
        config.setString("jobmanager.address", "http://hadoop01:9065"); // 设置集群地址
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(config, Thread.currentThread().getContextClassLoader()); // 配置执行环境
        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .inStreamingMode() // 设置为流模式
                .build();

        TableEnvironment tEnv = TableEnvironment.create(setting);
//        tEnv.createTemporarySystemFunction("你的函数名", YourFunctionClass.class); // 如果需要注册函数
        ArrayList<TableResult> tableResults = new ArrayList<>();
        if (CollectionUtil.isNullOrEmpty(sqlList)) return tableResults;
        sqlList.forEach(s -> tableResults.add(tEnv.executeSql(s)));
        return tableResults;
    }
}
