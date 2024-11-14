package com.mit.microgrid.flink.clickhouse;

import com.mit.microgrid.common.bean.domain.computation.po.ComputationTaskHistoryPo;
import com.mit.microgrid.common.bean.domain.flink.ClickhouseJobInfo;
import com.mit.microgrid.common.core.constant.SecurityConstants;
import com.mit.microgrid.common.core.domain.AjaxResult;
import com.mit.microgrid.history.api.RemoteHistoryService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableResult;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class ClickhouseService {

    @Resource
    private RemoteHistoryService remoteHistoryService;

    /**
     * 执行flink sql
     *
     * @param sql
     */
    public void execFlinkSql(String sql) {
        long startTime = System.currentTimeMillis();
        if (sql == null || sql.isEmpty()) {
            return;
        }
        System.out.println("执行单条sql: " + sql);
        TableResult tableResult = ClickhouseConnector.flinkSqlClient(sql);
        System.out.println(tableResult);
        String s = Util.formatDuration(startTime, System.currentTimeMillis());
        System.out.println("execFlinkSql耗时：" + s);
    }

    /**
     * 执行多条Flink sql
     *
     * @param sqls
     */
    public void execFlinkSqlsInsert(String sqls) throws ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();
        if (sqls == null || sqls.isEmpty()) return;
        log.info("orignal sqls: ", sqls);
        //处理字符，将字符串中所有连续的空白字符（包括多个空格、制表符、换行符等）替换为一个单个的空格
        String sqlStatements = sqls.replaceAll("\\s+", " ").trim();
        sqlStatements = sqlStatements.replaceAll("`", "");
        //分隔多条sql
        String[] statements = sqls.contains(SqlParserUtil.SEMICOLON) ? sqlStatements.split(SqlParserUtil.SEMICOLON) : new String[]{sqlStatements};
        List<String> sqlList = Arrays.asList(sqls.split(";"));
        System.out.println("执行多条sql: " + sqlList);
        JobClient jobClient = ClickhouseConnector.flinkSqlJobClientMultiple(sqlList);
        if (jobClient == null) {
            log.error("任务执行失败");
            return;
        }
        ComputationTaskHistoryPo computationTaskHistory = new ComputationTaskHistoryPo();
        computationTaskHistory.setHistoryId(99L);
        computationTaskHistory.setComputationTaskId(2L);
        computationTaskHistory.setExecuteType(2);
        computationTaskHistory.setStartDate(LocalDateTime.now());
        computationTaskHistory.setEntDate(LocalDateTime.now());
        computationTaskHistory.setHistoryCreatedTime(new Date());
        String jobId = jobClient.getJobID().toString();
        log.info("jobId: " + jobId);
        computationTaskHistory.setHistoryFlinkId(jobId);
        String currentJobStatus = jobClient.getJobStatus().get().toString();
        log.info("currentJobStatus: " + currentJobStatus);
        computationTaskHistory.setExecuteStatus(currentJobStatus);
        jobClient.getJobExecutionResult().thenAccept(executionResult -> {
            log.info("job completed successfully!");
            JobExecutionResult sucExecutionResult = executionResult;
            computationTaskHistory.setHistoryJobRuntime(sucExecutionResult.getNetRuntime());
            computationTaskHistory.setExecuteStatus("finished");
            log.info(sucExecutionResult.toString());
            //成功，更新
            AjaxResult<Object> objectAjaxResult = remoteHistoryService.logAdd(SecurityConstants.INNER, computationTaskHistory);
        }).exceptionally(ex -> {
            log.error("job failed", ex);
            computationTaskHistory.setExecuteStatus("failed");
            //失败
            AjaxResult<Object> objectAjaxResult = remoteHistoryService.logAdd(SecurityConstants.INNER, computationTaskHistory);
            return null;
        });
        //初始化
        AjaxResult<Object> objectAjaxResult = remoteHistoryService.logAdd(SecurityConstants.INNER, computationTaskHistory);
        String s = Util.formatDuration(startTime, System.currentTimeMillis());
        System.out.println("execFlinkSqls耗时：" + s);
    }

    /**
     * 执行多条Flink sql
     *
     * @param sqls
     */
    public ClickhouseJobInfo execFlinkSqls(String sqls) throws ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();
        if (sqls == null || sqls.isEmpty()) {
            log.error("参数不正确， sqls为空");
            throw new RuntimeException("参数不正确， sqls为空");
        }
        log.info("orignal sqls: ", sqls);
        //处理字符，将字符串中所有连续的空白字符（包括多个空格、制表符、换行符等）替换为一个单个的空格
        String sqlStatements = sqls.replaceAll("\\s+", " ").trim();
        sqlStatements = sqlStatements.replaceAll("`", "");
        log.info("sqlStatements: " + sqlStatements);
        //分隔多条sql
        List<String> sqlList = Arrays.asList(sqls.split(";"));
        System.out.println("执行多条sql: " + sqlList);
        JobClient jobClient = ClickhouseConnector.flinkSqlJobClientMultiple(sqlList);
        if (jobClient == null) {
            log.error("flink任务执行失败");
            throw new RuntimeException("flink任务执行失败");
        }
        String duration = Util.formatDuration(startTime, System.currentTimeMillis());
        ClickhouseJobInfo clickhouseJobInfo = ClickhouseJobInfo.builder().jobClient(jobClient).duration(duration).build();
        log.info("execFlinkSqls耗时：" + duration);
        return clickhouseJobInfo;
    }

    /**
     * 执行多条Flink sql至flink集群
     *
     * @param sqls
     */
    public void remoteExecuteSql(String sqls) {
        long startTime = System.currentTimeMillis();
        if (sqls == null || sqls.isEmpty()) return;
        List<String> sqlList = Arrays.asList(sqls.split(";"));
        System.out.println("执行多条sql: " + sqlList);
        ArrayList<TableResult> tableResults = ClickhouseConnector.remoteExecuteSql(sqlList);
        tableResults.forEach(System.out::println);
        String s = Util.formatDuration(startTime, System.currentTimeMillis());
        System.out.println("execFlinkSqls耗时：" + s);
    }
}
