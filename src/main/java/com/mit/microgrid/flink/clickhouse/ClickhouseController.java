package com.mit.microgrid.flink.clickhouse;

import com.mit.microgrid.common.bean.domain.flink.ClickhouseJobInfo;
import com.mit.microgrid.common.core.domain.AjaxResult;
import com.mit.microgrid.common.core.web.BaseController;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@Slf4j
@RequestMapping("/flink/clickhouse")
public class ClickhouseController extends BaseController {

    @Autowired
    private ClickhouseService clickhouseService;

    /**
     * 单条sql执行入口
     *
     * @param sql
     * @return
     */
    @RequestMapping("/flinkSql")
    public AjaxResult flinkSql(@RequestBody String sql) {
        clickhouseService.execFlinkSql(sql);
        return AjaxResult.success();
    }

    /**
     * 多条sql执行入口
     *
     * @param sqls
     * @return job客户端信息
     */
    @PostMapping("/flinkSqls")
    public AjaxResult<ClickhouseJobInfo> flinkSqls(@RequestBody String sqls) throws ExecutionException, InterruptedException {
        return AjaxResult.success(clickhouseService.execFlinkSqls(sqls));
    }

    /**
     * 提交任务至flink集群
     *
     * @param sql
     * @return
     */
    @RequestMapping("/remoteExecuteSql")
    public AjaxResult remoteExecuteSql(@RequestBody String sql) {
        clickhouseService.remoteExecuteSql(sql);
        return AjaxResult.success();
    }

    /**
     * test
     *
     * @return
     */
    @RequestMapping("/test")
    public AjaxResult test() {
        return AjaxResult.success();
    }

}
