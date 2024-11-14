package com.mit.microgrid.flink.clickhouse;

import com.mit.microgrid.common.core.exception.ServiceException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SqlParserUtil {

    public static final String SEMICOLON = ";";

    /*public static TableInfo parse(String sqls) {
        //校验空
        if (sqls == null || sqls.isEmpty()) {
            log.error("sqls is null or empty");
            return null;
        }
        //处理字符，将字符串中所有连续的空白字符（包括多个空格、制表符、换行符等）替换为一个单个的空格
        String sqlStatements = sqls.replaceAll("\\s+", " ").trim();
        sqlStatements = sqlStatements.replaceAll("`", "");
        //分隔多条sql
        String[] statements = sqls.contains(SEMICOLON) ? sqlStatements.split(SEMICOLON) : new String[]{sqlStatements};
        //解析
        Config config = SqlParser.config()
                .withCaseSensitive(false)
//                .withParserFactory(SqlDdlParserImpl.FACTORY)
                ;
        TableInfo tableInfo = new TableInfo();
        for (String statement : statements) {
            if (!statement.trim().isEmpty()) {
                System.out.println(statement.trim() + SEMICOLON);
                SqlParser sqlParser = SqlParser.create(statement.trim(), config);
                try {
                    SqlNode sqlNode = sqlParser.parseStmt();
                    if (sqlNode instanceof SqlCreateTable) {
                        SqlCreateTable createTable = (SqlCreateTable) sqlNode;
                        System.out.println(createTable.name);
                        tableInfo.setDesTable(createTable.name.toString());
                        ArrayList<ParamField> paramFields = new ArrayList<ParamField>();
                        SqlNodeList columnList = createTable.columnList;
                        for (SqlNode node : columnList) {
                            if (node instanceof SqlColumnDeclaration) {
                                SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) node;
                                SqlIdentifier name = columnDeclaration.name;
                                SqlDataTypeSpec dataType = columnDeclaration.dataType;
                                ParamField paramField = new ParamField();
                                paramField.setParamField(name.toString());
                                paramField.setParamFieldName(dataType.toString());
                            }
                        }
                    }
                    //访问sql语法数，提取信息
                    sqlNode.accept(new SqlBasicVisitor<Void>() {
                        public Void visit(SqlCreateTable createTable) {
                            String tableName = createTable.name.toString();
                            SqlNodeList columnList = createTable.columnList;
                            for (SqlNode column : columnList) {
                                String columnString = column.toString();
                                System.out.println("字段：" + columnString);
                            }
                            return null;
                        }

                        public Void visit(SqlInsert insert) {
                            String targetTable = insert.getTargetTable().toString();
                            System.out.println("目标表" + targetTable);
                            return null;
                        }
                    });
                } catch (SqlParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return tableInfo;
    }*/

    public static void main(String[] args) throws SqlParseException {
//        String sql = "CREATE TABLE t_user ( `user_id` BIGINT, `user_type` INTEGER, `language` STRING, `country` STRING, `gender` STRING, `score` DOUBLE, `list` ARRAY<STRING>, `map` Map<STRING, BIGINT> ) ;";
        String sql = "create table t_user ( `user_id` BIGINT COMMENT '用户ID');";
//        parse(sql);
        String sqls = "CREATE TABLE t_user ( \n" +
                "    `user_id` BIGINT comment '用户ID', \n" +
                "    `user_type` INTEGER COMMENT '用户类型', \n" +
                "    `language` STRING comment '语种', \n" +
                "    `country` STRING, \n" +
                "    `gender` STRING, \n" +
                "    `score` DOUBLE, \n" +
                "    `list` ARRAY<STRING>, \n" +
                "    `map` Map<STRING, BIGINT>, \n" +
                "    PRIMARY KEY (`user_id`) NOT ENFORCED \n" +
                ") WITH ( \n" +
                "    'connector' = 'clickhouse', \n" +
                "    'url' = 'clickhouse://192.168.2.179:8123', \n" +
                "    'database-name' = 'mit_microgrid_history', \n" +
                "    'table-name' = 'users', \n" +
                "    'sink.batch-size' = '500', \n" +
                "    'sink.flush-interval' = '1000', \n" +
                "    'sink.max-retries' = '3', \n" +
                "    'username' = 'default', \n" +
                "    'password' = '123456' \n" +
                ");\n" +
                "\n" +
                "CREATE TABLE t_user2 ( \n" +
                "    `user_id` BIGINT, \n" +
                "    `user_type` INTEGER, \n" +
                "    `language` STRING, \n" +
                "    `country` STRING, \n" +
                "    `gender` STRING, \n" +
                "    `score` DOUBLE, \n" +
                "    `list` ARRAY<STRING>, \n" +
                "    `map` Map<STRING, BIGINT>, \n" +
                "    PRIMARY KEY (`user_id`) NOT ENFORCED \n" +
                ") WITH ( \n" +
                "    'connector' = 'clickhouse', \n" +
                "    'url' = 'clickhouse://192.168.2.179:8123', \n" +
                "    'database-name' = 'mit_microgrid_history', \n" +
                "    'table-name' = 'users2', \n" +
                "    'sink.batch-size' = '500', \n" +
                "    'sink.flush-interval' = '1000', \n" +
                "    'sink.max-retries' = '3', \n" +
                "    'username' = 'default', \n" +
                "    'password' = '123456' \n" +
                "); \n" +
                "\n" +
                "INSERT INTO t_user select * from t_user2;\n";
//        parse(sqls);
        flinkParser(sqls);
    }

    /**
     * check sql
     *
     * @param sqls
     * @return
     */
    public static SqlNodeList flinkChecker(String sqls) {
        if (sqls == null || sqls.isEmpty()) {
            System.out.println("sqls is null or empty");
            throw new ServiceException("sql不能为空");
        }
        SqlParser sqlParser = SqlParser.create(sqls, SqlParser.config()
                .withParserFactory(FlinkSqlParserImpl.FACTORY)
                .withQuoting(Quoting.BACK_TICK)
                .withUnquotedCasing(Casing.TO_LOWER)
                .withQuotedCasing(Casing.UNCHANGED)
                .withConformance(FlinkSqlConformance.DEFAULT));
        SqlNodeList sqlNodeList = null;    //校验
        try {
            sqlNodeList = sqlParser.parseStmtList();
        } catch (SqlParseException e) {
            System.out.println("sql校验失败");
            throw new ServiceException("sql校验失败").setDetailMessage(e.getMessage());
        }
        return sqlNodeList;
    }

    /**
     * flink sql parser multiple sqls
     *
     * @param sqls
     * @return
     */
    public static TableInfo flinkParser(String sqls) {
        SqlNodeList sqlNodeList = flinkChecker(sqls);
        TableInfo tableInfo = new TableInfo();
        tableInfo.setOriSqlString(sqls);
        HashMap<String, SqlNodeList> createTableColumnListMap = new HashMap<>();
        tableInfo.setCreateTableColumnListMap(createTableColumnListMap);
        List<@Nullable SqlNode> sqlNodes = sqlNodeList.getList();
        for (SqlNode sqlNode : sqlNodes) {
            switch (sqlNode.getKind()) {
                case SELECT:
                    SqlNodeList nodes = ((SqlSelect) sqlNode).getSelectList();
                    System.out.println(nodes);
                    break;
                case INSERT:
                    SqlInsert insert = (SqlInsert) sqlNode;
                    tableInfo.setDesTable(insert.getTargetTable().toString());
                    SqlNodeList selectList = createTableColumnListMap.get(insert.getTargetTable().toString());
                    if (selectList == null) {
                        System.out.println("未匹配目标表");
                        throw new ServiceException("sql校验失败，未匹配目标表");
                    }
                    ArrayList<ParamField> paramFields = new ArrayList<>();
                    for (SqlNode sqlnode : selectList) {
                        ParamField paramField = new ParamField();
                        SqlTableColumn column = (SqlTableColumn) sqlnode;
                        paramField.setParamField(column.getName().toString());
                        column.getComment().ifPresent(comment -> paramField.setParamFieldComment(delSingleQuotationMark(comment.toString())));
                        paramField.setParamFieldType(((SqlTableColumn.SqlRegularColumn) sqlnode).getType().toString());
                        paramFields.add(paramField);
                    }
                    tableInfo.setParamFields(paramFields);
                    SqlNode source = insert.getSource();
                    if (source instanceof SqlSelect) {
                        SqlNode srcTable = ((SqlSelect) source).getFrom();
                        tableInfo.setSrcTable(srcTable.toString());
                    }
                    break;
                case UPDATE:
                    SqlNodeList targetColumnList2 = ((SqlUpdate) sqlNode).getTargetColumnList();
                    break;
                case DELETE:
                    SqlNode targetTable = ((SqlDelete) sqlNode).getTargetTable();
                    break;
                case CREATE_TABLE:
                    SqlCreateTable createTable = (SqlCreateTable) sqlNode;
                    System.out.println(createTable);
                    SqlIdentifier tableName = createTable.getTableName();
                    SqlNodeList columnList = createTable.getColumnList();
                    createTableColumnListMap.put(tableName.toString(), columnList);
                    break;
                default:
                    return null;
            }
        }
        return tableInfo;
    }

    @Data
    public static class TableInfo {
        private String desTable;    //目标表
        private List<ParamField> paramFields;   //字段集合
        private String srcTable;    //源表
        private String oriSqlString;  //原sql
        private Map<String, SqlNodeList> createTableColumnListMap;
    }

    @Data
    public static class ParamField {
        private String paramField;  //字段
        private String paramFieldComment;  //字段别名
        private String paramFieldType;  //字段类型
    }

    /**
     * 清除前后单引号
     *
     * @param comment
     * @return
     */
    private static String delSingleQuotationMark(String comment) {
        if (comment.startsWith("'") && comment.endsWith("'")) {
            return comment.substring(1, comment.length() - 1);
        }
        return comment;
    }
}






