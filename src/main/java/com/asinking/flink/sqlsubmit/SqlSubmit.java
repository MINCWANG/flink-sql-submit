/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.asinking.flink.sqlsubmit;

import com.asinking.flink.sqlsubmit.cli.CliOptions;
import com.asinking.flink.sqlsubmit.cli.CliOptionsParser;
import com.asinking.flink.sqlsubmit.cli.SqlCommandParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlSubmit {

    private final String sqlFilePath;

    // --------------------------------------------------------------------------------------------
    // private String workSpace;
    private StreamTableEnvironment tEnv;
    private StatementSet statementSet;
    private StreamExecutionEnvironment env;

    private SqlSubmit(CliOptions options) {
        this.sqlFilePath = options.getSqlFilePath();
        // this.workSpace = options.getWorkingSpace();
    }

    public static void main(String[] args) throws Exception {
        final CliOptions options = CliOptionsParser.parseClient(args);
        SqlSubmit submit = new SqlSubmit(options);
        submit.run();
    }

    private void run() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tEnv = StreamTableEnvironment.create(env, bsSettings);
        statementSet = tEnv.createStatementSet();
        List<String> sql = Files.readAllLines(Paths.get(sqlFilePath));
        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sql);
        for (SqlCommandParser.SqlCommandCall call : calls) {
            callCommand(call);
        }
        statementSet.execute();
    }

    // --------------------------------------------------------------------------------------------

    private void callCommand(SqlCommandParser.SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case EXPORT:
                callExport(cmdCall);
                break;
            case CREATE_TABLE:
            case DROP_TABLE:
            case CREATE_CATALOG:
                callCreateTable(cmdCall);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callSet(SqlCommandParser.SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        switch (key) {
            case "execution.checkpointing.interval":
                env.enableCheckpointing(Long.parseLong(value.trim()));
                break;
            case "execution.checkpointing.min-pause":
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Long.parseLong(value.trim()));
                break;
            case "execution.checkpointing.tolerable-failed-checkpoints":
                env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.parseInt(value.trim()));
                break;
            case "execution.checkpointing.unaligned":
                env.getCheckpointConfig().enableUnalignedCheckpoints(Boolean.parseBoolean(value.trim()));
                break;
            default:
                tEnv.getConfig().getConfiguration().setString(key, value.trim());
        }
    }

    private void callExport(SqlCommandParser.SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        HashMap<String, String> map = new HashMap<>(1);
        map.put(key, value);
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(map));
    }

    private void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = parseVariable(cmdCall.operands[0]);

        try {
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = parseVariable(cmdCall.operands[0]);
        try {
            statementSet.addInsertSql(dml);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }

    private String parseVariable(String s) {
        Map<String, String> map = env.getConfig().getGlobalJobParameters().toMap();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            s = s.replaceAll("\\$" + entry.getKey(), entry.getValue().trim());
        }
        return s;
    }
}
