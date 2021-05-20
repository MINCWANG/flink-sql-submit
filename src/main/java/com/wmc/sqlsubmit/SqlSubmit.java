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

package com.wmc.sqlsubmit;

import com.wmc.sqlsubmit.cli.CliOptions;
import com.wmc.sqlsubmit.cli.CliOptionsParser;
import com.wmc.sqlsubmit.cli.SqlCommandParser;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class SqlSubmit {

    public static void main(String[] args) throws Exception {
        final CliOptions options = CliOptionsParser.parseClient(args);
        SqlSubmit submit = new SqlSubmit(options);
        submit.run();
    }

    // --------------------------------------------------------------------------------------------

    private String sqlFilePath;
    // private String workSpace;
    private StreamTableEnvironment tEnv;
    private StatementSet statementSet;
    private StreamExecutionEnvironment env;

    private SqlSubmit(CliOptions options) {
        this.sqlFilePath = options.getSqlFilePath();
        // this.workSpace = options.getWorkingSpace();
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
    // TODO
    // DROP
    // CREATE_CATALOG
    private void callCommand(SqlCommandParser.SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
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
        if ("execution.checkpointing.interval".equals(key)){
            env.enableCheckpointing(Long.parseLong(value));
        }
        else if ("execution.checkpointing.min-pause".equals(key)){
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Long.parseLong(value));
        }else {
            tEnv.getConfig().getConfiguration().setString(key, value);
        }
    }

    private void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        try {
            statementSet.addInsertSql(dml);
            // tEnv.executeSql(dml);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }
}
