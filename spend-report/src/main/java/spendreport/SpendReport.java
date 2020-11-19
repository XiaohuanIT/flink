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

package spendreport;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.walkthrough.common.table.SpendReportTableSink;
import org.apache.flink.walkthrough.common.table.BoundedTransactionTableSource;
import org.apache.flink.walkthrough.common.table.TruncateDateToHour;

/**
 * Skeleton code for the table walkthrough
 */
public class SpendReport {
	public static void main(String[] args) throws Exception {
		/*
		运行环境
		前两行设置了你的 ExecutionEnvironment。 运行环境用来设置作业的属性、指定应用是批处理还是流处理，以及创建数据源。
		由于你正在建立一个定时的批处理报告，本教程以批处理环境作为开始。 然后将其包装进 BatchTableEnvironment 中从而能够使用所有的 Tabel API。
		 */
		ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		/*
		注册表
		接下来，表将会被注册到运行环境之中，这样你就可以用它们连接外部系统以读取或写入批数据或流数据。
		source 提供对存储在外部系统中的数据的访问；例如数据库、键-值存储、消息队列或文件系统。
		sink 则将表中的数据发送到外部存储系统。 根据 source 或 sink 的类型，它们支持不同的格式，如 CSV、JSON、Avro 或 Parquet。
		 */
		tEnv.registerTableSource("transactions", new BoundedTransactionTableSource());
		tEnv.registerTableSink("spend_report", new SpendReportTableSink());

		/*
		注册 UDF
		一个用来处理时间戳的自定义函数随表一起被注册到tEnv中。 此函数将时间戳向下舍入到最接近的小时。
		 */
		tEnv.registerFunction("truncateDateToHour", new TruncateDateToHour());

		/*
		查询
		完成配置环境和注册表后，你已准备好构建第一个应用程序。
		从 TableEnvironment 中，你可以 scan 一个输入表读取其中的行，然后用 insertInto 把这些数据写到输出表中。
		 */
		tEnv
			.scan("transactions")
			.window(Tumble.over("1.hour").on("timestamp").as("w"))
			.groupBy("accountId, w")
			.select("accountId, w.start as timestamp, amount.sum")
			.insertInto("spend_report");

		/*
		运行
		Flink 应用是延迟构建的，只有完全定义好之后才交付集群运行。
		你可以调用 ExecutionEnvironment#execute 来开始作业的执行并给它取一个名字。
		 */
		env.execute("Spend Report");
	}
}
