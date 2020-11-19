来自于官网的例子[table api](https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/getting-started/walkthroughs/table_api.html)

```java
import org.apache.flink.api.java.ExecutionEnvironment;
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
			.insertInto("spend_report");

		/*
		运行
		Flink 应用是延迟构建的，只有完全定义好之后才交付集群运行。
		你可以调用 ExecutionEnvironment#execute 来开始作业的执行并给它取一个名字。
		 */
		env.execute("Spend Report");
	}
}
```

执行完成后，输出是怎么来的？没有找到source。

```
/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/bin/java -javaagent:/Applications/IntelliJ IDEA.app/Contents/lib/idea_rt.jar=53157:/Applications/IntelliJ IDEA.app/Contents/bin -Dfile.encoding=UTF-8 -classpath /Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/charsets.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/deploy.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/cldrdata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/dnsns.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/jaccess.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/jfxrt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/localedata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/nashorn.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/sunec.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/sunjce_provider.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/sunpkcs11.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/zipfs.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/javaws.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jce.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jfr.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jfxswt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jsse.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/management-agent.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/plugin.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/resources.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/rt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/ant-javafx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/dt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/javafx-mx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/jconsole.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/packager.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/sa-jdi.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/tools.jar:/Users/yangxiaohuan/my_private/github/my_public/flink/spend-report/target/classes:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-walkthrough-common_2.11/1.10.0/flink-walkthrough-common_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/slf4j/slf4j-api/1.7.15/slf4j-api-1.7.15.jar:/Users/yangxiaohuan/.m2/repository/com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/force-shading/1.10.0/force-shading-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-java/1.10.0/flink-java-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-core/1.10.0/flink-core-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-annotations/1.10.0/flink-annotations-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-metrics-core/1.10.0/flink-metrics-core-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/com/esotericsoftware/kryo/kryo/2.24.0/kryo-2.24.0.jar:/Users/yangxiaohuan/.m2/repository/com/esotericsoftware/minlog/minlog/1.2/minlog-1.2.jar:/Users/yangxiaohuan/.m2/repository/org/objenesis/objenesis/2.1/objenesis-2.1.jar:/Users/yangxiaohuan/.m2/repository/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar:/Users/yangxiaohuan/.m2/repository/org/apache/commons/commons-compress/1.18/commons-compress-1.18.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-asm-7/7.1-9.0/flink-shaded-asm-7-7.1-9.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/commons/commons-lang3/3.3.2/commons-lang3-3.3.2.jar:/Users/yangxiaohuan/.m2/repository/org/apache/commons/commons-math3/3.5/commons-math3-3.5.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-streaming-java_2.11/1.10.0/flink-streaming-java_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-runtime_2.11/1.10.0/flink-runtime_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-queryable-state-client-java/1.10.0/flink-queryable-state-client-java-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-hadoop-fs/1.10.0/flink-hadoop-fs-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/commons-io/commons-io/2.4/commons-io-2.4.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-netty/4.1.39.Final-9.0/flink-shaded-netty-4.1.39.Final-9.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-jackson/2.10.1-9.0/flink-shaded-jackson-2.10.1-9.0.jar:/Users/yangxiaohuan/.m2/repository/commons-cli/commons-cli/1.3.1/commons-cli-1.3.1.jar:/Users/yangxiaohuan/.m2/repository/org/javassist/javassist/3.24.0-GA/javassist-3.24.0-GA.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/scala-library/2.11.12/scala-library-2.11.12.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-actor_2.11/2.5.21/akka-actor_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/config/1.3.3/config-1.3.3.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/modules/scala-java8-compat_2.11/0.7.0/scala-java8-compat_2.11-0.7.0.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-stream_2.11/2.5.21/akka-stream_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/org/reactivestreams/reactive-streams/1.0.2/reactive-streams-1.0.2.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/ssl-config-core_2.11/0.3.7/ssl-config-core_2.11-0.3.7.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/modules/scala-parser-combinators_2.11/1.1.1/scala-parser-combinators_2.11-1.1.1.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-protobuf_2.11/2.5.21/akka-protobuf_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-slf4j_2.11/2.5.21/akka-slf4j_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/org/clapper/grizzled-slf4j_2.11/1.3.2/grizzled-slf4j_2.11-1.3.2.jar:/Users/yangxiaohuan/.m2/repository/com/github/scopt/scopt_2.11/3.5.0/scopt_2.11-3.5.0.jar:/Users/yangxiaohuan/.m2/repository/org/xerial/snappy/snappy-java/1.1.4/snappy-java-1.1.4.jar:/Users/yangxiaohuan/.m2/repository/com/twitter/chill_2.11/0.7.6/chill_2.11-0.7.6.jar:/Users/yangxiaohuan/.m2/repository/com/twitter/chill-java/0.7.6/chill-java-0.7.6.jar:/Users/yangxiaohuan/.m2/repository/org/lz4/lz4-java/1.5.0/lz4-java-1.5.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-clients_2.11/1.10.0/flink-clients_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-optimizer_2.11/1.10.0/flink-optimizer_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-guava/18.0-9.0/flink-shaded-guava-18.0-9.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-java-bridge_2.11/1.10.0/flink-table-api-java-bridge_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-java/1.10.0/flink-table-api-java-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-planner_2.11/1.10.0/flink-table-planner_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-common/1.10.0/flink-table-common-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-scala-bridge_2.11/1.10.0/flink-table-api-scala-bridge_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-scala_2.11/1.10.0/flink-table-api-scala_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/scala-reflect/2.11.12/scala-reflect-2.11.12.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/scala-compiler/2.11.12/scala-compiler-2.11.12.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/modules/scala-xml_2.11/1.0.5/scala-xml_2.11-1.0.5.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-scala_2.11/1.10.0/flink-scala_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-streaming-scala_2.11/1.10.0/flink-streaming-scala_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/slf4j/slf4j-log4j12/1.7.7/slf4j-log4j12-1.7.7.jar:/Users/yangxiaohuan/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar spendreport.SpendReport
16:13:26,045 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - Log file environment variable 'log.file' is not set.
16:13:26,046 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - JobManager log files are unavailable in the web dashboard. Log file location not found in environment variable 'log.file' or configuration key 'Key: 'web.log.path' , default: null (fallback keys: [{key=jobmanager.web.log.path, isDeprecated=true}])'.
16:13:26,594 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 00:00:00.0, $188.23
16:13:26,594 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 00:06:00.0, $374.79
16:13:26,594 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 00:12:00.0, $112.15
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 00:18:00.0, $478.75
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 00:24:00.0, $208.85
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 00:30:00.0, $379.64
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 00:36:00.0, $351.44
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 00:42:00.0, $320.75
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 00:48:00.0, $259.42
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 00:54:00.0, $273.44
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 01:00:00.0, $267.25
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 01:06:00.0, $397.15
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 01:12:00.0, $0.22
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 01:18:00.0, $231.94
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 01:24:00.0, $384.73
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 01:30:00.0, $419.62
16:13:26,595 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 01:36:00.0, $412.91
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 01:42:00.0, $0.77
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 01:48:00.0, $22.10
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 01:54:00.0, $377.54
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 02:00:00.0, $375.44
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 02:06:00.0, $230.18
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 02:12:00.0, $0.80
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 02:18:00.0, $350.89
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 02:24:00.0, $127.55
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 02:30:00.0, $483.91
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 02:36:00.0, $228.22
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 02:42:00.0, $871.15
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 02:48:00.0, $64.19
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 02:54:00.0, $79.43
16:13:26,596 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 03:00:00.0, $56.12
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 03:06:00.0, $256.48
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 03:12:00.0, $148.16
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 03:18:00.0, $199.95
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 03:24:00.0, $252.37
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 03:30:00.0, $274.73
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 03:36:00.0, $473.54
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 03:42:00.0, $119.92
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 03:48:00.0, $323.59
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 03:54:00.0, $353.16
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 04:00:00.0, $211.90
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 04:06:00.0, $280.93
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 04:12:00.0, $347.89
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 04:18:00.0, $459.86
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 04:24:00.0, $82.31
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 04:30:00.0, $373.26
16:13:26,597 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 04:36:00.0, $479.83
16:13:26,598 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 04:42:00.0, $454.25
16:13:26,598 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 04:48:00.0, $83.64
16:13:26,598 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 04:54:00.0, $292.44

Process finished with exit code 0

```



目标是建立一个报表来显示每天每小时每个账户的总支出。 就像一个 SQL 查询一样，Flink 可以选取所需的字段并且按键分组。 由于时间戳字段具有毫秒的粒度，你可以使用自定义函数将其舍入到最近的小时。 最后，选取所有的字段，用内建的 sum 聚合函数函数合计每一个账户每小时的支出。

```java
tEnv
    .scan("transactions")
    .select("accountId, timestamp.truncateDateToHour as timestamp, amount")
    .groupBy("accountId, timestamp")
    .select("accountId, timestamp, amount.sum as total")
    .insertInto("spend_report");
```
 
输出：

```
/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/bin/java -javaagent:/Applications/IntelliJ IDEA.app/Contents/lib/idea_rt.jar=60023:/Applications/IntelliJ IDEA.app/Contents/bin -Dfile.encoding=UTF-8 -classpath /Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/charsets.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/deploy.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/cldrdata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/dnsns.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/jaccess.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/jfxrt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/localedata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/nashorn.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/sunec.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/sunjce_provider.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/sunpkcs11.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/zipfs.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/javaws.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jce.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jfr.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jfxswt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jsse.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/management-agent.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/plugin.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/resources.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/rt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/ant-javafx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/dt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/javafx-mx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/jconsole.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/packager.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/sa-jdi.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/tools.jar:/Users/yangxiaohuan/my_private/github/my_public/flink/spend-report/target/classes:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-walkthrough-common_2.11/1.10.0/flink-walkthrough-common_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/slf4j/slf4j-api/1.7.15/slf4j-api-1.7.15.jar:/Users/yangxiaohuan/.m2/repository/com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/force-shading/1.10.0/force-shading-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-java/1.10.0/flink-java-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-core/1.10.0/flink-core-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-annotations/1.10.0/flink-annotations-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-metrics-core/1.10.0/flink-metrics-core-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/com/esotericsoftware/kryo/kryo/2.24.0/kryo-2.24.0.jar:/Users/yangxiaohuan/.m2/repository/com/esotericsoftware/minlog/minlog/1.2/minlog-1.2.jar:/Users/yangxiaohuan/.m2/repository/org/objenesis/objenesis/2.1/objenesis-2.1.jar:/Users/yangxiaohuan/.m2/repository/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar:/Users/yangxiaohuan/.m2/repository/org/apache/commons/commons-compress/1.18/commons-compress-1.18.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-asm-7/7.1-9.0/flink-shaded-asm-7-7.1-9.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/commons/commons-lang3/3.3.2/commons-lang3-3.3.2.jar:/Users/yangxiaohuan/.m2/repository/org/apache/commons/commons-math3/3.5/commons-math3-3.5.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-streaming-java_2.11/1.10.0/flink-streaming-java_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-runtime_2.11/1.10.0/flink-runtime_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-queryable-state-client-java/1.10.0/flink-queryable-state-client-java-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-hadoop-fs/1.10.0/flink-hadoop-fs-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/commons-io/commons-io/2.4/commons-io-2.4.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-netty/4.1.39.Final-9.0/flink-shaded-netty-4.1.39.Final-9.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-jackson/2.10.1-9.0/flink-shaded-jackson-2.10.1-9.0.jar:/Users/yangxiaohuan/.m2/repository/commons-cli/commons-cli/1.3.1/commons-cli-1.3.1.jar:/Users/yangxiaohuan/.m2/repository/org/javassist/javassist/3.24.0-GA/javassist-3.24.0-GA.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/scala-library/2.11.12/scala-library-2.11.12.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-actor_2.11/2.5.21/akka-actor_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/config/1.3.3/config-1.3.3.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/modules/scala-java8-compat_2.11/0.7.0/scala-java8-compat_2.11-0.7.0.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-stream_2.11/2.5.21/akka-stream_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/org/reactivestreams/reactive-streams/1.0.2/reactive-streams-1.0.2.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/ssl-config-core_2.11/0.3.7/ssl-config-core_2.11-0.3.7.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/modules/scala-parser-combinators_2.11/1.1.1/scala-parser-combinators_2.11-1.1.1.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-protobuf_2.11/2.5.21/akka-protobuf_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-slf4j_2.11/2.5.21/akka-slf4j_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/org/clapper/grizzled-slf4j_2.11/1.3.2/grizzled-slf4j_2.11-1.3.2.jar:/Users/yangxiaohuan/.m2/repository/com/github/scopt/scopt_2.11/3.5.0/scopt_2.11-3.5.0.jar:/Users/yangxiaohuan/.m2/repository/org/xerial/snappy/snappy-java/1.1.4/snappy-java-1.1.4.jar:/Users/yangxiaohuan/.m2/repository/com/twitter/chill_2.11/0.7.6/chill_2.11-0.7.6.jar:/Users/yangxiaohuan/.m2/repository/com/twitter/chill-java/0.7.6/chill-java-0.7.6.jar:/Users/yangxiaohuan/.m2/repository/org/lz4/lz4-java/1.5.0/lz4-java-1.5.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-clients_2.11/1.10.0/flink-clients_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-optimizer_2.11/1.10.0/flink-optimizer_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-guava/18.0-9.0/flink-shaded-guava-18.0-9.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-java-bridge_2.11/1.10.0/flink-table-api-java-bridge_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-java/1.10.0/flink-table-api-java-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-planner_2.11/1.10.0/flink-table-planner_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-common/1.10.0/flink-table-common-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-scala-bridge_2.11/1.10.0/flink-table-api-scala-bridge_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-scala_2.11/1.10.0/flink-table-api-scala_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/scala-reflect/2.11.12/scala-reflect-2.11.12.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/scala-compiler/2.11.12/scala-compiler-2.11.12.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/modules/scala-xml_2.11/1.0.5/scala-xml_2.11-1.0.5.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-scala_2.11/1.10.0/flink-scala_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-streaming-scala_2.11/1.10.0/flink-streaming-scala_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/slf4j/slf4j-log4j12/1.7.7/slf4j-log4j12-1.7.7.jar:/Users/yangxiaohuan/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar spendreport.SpendReport
16:55:55,048 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - Log file environment variable 'log.file' is not set.
16:55:55,049 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - JobManager log files are unavailable in the web dashboard. Log file location not found in environment variable 'log.file' or configuration key 'Key: 'web.log.path' , default: null (fallback keys: [{key=jobmanager.web.log.path, isDeprecated=true}])'.
16:55:55,606 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name FlatMap (select: (accountId, TruncateDateToHour(timestamp) AS timestamp, amount)) exceeded the 80 characters length limit and was truncated.
16:55:55,671 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name GroupCombine (groupBy: (accountId, timestamp), select: (accountId, timestamp, SUM(amount) AS EXPR$0)) exceeded the 80 characters length limit and was truncated.
16:55:55,726 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name GroupReduce (groupBy: (accountId, timestamp), select: (accountId, timestamp, SUM(amount) AS EXPR$0)) exceeded the 80 characters length limit and was truncated.
16:55:55,728 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name GroupReduce (groupBy: (accountId, timestamp), select: (accountId, timestamp, SUM(amount) AS EXPR$0)) exceeded the 80 characters length limit and was truncated.
16:55:55,732 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name GroupReduce (groupBy: (accountId, timestamp), select: (accountId, timestamp, SUM(amount) AS EXPR$0)) exceeded the 80 characters length limit and was truncated.
16:55:55,737 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name GroupReduce (groupBy: (accountId, timestamp), select: (accountId, timestamp, SUM(amount) AS EXPR$0)) exceeded the 80 characters length limit and was truncated.
16:55:55,787 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 01:00:00.0, $686.87
16:55:55,787 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 00:00:00.0, $432.90
16:55:55,787 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 02:00:00.0, $859.35
16:55:55,787 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 04:00:00.0, $802.14
16:55:55,787 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 03:00:00.0, $330.85
16:55:55,787 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 00:00:00.0, $738.17
16:55:55,787 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 04:00:00.0, $585.16
16:55:55,788 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 01:00:00.0, $762.27
16:55:55,788 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 00:00:00.0, $726.23
16:55:55,788 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 02:00:00.0, $458.40
16:55:55,788 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 03:00:00.0, $730.02
16:55:55,788 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 03:00:00.0, $523.54
16:55:55,789 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 04:00:00.0, $543.50
16:55:55,790 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 00:00:00.0, $482.29
16:55:55,794 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 01:00:00.0, $0.99
16:55:55,794 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 01:00:00.0, $254.04
16:55:55,795 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 02:00:00.0, $415.08
16:55:55,795 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 03:00:00.0, $605.53
16:55:55,797 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 00:00:00.0, $567.87
16:55:55,797 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 01:00:00.0, $810.06
16:55:55,797 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 04:00:00.0, $760.76
16:55:55,797 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 02:00:00.0, $871.95
16:55:55,798 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 03:00:00.0, $268.08
16:55:55,798 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 02:00:00.0, $206.98
16:55:55,798 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 04:00:00.0, $374.75

Process finished with exit code 0

```



### 添加窗口
根据时间进行分组在数据处理中是一种很常见的方式，特别是在处理无限的数据流时。 基于时间的分组称为窗口 ，Flink 提供了灵活的窗口语义。 其中最基础的是 Tumble window （滚动窗口），它具有固定大小且窗口之间不重叠。

```java
tEnv
    .scan("transactions")
    .window(Tumble.over("1.hour").on("timestamp").as("w"))
    .groupBy("accountId, w")
    .select("accountId, w.start as timestamp, amount.sum")
    .insertInto("spend_report");
```

输出：

```
/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/bin/java -javaagent:/Applications/IntelliJ IDEA.app/Contents/lib/idea_rt.jar=60486:/Applications/IntelliJ IDEA.app/Contents/bin -Dfile.encoding=UTF-8 -classpath /Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/charsets.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/deploy.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/cldrdata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/dnsns.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/jaccess.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/jfxrt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/localedata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/nashorn.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/sunec.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/sunjce_provider.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/sunpkcs11.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/ext/zipfs.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/javaws.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jce.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jfr.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jfxswt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/jsse.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/management-agent.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/plugin.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/resources.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/rt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/ant-javafx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/dt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/javafx-mx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/jconsole.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/packager.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/sa-jdi.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/lib/tools.jar:/Users/yangxiaohuan/my_private/github/my_public/flink/spend-report/target/classes:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-walkthrough-common_2.11/1.10.0/flink-walkthrough-common_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/slf4j/slf4j-api/1.7.15/slf4j-api-1.7.15.jar:/Users/yangxiaohuan/.m2/repository/com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/force-shading/1.10.0/force-shading-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-java/1.10.0/flink-java-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-core/1.10.0/flink-core-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-annotations/1.10.0/flink-annotations-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-metrics-core/1.10.0/flink-metrics-core-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/com/esotericsoftware/kryo/kryo/2.24.0/kryo-2.24.0.jar:/Users/yangxiaohuan/.m2/repository/com/esotericsoftware/minlog/minlog/1.2/minlog-1.2.jar:/Users/yangxiaohuan/.m2/repository/org/objenesis/objenesis/2.1/objenesis-2.1.jar:/Users/yangxiaohuan/.m2/repository/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar:/Users/yangxiaohuan/.m2/repository/org/apache/commons/commons-compress/1.18/commons-compress-1.18.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-asm-7/7.1-9.0/flink-shaded-asm-7-7.1-9.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/commons/commons-lang3/3.3.2/commons-lang3-3.3.2.jar:/Users/yangxiaohuan/.m2/repository/org/apache/commons/commons-math3/3.5/commons-math3-3.5.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-streaming-java_2.11/1.10.0/flink-streaming-java_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-runtime_2.11/1.10.0/flink-runtime_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-queryable-state-client-java/1.10.0/flink-queryable-state-client-java-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-hadoop-fs/1.10.0/flink-hadoop-fs-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/commons-io/commons-io/2.4/commons-io-2.4.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-netty/4.1.39.Final-9.0/flink-shaded-netty-4.1.39.Final-9.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-jackson/2.10.1-9.0/flink-shaded-jackson-2.10.1-9.0.jar:/Users/yangxiaohuan/.m2/repository/commons-cli/commons-cli/1.3.1/commons-cli-1.3.1.jar:/Users/yangxiaohuan/.m2/repository/org/javassist/javassist/3.24.0-GA/javassist-3.24.0-GA.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/scala-library/2.11.12/scala-library-2.11.12.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-actor_2.11/2.5.21/akka-actor_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/config/1.3.3/config-1.3.3.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/modules/scala-java8-compat_2.11/0.7.0/scala-java8-compat_2.11-0.7.0.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-stream_2.11/2.5.21/akka-stream_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/org/reactivestreams/reactive-streams/1.0.2/reactive-streams-1.0.2.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/ssl-config-core_2.11/0.3.7/ssl-config-core_2.11-0.3.7.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/modules/scala-parser-combinators_2.11/1.1.1/scala-parser-combinators_2.11-1.1.1.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-protobuf_2.11/2.5.21/akka-protobuf_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/com/typesafe/akka/akka-slf4j_2.11/2.5.21/akka-slf4j_2.11-2.5.21.jar:/Users/yangxiaohuan/.m2/repository/org/clapper/grizzled-slf4j_2.11/1.3.2/grizzled-slf4j_2.11-1.3.2.jar:/Users/yangxiaohuan/.m2/repository/com/github/scopt/scopt_2.11/3.5.0/scopt_2.11-3.5.0.jar:/Users/yangxiaohuan/.m2/repository/org/xerial/snappy/snappy-java/1.1.4/snappy-java-1.1.4.jar:/Users/yangxiaohuan/.m2/repository/com/twitter/chill_2.11/0.7.6/chill_2.11-0.7.6.jar:/Users/yangxiaohuan/.m2/repository/com/twitter/chill-java/0.7.6/chill-java-0.7.6.jar:/Users/yangxiaohuan/.m2/repository/org/lz4/lz4-java/1.5.0/lz4-java-1.5.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-clients_2.11/1.10.0/flink-clients_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-optimizer_2.11/1.10.0/flink-optimizer_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-shaded-guava/18.0-9.0/flink-shaded-guava-18.0-9.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-java-bridge_2.11/1.10.0/flink-table-api-java-bridge_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-java/1.10.0/flink-table-api-java-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-planner_2.11/1.10.0/flink-table-planner_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-common/1.10.0/flink-table-common-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-scala-bridge_2.11/1.10.0/flink-table-api-scala-bridge_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-table-api-scala_2.11/1.10.0/flink-table-api-scala_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/scala-reflect/2.11.12/scala-reflect-2.11.12.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/scala-compiler/2.11.12/scala-compiler-2.11.12.jar:/Users/yangxiaohuan/.m2/repository/org/scala-lang/modules/scala-xml_2.11/1.0.5/scala-xml_2.11-1.0.5.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-scala_2.11/1.10.0/flink-scala_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/apache/flink/flink-streaming-scala_2.11/1.10.0/flink-streaming-scala_2.11-1.10.0.jar:/Users/yangxiaohuan/.m2/repository/org/slf4j/slf4j-log4j12/1.7.7/slf4j-log4j12-1.7.7.jar:/Users/yangxiaohuan/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar spendreport.SpendReport
16:58:55,204 WARN  org.apache.flink.api.java.operators.GroupReduceOperator       - Cannot check generic types of GroupReduceFunction. Enabling combiner but combine function might fail at runtime.
16:58:57,291 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - Log file environment variable 'log.file' is not set.
16:58:57,292 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - JobManager log files are unavailable in the web dashboard. Log file location not found in environment variable 'log.file' or configuration key 'Key: 'web.log.path' , default: null (fallback keys: [{key=jobmanager.web.log.path, isDeprecated=true}])'.
16:58:57,922 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name Combine (groupBy: (accountId), window: (TumblingGroupWindow('w, 'timestamp, 3600000.millis)), select: (accountId, SUM(amount) AS EXPR$1, start('w) AS EXPR$0)) exceeded the 80 characters length limit and was truncated.
16:58:58,021 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name Reduce (groupBy: (accountId), window: (TumblingGroupWindow('w, 'timestamp, 3600000.millis)), select: (accountId, SUM(amount) AS EXPR$1, start('w) AS EXPR$0)) exceeded the 80 characters length limit and was truncated.
16:58:58,021 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name Reduce (groupBy: (accountId), window: (TumblingGroupWindow('w, 'timestamp, 3600000.millis)), select: (accountId, SUM(amount) AS EXPR$1, start('w) AS EXPR$0)) exceeded the 80 characters length limit and was truncated.
16:58:58,022 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name Reduce (groupBy: (accountId), window: (TumblingGroupWindow('w, 'timestamp, 3600000.millis)), select: (accountId, SUM(amount) AS EXPR$1, start('w) AS EXPR$0)) exceeded the 80 characters length limit and was truncated.
16:58:58,023 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name Reduce (groupBy: (accountId), window: (TumblingGroupWindow('w, 'timestamp, 3600000.millis)), select: (accountId, SUM(amount) AS EXPR$1, start('w) AS EXPR$0)) exceeded the 80 characters length limit and was truncated.
16:58:58,122 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 00:00:00.0, $567.87
16:58:58,123 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 04:00:00.0, $760.76
16:58:58,123 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 01:00:00.0, $0.99
16:58:58,123 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 02:00:00.0, $871.95
16:58:58,123 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 03:00:00.0, $268.08
16:58:58,123 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 04:00:00.0, $802.14
16:58:58,123 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 00:00:00.0, $738.17
16:58:58,124 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 03:00:00.0, $330.85
16:58:58,124 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 00:00:00.0, $726.23
16:58:58,124 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 03:00:00.0, $730.02
16:58:58,124 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 3, 2019-01-01 00:00:00.0, $432.90
16:58:58,124 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 03:00:00.0, $523.54
16:58:58,124 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 04:00:00.0, $543.50
16:58:58,129 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 02:00:00.0, $859.35
16:58:58,130 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 01:00:00.0, $254.04
16:58:58,130 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 4, 2019-01-01 02:00:00.0, $415.08
16:58:58,130 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 00:00:00.0, $482.29
16:58:58,130 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 01:00:00.0, $762.27
16:58:58,130 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 02:00:00.0, $206.98
16:58:58,130 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 04:00:00.0, $374.75
16:58:58,135 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 01:00:00.0, $686.87
16:58:58,136 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 1, 2019-01-01 04:00:00.0, $585.16
16:58:58,136 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 01:00:00.0, $810.06
16:58:58,137 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 2, 2019-01-01 02:00:00.0, $458.40
16:58:58,137 INFO  org.apache.flink.walkthrough.common.sink.LoggerOutputFormat   - 5, 2019-01-01 03:00:00.0, $605.53

Process finished with exit code 0

```


你的应用将会使用基于时间戳字段的一小时的滚动窗口。 因此时间戳是 2019-06-01 01:23:47 的行被放入 2019-06-01 01:00:00 这个时间窗口之中。

在持续的流式应用中，基于时间的聚合结果是唯一的，因为相较于其他属性，时间通常会向前移动。 在批处理环境中，窗口提供了一个方便的 API，用于按时间戳属性对记录进行分组。

运行这个更新过的查询将会得到和之前一样的结果。


### 通过流处理的方式再来一次！
因为 Flink 的 Table API 为批处理和流处理提供了相同的语法和语义，从一种方式迁移到另一种方式只需要两步。

第一步是把批处理的 ExecutionEnvironment 替换成流处理对应的 StreamExecutionEnvironment，后者创建连续的流作业。 它包含特定于流处理的配置，比如时间特性。当这个属性被设置成 事件时间时，它能保证即使遭遇乱序事件或者作业失败的情况也能输出一致的结果。 滚动窗口在对数据进行分组时就运用了这个特性。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
```

第二步就是把有界的数据源替换成无限的数据源。 这个项目通过 UnboundedTransactionTableSource 持续不断地实时生成交易事件。 与 BoundedTransactionTableSource 一样，这个表也是通过在内存中生成数据从而不依赖外部系统。 在实践中，这个表可能从一个流式数据源中读取数据，比如 Apache Kafka、AWS Kinesis 或者 Pravega。
```java
tEnv.registerTableSource("transactions", new UnboundedTransactionTableSource());
```

这就是一个功能齐全、有状态的分布式流式应用！ 这个查询会持续处理交易流，计算每小时的消费额，然后实时输出结果。 由于输入是无界的，因此查询将一直运行，直到手动停止为止。 因为这个作业使用了基于时间窗口的聚合，Flink 可以使用一些特定的优化，比如当系统知道一个特定的窗口不会再有新的数据到来，它就会对状态进行清理。



###  TODO
没有搞清楚数据是怎么来的。就是代码没有看明白。 在frauddetection项目中，可以找到数据的来源，但是在这里，没有找到。