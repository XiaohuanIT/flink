package spendreport;

import org.apache.flink.walkthrough.common.table.SpendReportTableSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.walkthrough.common.table.UnboundedTransactionTableSource;

/**
 * @Author: xiaohuan
 * @Date: 2020/6/2 17:01
 */
public class SpendReportStream {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		tEnv.registerTableSource("transactions", new UnboundedTransactionTableSource());
		tEnv.registerTableSink("spend_report", new SpendReportTableSink());

		tEnv
				.scan("transactions")
				.window(Tumble.over("1.hour").on("timestamp").as("w"))
				.groupBy("accountId, w")
				.select("accountId, w.start as timestamp, amount.sum")
				.insertInto("spend_report");

		env.execute("Spend Report");
	}
}
