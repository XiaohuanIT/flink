package streaming.flink_sql;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @Author: xiaohuan
 * @Date: 2020/6/1 21:00
 */
public class FlinkSqlDemo1 {
	public static final String PATH = "/Users/yangxiaohuan/Downloads/testdata.csv";
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
		DataSet<Tuple10<Integer,String,String,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> csvInput = env.readCsvFile(PATH)
				.ignoreFirstLine()
				.types(Integer.class,String.class,String.class,Integer.class,Integer.class,Integer.class,Integer.class,Integer.class,Integer.class,Integer.class);
		Table topScore = tableEnv.fromDataSet(csvInput);
		tableEnv.registerTable("topScore",topScore);
		Table t = tableEnv.sqlQuery("select * from topScore");
		TypeInformation<Tuple10<Integer,String,String,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> info = TypeInformation.of(new TypeHint<Tuple10<Integer,String,String,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>(){});
		DataSet<Tuple10<Integer,String,String,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> dst10 = tableEnv.toDataSet(t,info);
		dst10.collect().forEach(it->System.out.println(it));
	}
}
