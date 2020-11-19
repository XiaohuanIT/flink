package streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * 统计一个文件中单词出现的总次数，并且把结果存储到文件中。
 * @Author: xiaohuan
 * @Date: 2020/5/31 16:48
 */
public class BatchWordCountJava {
	public static void main(String[] args) throws Exception {
		String inputFilePath = "/Users/yangxiaohuan/Downloads/temp.txt";
		String outputFilePath = "/Users/yangxiaohuan/Downloads/flink_exercise1.txt";

		// 获取运行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// 获取文件中的内容
		DataSource<String> text = env.readTextFile(inputFilePath);
		// 3.flatMap将数据转成大写并以空格进行分割
		// groupBy归纳相同的key，sum将value相加
		DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())
				.groupBy(0)
				.sum(1);
		counts.writeAsCsv(outputFilePath, "\n", " ").setParallelism(1);
		env.execute("batch word count");
	}

	public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>>{

		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			String[] tokens = value.toLowerCase().split("\\W+");
			for(String token : tokens){
				if(token.length()>0){
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
