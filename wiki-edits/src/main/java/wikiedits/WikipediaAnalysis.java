package wikiedits;

/**
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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * Skeleton for a Flink Streaming Job.
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/wiki-edits-0.1.jar
 * From the CLI you can then run
 * 		./bin/flink run -c wikiedits.WikipediaAnalysis target/wiki-edits-0.1.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class WikipediaAnalysis {

	public static void main(String[] args) throws Exception {
		// 环境信息 set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/**
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */


		env.addSource(new WikipediaEditsSource())
				//以用户名为key分组
				.keyBy((KeySelector<WikipediaEditEvent, String>) wikipediaEditEvent -> wikipediaEditEvent.getUser())
				//时间窗口为5秒
				.timeWindow(Time.seconds(15))
				//在时间窗口内按照key将所有数据做聚合
				.aggregate(new AggregateFunction<WikipediaEditEvent, Tuple3<String, Integer, StringBuilder>, Tuple3<String, Integer, StringBuilder>>() {
					@Override
					public Tuple3<String, Integer, StringBuilder> createAccumulator() {
						//创建ACC
						return new Tuple3<>("", 0, new StringBuilder());
					}

					@Override
					public Tuple3<String, Integer, StringBuilder> add(WikipediaEditEvent wikipediaEditEvent, Tuple3<String, Integer, StringBuilder> tuple3) {

						StringBuilder sbud = tuple3.f2;

						//如果是第一条记录，就加个"Details ："作为前缀，
						//如果不是第一条记录，就用空格作为分隔符
						if(StringUtils.isBlank(sbud.toString())){
							sbud.append("Details : ");
						}else {
							sbud.append(" ");
						}

						//聚合逻辑是将改动的字节数累加
						return new Tuple3<>(wikipediaEditEvent.getUser(),
								wikipediaEditEvent.getByteDiff() + tuple3.f1,
								sbud.append(wikipediaEditEvent.getByteDiff()));
					}

					@Override
					public Tuple3<String, Integer, StringBuilder> getResult(Tuple3<String, Integer, StringBuilder> tuple3) {
						return tuple3;
					}

					@Override
					public Tuple3<String, Integer, StringBuilder> merge(Tuple3<String, Integer, StringBuilder> tuple3, Tuple3<String, Integer, StringBuilder> acc1) {
						//合并窗口的场景才会用到
						return new Tuple3<>(tuple3.f0,
								tuple3.f1 + acc1.f1, tuple3.f2.append(acc1.f2));
					}
				})
				//聚合操作后，将每个key的聚合结果单独转为字符串
				.map((MapFunction<Tuple3<String, Integer, StringBuilder>, String>) tuple3 -> tuple3.toString())
				//输出方式是STDOUT
				.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
