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

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * An example of grouped stream windowing into sliding time windows.
 * This example uses [[RichParallelSourceFunction]] to generate a list of key-value pairs.
 */
public class DataStreamAPIExample {

	private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
			Random random = new Random(System.currentTimeMillis());
			while (running) {
				Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 + 500);
				String key = "类别" + (char) ('A' + random.nextInt(3));
				int value = random.nextInt(10) + 1;
				System.out.println(String.format("Emit:\t(%s, %d)", key, value));
				ctx.collect(new Tuple2<>(key, value));
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<Tuple2<String, Integer>> ds = env.addSource(new DataSource());

		KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);
		keyedStream.sum(1)
//			.addSink(new SinkFunction<Tuple2<String, Integer>>() {
//				@Override
//				public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
//					System.out.println(String.format("Sum: (%s, %d)", value.f0, value.f1));
//				}
//			});

			.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
				@Override
				public Object getKey(Tuple2<String, Integer> value) throws Exception {
					return "";
				}
			}).fold(
			new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, Map<String, Integer>>() {
				@Override
				public Map<String, Integer> fold(Map<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
					accumulator.put(value.f0, value.f1);
					return accumulator;
				}
			}
		).addSink(new SinkFunction<Map<String, Integer>>() {
			@Override
			public void invoke(Map<String, Integer> value, Context context) throws Exception {
				System.out.println(value.values().stream().mapToInt(v -> v).sum());
			}
		});

		ds.addSink(new SinkFunction<Tuple2<String, Integer>>() {
			@Override
			public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
				System.out.println(String.format("Get:\t(%s, %d)", value.f0, value.f1));
			}
		});

		env.execute();
	}
}
