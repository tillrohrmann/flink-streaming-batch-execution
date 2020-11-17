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

package org.stsffap;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final Configuration configuration = new Configuration();
		configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.valueOf(parameterTool.get("mode", "BATCH")));
		env.configure(configuration, StreamingJob.class.getClassLoader());

		env.setParallelism(10);

		final String inputFilename = parameterTool.get("input", "test-input/input");
		final String outputFilename = parameterTool.get("output", "test-output");
		final FileSource<String> source = FileSource
				.forRecordStreamFormat(new TextLineFormat(), new Path(inputFilename))
				.build();

		final DataStream<String> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "foobar");

		final DataStream<String> words = input.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
			for (String split : s.split(" ")) {
				collector.collect(split.trim());
			}
		}).returns(String.class);

		final DataStream<Tuple2<String, Integer>> wordsWithSingleCount = words.rescale().map(i -> i).setParallelism(20)
				.map((MapFunction<String, Tuple2<String, Integer>>) word -> Tuple2.of(word, 1))
				.returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

		final SingleOutputStreamOperator<Tuple2<String, Integer>> wordsWithCounts = wordsWithSingleCount
				.keyBy(t -> t.f0)
				.reduce((ReduceFunction<Tuple2<String, Integer>>) (stringIntegerPair, t1) -> Tuple2.of(stringIntegerPair.f0, stringIntegerPair.f1 + t1.f1));

		final FileSink<Tuple2<String, Integer>> sink = FileSink.<Tuple2<String, Integer>>forRowFormat(new Path(outputFilename), new SimpleStringEncoder<>())
				.build();

		wordsWithCounts.sinkTo(sink);

		// execute program
		env.execute("Flink Batch streaming job");
	}
}
