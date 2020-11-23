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
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;

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
		final Configuration conf = new Configuration();
		conf.setInteger(RestOptions.PORT, 8081);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		// terminates
		env.setParallelism(4);
		// does not terminate
		// env.setParallelism(6);
		final Configuration configuration = new Configuration();
		configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.valueOf(parameterTool.get("mode", "BATCH")));
		env.configure(configuration, StreamingJob.class.getClassLoader());

		final String inputFilename = parameterTool.get("input", "test-input/input");
		final String outputFilename = parameterTool.get("output", "test-output");
		final FileSource<Event> source = FileSource
				.forRecordStreamFormat(new EventStreamFormat(), new Path(inputFilename))
				.build();

		final DataStream<Event> input = env.fromSource(
				source,
				WatermarkStrategy
					.<Event>forMonotonousTimestamps()
					.withTimestampAssigner((event, timestamp) -> event.getTimestamp()),
				"foobar");
//		final DataStream<String> input = env.fromElements("ab c", "d e f", "foobar barfoo");

		final DataStream<String> words = input.flatMap((FlatMapFunction<Event, String>) (s, collector) -> {
			for (String split : s.getValue().split(" ")) {
				collector.collect(split.trim());
			}
		}).returns(String.class);

		final SingleOutputStreamOperator<DataObject> process = words.rebalance().keyBy(x -> x).process(new KeyedProcessFunction<String, String, DataObject>() {
			@Override
			public void processElement(String value, Context ctx, Collector<DataObject> out) throws Exception {
				ctx.timerService().registerProcessingTimeTimer(ctx.timestamp() + 10);
				out.collect(new DataObject(
						value,
						ctx.timerService().currentWatermark(),
						ctx.timerService().currentProcessingTime()));
			}

			@Override
			public void onTimer(long timestamp, OnTimerContext ctx, Collector<DataObject> out) throws Exception {
				out.collect(new DataObject("timer", timestamp, timestamp));
			}
		});

		final DataStreamSource<Integer> rules = env.fromElements(1, 2, 3, 4, 5, 6);
		final MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<String, Integer>(
				"mapStateDescriptor",
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO);

		final BroadcastStream<Integer> broadcastStream = rules.broadcast(mapStateDescriptor);

		final SingleOutputStreamOperator<DataObject> broadcastOutput = process.connect(broadcastStream).process(new BroadcastProcessFunction<DataObject, Integer, DataObject>() {
			@Override
			public void processElement(DataObject value, ReadOnlyContext ctx, Collector<DataObject> out) throws Exception {
				out.collect(value);
			}

			@Override
			public void processBroadcastElement(Integer value, Context ctx, Collector<DataObject> out) throws Exception {
				out.collect(new DataObject("broadcast", value, value));
			}
		});


		final FileSink<DataObject> sink = FileSink.<DataObject>forRowFormat(new Path(outputFilename), new SimpleStringEncoder<>())
				.withBucketAssigner(new DateTimeBucketAssigner<>())
				.build();

		broadcastOutput.sinkTo(sink);

		// execute program
		env.execute("Flink Batch streaming job");

		System.out.println("foobar");
	}

	public static final class EventStreamFormat extends SimpleStreamFormat<Event> {
		@Override
		public Reader<Event> createReader(Configuration config, FSDataInputStream stream) throws IOException {
			return new EventReader(new BufferedReader(new InputStreamReader(stream)));
		}

		@Override
		public TypeInformation<Event> getProducedType() {
			return TypeInformation.of(new TypeHint<Event>() {});
		}
	}

	public static final class EventReader implements StreamFormat.Reader<Event> {
		private final BufferedReader reader;

		public EventReader(BufferedReader reader) {
			this.reader = reader;
		}

		@Override
		public Event read() throws IOException {
			final String line = reader.readLine();

			if (line != null) {
				final String[] split = line.split(";");

				return new Event(Long.parseLong(split[0]), split[1]);
			} else {
				return null;
			}
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}
	}

	public static final class Event {
		private final long timestamp;
		private final String value;

		public Event(long timestamp, String value) {
			this.timestamp = timestamp;
			this.value = value;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public String getValue() {
			return value;
		}
	}

	public static final class DataObject {
		private final String value;
		private final long watermark;
		private final long processingTime;

		public DataObject(String value, long watermark, long processingTime) {
			this.value = value;
			this.watermark = watermark;
			this.processingTime = processingTime;
		}

		public String getValue() {
			return value;
		}

		public long getWatermark() {
			return watermark;
		}

		public long getProcessingTime() {
			return processingTime;
		}

		@Override
		public String toString() {
			return "DataObject{" +
					"value='" + value + '\'' +
					", watermark=" + watermark +
					", processingTime=" + processingTime +
					'}';
		}
	}
}
