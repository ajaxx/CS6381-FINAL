package edu.vanderbilt.cs6381;

import java.util.regex.Pattern;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OHLCAnalyzer {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create the deserialization schema
        final KafkaRecordDeserializationSchema<ObjectNode> deserializationSchema = KafkaRecordDeserializationSchema
                .of(new JSONKeyValueDeserializationSchema(false));

        // create the consumer - this is where the events will come from and
        // will nominally be injected into the stream
        KafkaSource<ObjectNode> kafkaSource = KafkaSource.<ObjectNode>builder()
                .setBootstrapServers("localhost:9202")
                .setTopicPattern(Pattern.compile("OHLC\\..*"))
                .setGroupId("data-analyzer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(deserializationSchema)
                .build();

        DataStreamSource<ObjectNode> dataSource = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "OHLC Source");

        dataSource.addSink(new PrintSinkFunction<>());

        env.execute();
    }
}
