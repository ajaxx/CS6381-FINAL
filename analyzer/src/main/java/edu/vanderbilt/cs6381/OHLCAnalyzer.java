package edu.vanderbilt.cs6381;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class OHLCAnalyzer {
    private static final String CONST_ASCII_UPPERCASE = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static OHLC convertToOHLC(final ObjectNode objectNode) {
        try {
            System.out.println("convertToOHLC: " + objectNode.toString());
            System.out.println();
            final Wrapper wrapper = objectMapper.treeToValue(objectNode, Wrapper.class);
            final OHLC returnValue = wrapper.getValue();
            System.out.println("convertToOHLC: " + returnValue);
            return returnValue;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        // return null null on failure
        return null;
    }

    public static Tuple2<OHLC, Latency> calculateScalarLatency(
            final OHLC ohlc)
    {
        final Instant current = Instant.now();
        final Duration duration = Duration.between(Instant.EPOCH, current);
        final long microTime = duration.getNano() / 1000L + duration.getSeconds() * 1000000L;
        final Latency latency = new Latency(ohlc.getMtime(), microTime);
        return new Tuple2<>(ohlc, latency);
    }

    public static void calculateStatisticsForOHLC(
            final String symbol,
            final TimeWindow window,
            final Iterable<OHLC> values,
            final Collector<OHLCAverage> out)
        throws Exception
    {
        double open = 0.0;
        double high = 0.0;
        double low = 0.0;
        double close = 0.0;
        double vwap = 0.0;
        long tradeCount = 0;

        int count = 0;
        for (OHLC value : values) {
            open += value.getOpen();
            high += value.getHigh();
            low += value.getLow();
            close += value.getClose();
            vwap += value.getVwap();
            tradeCount += value.getTradeCount();
            count ++;
        }

        //OHLCAverage result = values.iterator().next();
        OHLCAverage result = new OHLCAverage();
        result.setSymbol(symbol);
        result.setOpen(open / count);
        result.setHigh(high / count);
        result.setLow(low / count);
        result.setClose(close / count);
        result.setVwap(vwap / count);
        result.setTradeCount(tradeCount);
        out.collect(result);
    }

    private static void calculateAverageLatency(
            final String symbol,
            final TimeWindow window,
            final Iterable<Tuple2<OHLC, Latency>> values,
            final Collector<Tuple2<String, Long>> out)
    {
        long latency = 0L;
        int count = 0;
        for (Tuple2<OHLC, Latency> value : values) {
            latency += value.f1.getLatency();
            count++;
        }

        out.collect(new Tuple2<>(symbol, latency / count));
    }

    private static void buildLatencyStatistics(
            final SingleOutputStreamOperator<OHLC> dataSource,
            final String pipelinePath)
    {
        String latencyPath = pipelinePath + "/latency";
        String latencyRawPath = pipelinePath + "/latency-raw";

        // Create a stream that injects latency into each element
        SingleOutputStreamOperator<Tuple2<OHLC, Latency>> latencySource = dataSource
                .map(OHLCAnalyzer::calculateScalarLatency);

        // Write the raw latency numbers out
        StreamingFileSink<Tuple2<OHLC, Latency>> latencySink = StreamingFileSink
                .forRowFormat(new Path(latencyRawPath), new SimpleStringEncoder<Tuple2<OHLC, Latency>>())
                .build();

        latencySource
                .addSink(latencySink)
                .name("sink-latency");

        // Create some aggregate latency statistics
        latencySource
                .keyBy(tuple -> tuple.f0.getSymbol())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .apply(OHLCAnalyzer::calculateAverageLatency)
                .name("average(latency)")
                .map(tuple -> tuple.f0 + "," + tuple.f1)
                .name("csv(average(latency))")
                .addSink(StreamingFileSink
                        .forRowFormat(new Path(latencyPath), new SimpleStringEncoder<String>())
                        .build());
    }

    private static void buildInfoStatistics(
            final SingleOutputStreamOperator<OHLC> dataSource,
            final String pipelinePath)
    {
        String averagePath = pipelinePath + "/ohlc-average";

        // Map the object node to the OHLC type
        KeyedStream<OHLC, String> dataSourceBySymbol = dataSource
                .keyBy(ohlc -> ohlc.getSymbol());

        // we would like to transform this data, but in what ways would this be useful?
        // - we can create the moving average, macd, aroons, but they are based on a single value
        // - we could also calculate the end to end latency of the message

        SingleOutputStreamOperator<String> statisticsSource = dataSourceBySymbol
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                //.window(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(OHLCAnalyzer::calculateStatisticsForOHLC)
                .name("average(OHLC)")
                .map(OHLCAverage::toCsvString)
                .name("csv(average(OHLC))");

        statisticsSource
                .addSink(StreamingFileSink
                        .forRowFormat(new Path(averagePath), new SimpleStringEncoder<String>())
                        .build())
                .name("stream-results");
    }

    private static SingleOutputStreamOperator<OHLC> buildPipeline(
            final StreamExecutionEnvironment env,
            final String bootstrapServer,
            final String rootPath,
            final Pattern topicPattern,
            final List<String> topics) throws IOException {

        // create the deserialization schema
        final KafkaRecordDeserializationSchema<ObjectNode> deserializationSchema = KafkaRecordDeserializationSchema
                .of(new JSONKeyValueDeserializationSchema(false));

        // create the consumer - this is where the events will come from and
        // will nominally be injected into the stream
        KafkaSourceBuilder<ObjectNode> kafkaSourceBuilder = KafkaSource.<ObjectNode>builder()
                .setBootstrapServers(bootstrapServer)
                .setGroupId("data-analyzer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(deserializationSchema);

        String pipelinePath = rootPath;
        if (topicPattern != null) {
            kafkaSourceBuilder.setTopicPattern(topicPattern);
        } else if (topics != null) {
            kafkaSourceBuilder.setTopics(topics);
        }

        KafkaSource<ObjectNode> kafkaSource = kafkaSourceBuilder.build();

        WatermarkStrategy<OHLC> watermarkStrategy = WatermarkStrategy
                .<OHLC>noWatermarks()
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime());
//                .<OHLC>forMonotonousTimestamps()
//                .<OHLC>forBoundedOutOfOrderness(Duration.ofSeconds(1))


        SingleOutputStreamOperator<OHLC> rootDataSource = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "OHLC Source")
                .map(OHLCAnalyzer::convertToOHLC)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .filter(ohlc -> !Objects.isNull(ohlc))
                .name("filter-null(OHLC)");

        buildLatencyStatistics(rootDataSource, pipelinePath);
        buildInfoStatistics(rootDataSource, pipelinePath);

        return rootDataSource;
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // See if a bootstrap server has been provided through command
        // line arguments
        String bootstrapServer = parameterTool.get("bootstrap-server");
        if (bootstrapServer == null) {
            bootstrapServer = "localhost:9092";
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final String rootPath = "/tmp/cs6381";

        final String alphaSplit = parameterTool.get("alpha");
        System.out.println("alphaSplit = " + alphaSplit);
        if (alphaSplit != null) {
            final String[] alphaSplitPartitions = {
                    "ABCDEF",
                    "GHIJKL",
                    "MNOPQR",
                    "STUVWX",
                    "YZ"
            };

            int instance = 0;
            for (String alphaPartition : alphaSplitPartitions) {
                instance++;
                List<String> topics = alphaPartition
                        .chars()
                        .mapToObj(c -> (char) c)
                        .map(v -> "OHLC." + v)
                        .collect(Collectors.toList());

                java.nio.file.Path alphaPath = Paths.get(rootPath, String.valueOf(instance));
                if (Files.notExists(alphaPath)) {
                    Files.createDirectory(alphaPath);
                }

                buildPipeline(env, bootstrapServer, alphaPath.toString(), null, topics);
            }
        } else {
            buildPipeline(env, bootstrapServer, rootPath, Pattern.compile("OHLC.*"), null);
        }

        env.execute();
    }
}
