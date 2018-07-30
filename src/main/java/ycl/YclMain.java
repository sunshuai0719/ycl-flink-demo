package ycl;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.filter.CommonDataValidateFilter;
import com.run.ycl.map.CommonDataMappingMapper;
import com.run.ycl.map.CommonNormalizingMapper;
import com.run.ycl.utils.DvUtils;
import com.run.ycl.utils.ReflectionUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.junit.Test;

import java.lang.reflect.Method;

public class YclMain {

    @Test
    public void test() {
        Method method = ReflectionUtils.findMethod(DvUtils.class, "number", String.class);
        Object[] param1 = new Object[1];
        param1[0] = "14";
        System.out.println(ReflectionUtils.invokeMethod(method, new DvUtils(), param1));
    }
    public static void main(String[] args) {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer010 flinkKafkaConsumer = new FlinkKafkaConsumer010<String>(
                parameterTool.getRequired("input-topic"),
                new SimpleStringSchema(),
                parameterTool.getProperties());
        flinkKafkaConsumer.setStartFromEarliest();
        //flinkKafkaConsumer.setStartFromGroupOffsets();
        DataStream<String> input = env.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator<JSONObject> dataMapRes;
        if (true) {
            //格式转化
            //后期将参数转换从数据中指定的列获取
            dataMapRes = input.map(new CommonDataMappingMapper("WA_YCL_TEST_0001"));
        }

        if (true) {
            //数据校验
            dataMapRes = dataMapRes.filter(new CommonDataValidateFilter());
        }

        if (true) {
            //数据归一化
            dataMapRes = dataMapRes.map(new CommonNormalizingMapper("WA_YCL_TEST_0001"));
        }
/*
        input.addSink(
                new FlinkKafkaProducer010<String>(
                        parameterTool.getRequired("output-topic"),
                        new SimpleStringSchema(),
                        parameterTool.getProperties()));
*/
        input.print();
        try {
            env.execute("Kafka 0.10 Example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
