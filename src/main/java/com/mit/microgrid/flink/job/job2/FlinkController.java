//package com.missiongroup.mitmicrogridflink.job.job2;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RequestMethod;
//import org.springframework.web.bind.annotation.RestController;
//
//import java.util.Properties;
//
//@RestController
//@RequestMapping("flink")
//public class FlinkController {
//    @RequestMapping(value = "/test", method = RequestMethod.POST)
//    public void test() throws Exception {
//        String[] jars = {"flink-service/target/flink-service-1.0-SNAPSHOT-kafka.jar", "flink-service/target/flink-service-1.0-SNAPSHOT.jar"};
//        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("hadoop01", 9065, 2, jars)) {
//            Properties properties = new Properties();
//            properties.setProperty("bootstrap.servers", "192.168.11.12:9092");
//            properties.setProperty("group.id", "flinksink");
//            DataStream<String> stream = env.addSource(new FlinkKafkaConsumer("flinktest", new SimpleStringSchema(), properties));
//            stream.map(new MapFunction<String, String>() {
//                private static final long serialVersionUID = -6867736771747690202L;
//
//                @Override
//                public String map(String value) throws Exception {
//                    System.out.println(value);
//                    return value;
//                }
//            }).print();
//            env.execute();
//        }
//    }
//}
