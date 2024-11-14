//package com.missiongroup.mitmicrogridflink.job.job2;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Properties;
//
//public class ReadFromsinkKafka {
//    public static void main(String[] args) throws Exception {
//        // create execution environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "192.168.11.12:9092");
//        properties.setProperty("group.id", "flinksink");
//        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer("flinktest", new SimpleStringSchema(), properties));
//        stream.map(new MapFunction<String, String>() {
//            private static final long serialVersionUID = -6867736771747690202L;
//
//            @Override
//            public String map(String value) throws Exception {
//                System.out.println(value);
//                return value;
//            }
//        }).print();
//        env.execute();
//    }
//}