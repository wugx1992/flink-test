package indi.gxwu.flink.somcase;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: gx.wu
 * @Date: 2020/11/30 10:29
 * @Description: code something to describe this module what it is
 * 需求：实时的wordcount
 * 往端口中发送数据，实时的计算数据
 */
public class SocketWordCount {
    public static void main(String[] args) throws Exception {
        //1.定义连接端口
        final int port = 9999;
        //2.创建执行环境对象
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //3.得到套接字对象(指定：主机、端口、分隔符)
        DataStreamSource<String> text = env.socketTextStream("192.168.1.238", port, "\n");

        //4.解析数据，统计数据-单词计数 hello lz hello world
        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> collector){
                //按照空白符进行切割
                for (String word : s.split("\\s")) {
                    //<单词，1>
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        })
                //按照key进行分组
                .keyBy("word")
                //设置窗口的时间长度 5秒一次窗口 1秒计算一次
                .timeWindow(Time.seconds(5), Time.seconds(1))
                //聚合，聚合函数
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        //按照key聚合
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        //5.打印可以设置并发度
        windowCounts.print().setParallelism(1);

        //6.执行程序
        env.execute("Socket window WordCount");
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {

        }

        public WordWithCount(String word, long count){
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString(){
            return word + " : " + count;
        }
    }
}
