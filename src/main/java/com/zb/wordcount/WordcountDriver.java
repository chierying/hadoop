package com.zb.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于一个yarn集群的客户端， 要再次封装我们map reduce程序的运行参数，指定jar包，最后提交给yarn
 *
 * Created by zb on 2017/7/14.
 */
public class WordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // 本地调试MapReduce
        conf.set("mrjoin.framework.name","local");
        conf.set("fs.defaultFS","file:///");

        Job job = Job.getInstance(conf);

        // 指定本程序的jar包所在本地路径
        job.setJarByClass(WordcountDriver.class);

        // 指定本业务用的mapper reducer 类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 指定job的输入源文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // job.submit();
        boolean result = job.waitForCompletion(true);

        // 根据处理结果给程序设定退出码
        System.exit(result ? 0 : 1);

    }
}
