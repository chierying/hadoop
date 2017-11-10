package com.zb.logenhance;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义输出格式
 * 如何输出由自己决定, 可以把数据写入到任何地方.
 */
public class CustomOutputFormat extends FileOutputFormat<Text, NullWritable> {

    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        // 从上下文中得到配置, 并打开hdfs的连接
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        FSDataOutputStream outputStream = fileSystem.create(new Path("path to hdfs dir"));

        return new LogRecordWriter(outputStream);
    }


    /**
     * 自己的RecordWriter
     */
    static class LogRecordWriter extends RecordWriter<Text, NullWritable> {
        // 把流作为构造传入
        private FSDataOutputStream outputStream;

        public LogRecordWriter(FSDataOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        /**
         * 每条reduce输出记录都会调用一次
         *
         * @param key   reduce输出的key
         * @param value reduce输出的value
         * @throws IOException
         * @throws InterruptedException
         */
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            outputStream.write(key.getBytes());
        }

        /**
         * 关闭流
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            outputStream.flush();
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }


}