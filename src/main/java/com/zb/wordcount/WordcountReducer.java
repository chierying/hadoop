package com.zb.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 单词统计的ReducerTask，泛型参数如下：
 *
 * KeyIn ValueIn 对应Mapper输出的KeyOut ValueOut
 *
 * KeyOut ValueOut 是自定义Reducer逻辑处理结构的输出
 * KeyOut是单词 ValueOut是次数
 *
 * Created by zb on 2017/7/13.
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * @param key     是一组单词相同的KV对的key
     * @param values  是值得集合
     * @param context 上下文
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for (IntWritable value : values) {
            count++;
        }

        context.write(key,new IntWritable(count));

    }
}
