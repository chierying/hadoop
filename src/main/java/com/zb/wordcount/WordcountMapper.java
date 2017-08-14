package com.zb.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 一个单词次数统计的MapReduce程序。
 * 泛型的含义如下：
 *
 * KeyIn: 默认情况下是mr框架所读到的第一行文本的偏移量， Long.
 * 但是在hadoop中有自己更精简的序列化接口， 所以不用Long， 而用LongWritable
 * ValueIn: 默认情况是时mr框架读到的一行文本内容， String 同上用Text
 *
 * KeyOut: 是用户自定义逻辑处理完之后输出数据中的Key, 在此处是单词， String
 * ValueOut： 是用户自定义逻辑处理完之后输出数据中的， 在此处是单次次数， Integer
 *
 * Created by zb on 2017/7/13.
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * map阶段的业务逻辑就写在自定义的map()中
     * mapTask会对每一行输入数据调用一次我们的map()
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将mapTask传给我们的的文本内容先转换成String
        String line = value.toString();
        // 根据空格切分单词
        String[] words = line.split(" ");

        // 将单词输出为<单词, 1>
        for (String word : words) {
            // 把单词作为key 次数作为value 分发给reduce, 相同的key会给到同一个reduceTask
            context.write(new Text(word), new IntWritable(1));
        }

    }
}
