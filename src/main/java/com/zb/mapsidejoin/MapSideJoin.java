package com.zb.mapsidejoin;

import com.zb.mapsidejoin.po.Order;
import com.zb.mapsidejoin.po.Product;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by v_zhangbing on 2017/8/14.
 */
public class MapSideJoin {

    static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Map<String, Product> products = new HashMap<String, Product>();

        Pattern pattern = Pattern.compile("(\\d+) (\\d+) (\\d+)");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 从工作目录读取缓存文件，这个文件就在程序运行的当前目录。
            List<String> lines = FileUtils.readLines(new File("product.txt"));
            for (String line : lines) {
                if (StringUtils.isNotEmpty(line)) {
                    String[] split = line.split(" ");
                    Product product = new Product();
                    product.setId(split[0]);
                    product.setName(split[1]);

                    products.put(product.getId(), product);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Matcher matcher = pattern.matcher(value.toString());
            Order order = Order.newInstance(matcher.group(0), matcher.group(1), Integer.parseInt(matcher.group(2)), null);
            order.setProductName(products.get(order.getProductId()).getName());

        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapSideJoin.class);

        job.setMapperClass(MapSideJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoop\\mrjoin\\in\\order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\mrjoin\\out"));

        // 将产品列表文件缓存到task工作节点的工作目录中去
        job.addCacheFile(new URI("file://D:/hadoop/mrjoin/in/product.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
