package com.cc.mapreducer.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 1.获取job
 * 2.设置jar包
 * 3.关联mapper和reducer
 * 4.设置mapper输出的KV类型
 * 5.设置最终输出的KV类型
 * 6.设置数据的输入和输出路径
 * 7.提交job
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance();

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\input\\inputflow"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output3"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
