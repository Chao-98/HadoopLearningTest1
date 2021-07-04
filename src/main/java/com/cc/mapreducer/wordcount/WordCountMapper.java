package com.cc.mapreducer.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN,  map阶段输入量的类型
 * VALUEIN,map阶段输入的value类型
 * KEYOUT,map阶段输出key的类型
 * VALUEOUT,map阶段输出的value类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(" ");

        for (String word : words) {
            outK.set(word);
            context.write(outK,outV);
        }

    }
}
