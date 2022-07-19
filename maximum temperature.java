import java.util.*;
import java.io.IOException;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class MaxMonTem {
 //Mapper class
 public static class MM_TMapper extends MapReduceBase implements
 Mapper<LongWritable, /*Input key*/
 Text,
 Text,
 IntWritable> {
 //Map function
 public void map (LongWritable key, Text value,
 OutputCollector<Text, IntWritable>output,
 Reporter reporter) throws IOException {

 String line = value.toString();
 StringTokenizer st = new StringTokenizer(line,")(");
 while(st.hasMoreTokens()){
 String token = st.nextToken();
 if(token.length() > 2){
 StringTokenizer sst = new StringTokenizer(token,",");
 String year = sst.nextToken().substring(0,4);
 int tem = Integer.parseInt(sst.nextToken());
 output.collect(new Text(year), new IntWritable(tem));
 }
 }
 }
 }
  
 public static class MM_TReduce extends MapReduceBase implements
 Reducer<Text, IntWritable, Text, IntWritable> {
 //Reduce function
 public void reduce(Text key, Iterator <IntWritable> values,
 OutputCollector<Text,IntWritable> output, Reporter
reporter) throws IOException {
 int max = values.next().get();
 while(values.hasNext()){
 int tem = values.next().get();
 if(max < tem){
 max = tem;
 }
 }
 output.collect(key, new IntWritable(max));
 }
 }
 public static void main(String[] args) throws Exception{
 JobConf conf = new JobConf(MaxMonTem.class);
 conf.setJobName("maximumMonthly_temperature");
 conf.setOutputKeyClass(Text.class);
 conf.setOutputValueClass(IntWritable.class);
 conf.setMapperClass(MM_TMapper.class);
 conf.setCombinerClass(MM_TReduce.class);
 conf.setReducerClass(MM_TReduce.class);
 conf.setInputFormat(TextInputFormat.class);
 conf.setOutputFormat(TextOutputFormat.class);
 FileInputFormat.setInputPaths(conf, new Path(args[0]));
 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 JobClient.runJob(conf);
 }
}
