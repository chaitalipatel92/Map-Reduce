import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class IncomingEdges {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
	  ArrayList<String> table = new ArrayList<String>();
	  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

      String line = value.toString();
      String[] lineNo=line.split("\n");
      String [] str = (line.split(" "));
      
      String keystring=str[0];
      String valstring=str[1];
      String s = valstring+keystring;
      
      if(!table.contains(s))
      {
    	  
         table.add(s);
         String a1 = s.substring(0, 3);
         String b1 = s.substring(3, 6);
         
         Text text = new Text(a1);
         Text text1 = new Text(b1);
         
         for(int i=0;i<lineNo.length;i++)
         {
           output.collect(text, text1);
         }  
      }
     }
  }


  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    	String st1 = "";
        while (values.hasNext()) {
            Text t =values.next();
            st1 = st1 + " " + t.toString(); 
            }
        	st1 ="[" + st1 + " ]";
            Text st=new Text(st1);
            output.collect(key, st);
      }
    }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(IncomingEdges.class);
    conf.setJarByClass(IncomingEdges.class);
    conf.setJobName("wordcount");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);

  }
}