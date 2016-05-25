import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class InOutDegrees {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
	  ArrayList<String> table = new ArrayList<String>();
	  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

      String line = value.toString();
      String[] lineNo=line.split("\n");
      String [] str = (line.split(" "));
      
      String keystring=str[0];
      String valstring=str[1];
      String s = keystring+valstring;
      String r = valstring+keystring+"*";
      
      if(!table.contains(s) && !table.contains(r))
      {
    	  
         table.add(s);
         table.add(r);
  
         String a1 = s.substring(0, 3);
         String b1 = s.substring(3, 6);
         
         String a11=r.substring(0,3);
         String b11=r.substring(3,7);
         
         Text text = new Text(a1);
         Text text1 = new Text(b1);
         Text text2=new Text(a11);
         Text text3=new Text(b11);
         
         for(int i=0;i<lineNo.length;i++)
         {
           output.collect(text, text1);
           output.collect(text2, text3);
         }  
      }
      
    }
  }
  
  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

      int sum = 0;
      int count=0;
      
      while (values.hasNext()) {
    	  
    	 Text store = new Text (values.next());
    	 String storetext = store.toString();
    	 if(storetext.contains("*"))
    	 {
    		 count++;
    	 }
    	 else 
        sum++;
      }
      String abc = sum + " " + count;
      Text st=new Text(abc);
      output.collect(key,st);
      
    }
  }
    
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(InOutDegrees.class);
    conf.setJarByClass(InOutDegrees.class);
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
