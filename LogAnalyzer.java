import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class LogAnalyzer {
	
	public static class LogAnalyzerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text logType = new Text();

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        if (line.contains("wallet-rest-api")) {
	            if (line.contains("[INFO]")) {
	                logType.set("[INFO]");
	                context.write(logType, one);
	            } else if (line.contains("[SEVERE]")) {
	                logType.set("[SEVERE]");
	                context.write(logType, one);
	            } else if (line.contains("[WARN]")) {
	                logType.set("[WARN]");
	                context.write(logType, one);
	            }
	        }
	    }
	}
	
	public static class LogAnalyzerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        result.set(sum);
	        context.write(key, result);
	    }
	}
	
}

