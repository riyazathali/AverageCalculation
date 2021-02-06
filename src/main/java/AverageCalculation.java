import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageCalculation
{
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {
        private static final IntWritable num = new IntWritable();
        private Text NumberKey = new Text();


        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                this.NumberKey.set("");
                num.set(Integer.parseInt(itr.nextToken()));
                context.write(this.NumberKey, num);
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, IntWritable, Text, FloatWritable> {
        private FloatWritable averageResult = new FloatWritable();
        Float numberAverage = Float.valueOf(0.0F);
        Float counofNumbers = Float.valueOf(0.0F);
        int sumValue = 0;




        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            Text averageSalutation = new Text("Final Average of all the numbers = ");
            for (IntWritable val : values) {
                this.sumValue += val.get();
                this.counofNumbers = Float.valueOf(this.counofNumbers.floatValue() + 1.0F);
            }
            this.numberAverage = Float.valueOf(this.sumValue / this.counofNumbers.floatValue());
            this.averageResult.set(this.numberAverage.floatValue());
            context.write(averageSalutation, this.averageResult);
        }
    }




    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Calculation");
        job.setJarByClass(AverageCalculation.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
