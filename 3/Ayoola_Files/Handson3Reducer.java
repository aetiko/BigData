import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Handson3Reducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String maxPopulationCityName = "";
        String minPopulationCityName = "";
        int maxValue = Integer.MIN_VALUE;
        int minValue = Integer.MAX_VALUE;
        for (Text value : values) {
            String[] array = value.toString().split(",");
            int population = Integer.valueOf(array[1]);
            if (population > maxValue) {
                maxPopulationCityName = array[0];
                maxValue = population;
            }
            if (population < minValue) {
                minPopulationCityName = array[0];
                minValue = population;
            }

        }
        context.write(new Text(key + " " + minPopulationCityName), new IntWritable(minValue));
        context.write(new Text(key + " " + maxPopulationCityName), new IntWritable(maxValue));
    }

}