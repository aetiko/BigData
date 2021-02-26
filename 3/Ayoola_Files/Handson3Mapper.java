import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Handson3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int MISSING = 9999;

    @Override

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        int populationVal;
        String line = value.toString();
        String field[] = line.split(",");
        String country = field[4].substring(1, field[4].length() - 1);
//        String newString = country.concat(field[0].substring(1, field[0].length() - 1));

        String population = field[9].substring(1, field[9].length() - 1);
        String city = field[0].substring(1, field[0].length() - 1);


        if (!population.matches(".*\\d.*") || population.equals("") ||
                population.matches("([0-9].*)\\.([0-9].*)")) {
            return;
        } else {
            populationVal = Integer.parseInt(population);
            context.write(new Text(country), new Text(city + "," + populationVal));
        }
    }

}