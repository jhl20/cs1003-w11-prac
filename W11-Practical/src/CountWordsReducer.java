
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CountWordsReducer.java.
 */
public class CountWordsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	/**
	 * Combines same words and maps the unique words to the total counts.
	 * @param key                       this is a word
	 * @param values                    this are all the counts associated with the word
	 * @param output                    this is a map from unique words to the total counts
	 * @throws IOException              throws possible IOException
	 * @throws InterruptedException     throws possible InterruptedException
	 */
	public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {

		int sum = 0;
		for (LongWritable value : values) {
			long l = value.get();
			sum += l;
		}
		output.write(key, new LongWritable(sum));
	}
}
