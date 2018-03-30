/**
 * 2.3 Inverted Index + Stop & Scrub Words + Document Positions
 * Group number: cs132g2
 * Group name: Yet Another Mapreduce Program (YAMP)
 * Group members: Shuai Yu, Jinli Yu, Chuanxiong Yi, Minghui Zhu
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MergeReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> docIDs, Context ctx)
		throws IOException, InterruptedException {
		StringBuffer bff = new StringBuffer();
		for (Text id : docIDs) {
			if(bff.length() != 0) {
				bff.append(" ");
			}
			bff.append(id.toString());
		}
		Text idList = new Text();
		idList.set(bff.toString());
		ctx.write(key, idList);

//		List<Writable> lst = new ArrayList<Writable>();
//		for (Text id : docIDs) {
//			lst.add(id);
//		}
//		Writable[] wtbs = lst.toArray(new Writable[lst.size()]);
//		ArrayWritable aw = new ArrayWritable(Writable.class);
//		aw.set(wtbs);
//		ctx.write(key, aw);
	}
}