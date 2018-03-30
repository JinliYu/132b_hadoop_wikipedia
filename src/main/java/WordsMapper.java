/**
 * 2.3 Inverted Index + Stop & Scrub Words + Document Positions
 * Group number: cs132g2
 * Group name: Yet Another Mapreduce Program (YAMP)
 * Group members: Shuai Yu, Jinli Yu, Chuanxiong Yi, Minghui Zhu
 */
import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

public class WordsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String[] docline = value.toString().split(",");
		Text docID = new Text(docline[0]);
		Text emitValue = new Text();
		
//		Analyzer analyzer = new StandardAnalyzer();
		Analyzer analyzer = new EnglishAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		RAMDirectory ramDirectory = new RAMDirectory();

		Document doc = new Document();
		FieldType type = new FieldType();
		type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		type.setStored(true);
		type.setStoreTermVectors(true);
//		type.setStoreTermVectorOffsets(true);
		type.setStoreTermVectorPositions(true);
//		type.setStoreTermVectorPayloads(true);
		doc.add(new Field("title", docline[2], type));
		doc.add(new Field("body", docline[3], type));

		IndexWriter indexWriter = new IndexWriter(ramDirectory, config);
		indexWriter.addDocument(doc);
//		indexWriter.commit();
		indexWriter.close();
		
		IndexReader indexReader = DirectoryReader.open(ramDirectory);
		Terms terms = indexReader.getTermVector(0, "body");
		TermsEnum it = terms.iterator();
		BytesRef term = it.next();

//		while (term != null) {
////			final long freq = it.totalTermFreq();
//			// returns the total number of occurrences of this term across all documents
//			// the sum of the freq() for each doc that has this term
////			PostingsEnum postings = it.postings(null, PostingsEnum.OFFSETS);
//			PostingsEnum postings = it.postings(null, PostingsEnum.POSITIONS);
//			int doc_id = postings.nextDoc();
//			
//			int totalFreq = postings.freq();
//			for (int i = 0; i < totalFreq; ++i) {
//				int cur_pos = postings.nextPosition();
////				int cur_startoffset = postings.startOffset();
//				String cur_word = term.utf8ToString();
//				emitValue.set(docline[0] + "(position):" + cur_pos );
//	//			emitValue.set(docline[0]);
//				context.write(new Text(cur_word), emitValue);
//			}
//			term = it.next();
//		}
		while (term != null) {
			PostingsEnum postings = it.postings(null, PostingsEnum.POSITIONS);
			int doc_id = postings.nextDoc();
			int totalFreq = postings.freq();
			for (int i = 0; i < totalFreq; ++i) {
				int cur_pos = postings.nextPosition();
				String cur_word = term.utf8ToString();
				
				cur_word.trim();
				cur_word = cur_word.replaceAll("[^.a-zA-Z0-9]", "");
				if (cur_word.length() > 3) {
					if (cur_word.contains("."))
						continue;
					boolean repeatedchar = false;
					for (char c = '0'; c <= '9'; c++) {
						if (cur_word.matches(String.valueOf(c) + "+")) {
							repeatedchar = true;
							break;
						}
					}
					if (repeatedchar) continue;
					for (char c = 'a'; c <= 'z'; c++) {
						if (cur_word.matches(String.valueOf(c) + "+")) {
							repeatedchar = true;
							break;
						}
					}
					if (repeatedchar) continue;
					emitValue.set("(" + "d:" + docline[0] + " p:" + cur_pos + ")");
					context.write(new Text(cur_word), emitValue);
				}
			}
			term = it.next();
		}
	}
}