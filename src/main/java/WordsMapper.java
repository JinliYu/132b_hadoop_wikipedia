/**
 * 132B Basic inverted index
 * Group number: cs132g2
 * Group name: Yet Another Mapreduce Program (YAMP)
 * Group members: Shuai Yu, Jinli Yu, Chuanxiong Yi, Minghui Zhu
 */
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
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
		
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		RAMDirectory ramDirectory = new RAMDirectory();

		Document doc = new Document();
		FieldType type = new FieldType();
		type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		type.setStored(true);
		type.setStoreTermVectors(true);
		type.setStoreTermVectorOffsets(true);
		type.setStoreTermVectorPositions(true);
		type.setStoreTermVectorPayloads(true);
		doc.add(new Field("title", docline[2], type));
		doc.add(new Field("body", docline[3], type));
//		doc.add(new Field("body", "quick fox run faster", type));

		IndexWriter indexWriter = new IndexWriter(ramDirectory, config);
		indexWriter.addDocument(doc);
//		indexWriter.commit();
		indexWriter.close();
		
		IndexReader indexReader = DirectoryReader.open(ramDirectory);
//		Terms terms = indexReader.getTermVector(0, "body");

		Terms terms = indexReader.getTermVector(0, "body");
		TermsEnum it = terms.iterator();
		BytesRef term = it.next();
		while (term != null) {
//			final long freq = it.totalTermFreq();
			// returns the total number of occurrences of this term across all documents
			// the sum of the freq() for each doc that has this term
			PostingsEnum postings = it.postings(null, PostingsEnum.OFFSETS);
			int doc_id = postings.nextDoc();
			
			int totalFreq = postings.freq();
			for (int i = 0; i < totalFreq; ++i) {
				int cur_pos = postings.nextPosition();
				int cur_startoffset = postings.startOffset();
				String cur_word = term.utf8ToString();
				emitValue.set(docline[0] + "(startOffset):" + cur_startoffset + "(position):" + cur_pos );
	//			emitValue.set(docline[0]);
				context.write(new Text(cur_word), emitValue);
			}
			term = it.next();
		}
	}
}
		
//		Terms tv = indexReader.getTermVector(0, "title");
//		
//		Document doc = new Document();
//		doc.add(new Field("title", docline[2],
//				Field.Store.YES,
//				Field.Index.ANALYZED,
//				Field.TermVector.WITH_POSITIONS_OFFSETS));
//		doc.add(new Field("body", "quick fox run faster", Field.Store.YES, Field.Index.ANALYZED,Field.TermVector.WITH_POSITIONS_OFFSETS));
// 
//		
//		Tokenizer analyzer = new StandardTokenizer();
//		analyzer.setReader(new StringReader(docline[3]));
//		TokenStream ts = new StopFilter(analyzer, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
//		ts = new LowerCaseFilter(ts);
//		ts = new SnowballFilter(ts, "English");
//		ts = new PorterStemFilter(ts);
////		TokenStream ts = analyzer.tokenStream("input", new StringReader(docline[3]));
////		SnowballFilter filter = new SnowballFilter(ts, "English");
////		ts = new PorterStemFilter(filter);
//		CharTermAttribute attr = ts.addAttribute(CharTermAttribute.class);
//		try {
//			ts.reset();
//			while (ts.incrementToken()) {
//				String cur_token = attr.toString();
//				cur_token.trim();
//				cur_token = cur_token.replaceAll("[^.a-zA-Z0-9]",  "");
////				cur_token = cur_token.replaceFirst("^0*", "");
//				if(cur_token.length() > 3) {
//					if (cur_token.contains(".") )
//							continue;
//					boolean repeatedchar = false;
//					for (char c = '0'; c <= '9'; c++) {
//						if (cur_token.matches(String.valueOf(c) + "+")) {
//							repeatedchar = true;
//							break;
//						}
//					}
//					if (repeatedchar)
//						continue;
//					for (char c = 'a'; c <= 'z'; c++) {
//						if (cur_token.matches(String.valueOf(c) + "+")) {
//							repeatedchar = true;
//							break;
//						}
//					}
//					if (repeatedchar)
//						continue;
//					if(NumberUtils.isDigits(cur_token.substring(0,1)) &&
//						!NumberUtils.isDigits(cur_token.substring(cur_token.length() - 1))) 
//						continue;
//					context.write(new Text(cur_token), docID);
//				}
//			}
//			ts.end();
//		} finally {
//			ts.close();
//			analyzer.close();
//		}
//	}
//}
