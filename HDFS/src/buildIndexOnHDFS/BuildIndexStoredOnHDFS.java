package buildIndexOnHDFS;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.apache.solr.store.hdfs.HdfsDirectory;

public class BuildIndexStoredOnHDFS {
	public static void main(String[] args) throws Exception {
		// long a=System.currentTimeMillis();
		// add();
		// long b=System.currentTimeMillis();
		// System.out.println("耗时: "+(b-a)+"毫秒");
		query("今天");
		// delete("3");//删除指定ID的数据

	}

	/***
	 * 得到HDFS的writer
	 * 
	 * **/
	public static IndexWriter getIndexWriter() throws Exception {
		Analyzer analyzer = new SmartChineseAnalyzer(Version.LUCENE_CURRENT);
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46,
				analyzer);
		Configuration conf = new Configuration();
		Path path = new Path("hdfs://192.168.0.101:9000/lucence");
		HdfsDirectory directory = new HdfsDirectory(path, conf);
		IndexWriter writer = new IndexWriter(directory, config);
		return writer;

	}

	/**
	 * 建索引的方法
	 * 
	 * **/
	public static void add() throws Exception {
		IndexWriter writer = getIndexWriter();
		Document doc = new Document();
		doc.add(new StringField("id", "3", Store.YES));
		doc.add(new StringField("name", "lucene是一款非常优秀的全文检索框架", Store.YES));
		doc.add(new TextField("content", "今天发工资了吗", Store.YES));
		Document doc2 = new Document();
		doc.add(new StringField("id", "4", Store.YES));
		doc2.add(new StringField("name", "今天天气不错呀", Store.YES));
		doc2.add(new TextField("content", "钱存储在银行靠谱吗", Store.YES));
		Document doc3 = new Document();
		doc3.add(new StringField("id", "5", Store.YES));
		doc3.add(new StringField("name", "没有根的野草，飘忽的命途！", Store.YES));
		doc3.add(new TextField("content", "你工资多少呀！", Store.YES));
		writer.addDocument(doc);
		writer.addDocument(doc2);
		writer.addDocument(doc3);
		writer.commit();
		writer.forceMerge(1);
		System.out.println("索引3条数据添加成功!");
		writer.close();
	}

	public static void DocIndex(){
		
	}
	/***
	 * 添加索引
	 * 
	 * **/
	public static void add(Document d) throws Exception {
		IndexWriter writer = getIndexWriter();
		writer.addDocument(d);
		writer.forceMerge(1);
		writer.commit();
		System.out.println("索引3条数据添加成功!");
		writer.close();
	}

	/**
	 * 根据指定ID 删除HDFS上的一些数据
	 * 
	 * **/
	public static void delete(String id) throws Exception {
		IndexWriter writer = getIndexWriter();
		writer.deleteDocuments(new Term("id", id));// 删除指定ID的数据
		writer.forceMerge(1);// 清除已经删除的索引空间
		writer.commit();// 提交变化
		System.out.println("id为" + id + "的数据已经删除成功.........");
	}

	/**
	 * 检索的方法
	 * 
	 * **/
	public static void query(String queryTerm) throws Exception {
		System.out.println("本次检索内容:  " + queryTerm);
		Configuration conf = new Configuration();
		Path path = new Path("hdfs://192.168.0.101:9000/lucence");
		Directory directory = new HdfsDirectory(path, conf);
		IndexReader reader = DirectoryReader.open(directory);
		System.out.println("总数据量: " + reader.numDocs());
		long a = System.currentTimeMillis();
		IndexSearcher searcher = new IndexSearcher(reader);
		SmartChineseAnalyzer analyzer = new SmartChineseAnalyzer(Version.LUCENE_CURRENT);
		QueryParser parse = new QueryParser(Version.LUCENE_4_9_0, "content",
				analyzer);
		Query query = parse.parse(queryTerm);
		TopDocs docs = searcher.search(query, 100);
		System.out.println("本次命中结果:   " + docs.totalHits + "  条");
		for (ScoreDoc sc : docs.scoreDocs) {
			System.out.println("评分:  " + sc.score + "  id : "
					+ searcher.doc(sc.doc).get("id") + "  name:   "
					+ searcher.doc(sc.doc).get("name") + "   字段内容: "
					+ searcher.doc(sc.doc).get("content"));
		}
		long b = System.currentTimeMillis();
		System.out.println("第一次耗时:" + (b - a) + " 毫秒");
		System.out.println("============================================");
		long c = System.currentTimeMillis();
		query = parse.parse(queryTerm);
		docs = searcher.search(query, 100);
		System.out.println("本次命中结果:   " + docs.totalHits + "  条");
		// for(ScoreDoc sc:docs.scoreDocs){
		//
		// System.out.println("评分:  "+sc.score+"  id : "+searcher.doc(sc.doc).get("id")+"  name:   "+searcher.doc(sc.doc).get("name")+"   字段内容: "+searcher.doc(sc.doc).get("content"));
		//
		// }
		long d = System.currentTimeMillis();
		System.out.println("第二次耗时:" + (d - c) + " 毫秒");
		reader.close();
		directory.close();
		System.out.println("检索完毕...............");

	}

}