package Lucence;

import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.apache.solr.store.hdfs.HdfsDirectory;

public class createIndex {
	/**
	 * Description: 建立索引 建立索引的步骤
	 * 
	 * 1.创建Directory 2.创建IndexWriter 3.创建Document对象 4.为Document添加Field
	 */
	public void createIndex() {

		IndexWriter indexWriter = null;
		try {
			// 1.创建Directory
			Directory directory = new RAMDirectory(); // 在内存中建立索引
			// 2.创建IndexWriter
			IndexWriterConfig indexWiterConfig = new IndexWriterConfig(
					Version.LUCENE_36, new StandardAnalyzer(Version.LUCENE_36));
			indexWriter = new IndexWriter(directory, indexWiterConfig);
			// 3.创建Document对象
			Document document = new Document();
			// 4.为Document添加Field
			File filePath = new File("luence/example");
			for (File file : filePath.listFiles()) { // 为该文件夹下的所有文件建立索引
				document = new Document();
				// 传入文件内容
				document.add(new Field("content", new FileReader(file)));
				// 传入文件名
				document.add(new Field("filename", file.getName(),
						Field.Store.YES, Field.Index.NOT_ANALYZED));
				// 传入文件路径
				document.add(new Field("path", file.getAbsolutePath(),
						Field.Store.YES, Field.Index.NOT_ANALYZED));
				// 5.通过IndexWriter添加文档到索引中
				indexWriter.addDocument(document);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (indexWriter != null) {
				try {
					indexWriter.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}

	/**
	 * 搜索操作的步骤: 1.创建Directory 2.创建IndexReader 3.根据IndexReader创建IndexSearcher
	 * 4.创建搜索的Query 5.根据Searcher搜索并且返回TopDocs 6.根据TopDocs获取ScoreDoc对象
	 * 7.根据Seacher和ScoreDoc对象获取具体的Document对象 8.根据Document对象获取需要的值
	 * 9.关闭IndexReader
	 */

	public void searcher() {
		try {
			// 1.创建Directory 在硬盘上的F:/luence/index下建立索引
			Directory directory = FSDirectory.open(new File("F:/luence/index"));
			// 2.创建IndexReader
			IndexReader indexReader = IndexReader.open(directory);
			// 3.根据IndexReader创建IndexSearcher
			IndexSearcher indexSearcher = new IndexSearcher(indexReader);
			// 4.创建搜索的Query
			// 创建parser来确定要搜索文件的内容,第二个参数表示搜索的域, 实例中为"content",表示在内容中查找
			QueryParser queryParser = new QueryParser(Version.LUCENE_36,
					"content", new StandardAnalyzer(Version.LUCENE_36));
			// 创建query,表示搜索域为content中包含Java关键字的文档
			Query query = queryParser.parse("Java"); // 搜索包含关键字Java的信息
			// 5.根据Searcher搜索并且返回TopDocs
			// 查询,第二个参数表示显示前10条记录
			TopDocs topDoc = indexSearcher.search(query, 10);
			// 6.根据TopDocs获取ScoreDoc对象
			ScoreDoc[] scoreDocs = topDoc.scoreDocs;
			for (ScoreDoc scoreDoc : scoreDocs) {
				// 7.根据Seacher和ScoreDoc对象获取具体的Document对象
				Document document = indexSearcher.doc(scoreDoc.doc);
				// 8.根据Document对象获取需要的值
				System.out.println(document.get("filename") + "["
						+ document.get("path") + "]");
			}

			// 9.关闭IndexReader
			indexReader.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	/**
	 * 文档Document和域Field的关系 文档Document相当于关系表中的每一条记录,域相当于表中的每一个字段,先创建文档,之后为文档添加域.
	 * 域存储选项和域索引选项,均需要在域添加的时候设置
	 * 
	 * 存储域选项
	 * Field.Store.YES表示把这个域中的内容完全存储到文件中,方便进行文本的还原
	 * Field.Store.NO表示把这个域中的内容不存储到文件中,但是可以被索引,此时内容无法还原(即无法document.get());
	 * 
	 * 索引域选项
	 * Field.Index.ANALYZED：进行分词和索引,适用于标题和内容等
	 * Field.Index.NOT_ANALYZED：进行索引,但是不进行分词,像身份证号,姓名,ID等,适用于精确索索
	 * Field.Index.ANALYZED_NO_NORMS：进行分词但是不存储norms信息,这个norms中包括了创建索引的时间和权值等信息
	 * Field.Index.NOT_ANALYZED_NO_NORMS：即不进行分词也不存储norms信息
	 * Field.Index.NO：不进行索引
	 * 最佳实践
	 * Field.Index.NOT_ANALYZED_NO_NORMS,
	 * Field.Store.YES标识符(主键,文件名),电话号码,身份证号,姓名,日期
	 * Field.Index.ANALYZED, Field.Store.YES文档标题和摘要
	 * Field.Index.ANALYZED, Field.Store.NO文档正文
	 * Field.Index.NO,Field.Store.YES文档类型,数据库主键(不进行索引)
	 * Field.Index.NOT_ANALYZED,Field.Store.NO 隐藏关键字
	 */
	
	public void delete(){
        IndexWriter indexWriter = null;
        try {
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_36,new StandardAnalyzer(Version.LUCENE_36));
            indexWriter = new IndexWriter(directory, indexWriterConfig);
            //参数是一个选项,可以是一个Query,也可以是一个Term,Term是一个精确查找的值
            //此时删除的文档并不会完全被删除,而是存储在一个回收站中,可以恢复
            //使用Reader可以有效的恢复取到的文档数
            indexWriter.deleteDocuments(new Term("id","1"));
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(indexWriter!=null){
                try {
                    indexWriter.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
	//indexWriter.deleteDocuments()文档并不会完全被删除,而是存储在一个回收站中,我们可以编写查询类来进行查询
	public void query(){
        try {
            IndexReader indexReader = IndexReader.open(directory);
            System.out.println("存储的文档数:" + indexReader.numDocs());
            System.out.println("总存储量:" + indexReader.maxDoc());
            System.out.println("被删除的文档：" + indexReader.numDeletedDocs());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
