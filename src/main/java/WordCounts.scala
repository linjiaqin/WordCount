
import org.apache.spark.{SparkConf, SparkContext}

object WordCounts {
  def main(args : Array[String])={
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")//spark://127.0.0.1:7077
    val jarPath = "/home/linjiaqin/IdeaProjects/WordCount/out/artifacts/WordCount_jar/WordCount.jar";

    val sc = new SparkContext(conf);
    sc.addJar(jarPath)
    println("hello");
    val hdfs = "hdfs://localhost:9000";
    val input = hdfs+"/linjiaqin/a.txt";
    val output = "";

    val text = sc.textFile(input);
    println(text.count())

    val words = text.flatMap(x=>x.split(" "));
    println(words.count())

    val wordcount = words.map(x=>(x,1));
    println(wordcount.count())

    val wordcounts = wordcount.reduceByKey((a,b)=>a+b)

    val wordsort = wordcounts.sortBy(x=>x._2,false).take(10)

    wordsort.foreach(x=>println(x._1,x._2))
  }
}