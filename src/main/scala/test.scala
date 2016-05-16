/**
  * Created by SX_H on 2016/5/16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
object test {
def main(args:Array[String])= {
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val hadoopConf = sc.hadoopConfiguration
  hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  hadoopConf.set("fs.s3n.awsAccessKeyId", "AKIAJIVCMRHJ56IM7IVA")
  hadoopConf.set("fs.s3n.awsSecretAccessKey", "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")

  //val path:String = "s3n://word.txt"
  val path = "hdfs:///sx/test"
  val data = sc.textFile(path)
    .flatMap {
      x =>
        x.split(",").map { y => (y, 1) }
    }
    .reduceByKey { case (x: Int, y: Int) =>
      x + y
    }
    .collect().foreach(x => println("suxing " + x))
}
}
