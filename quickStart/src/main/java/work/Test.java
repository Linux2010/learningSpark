package work;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 1，获取sparkContext上下文,已local模式运行。
 * 2，textFile读取文本获取rdd。
 * 3，将string类型的rdd已‘，’进行split，然后进行pairmap转换成k-v对rdd，接着根据key排序，在进行map操作返回string，collection的rdd，
 *    最终将结果是将点能到达的点以及路径长度进行组装成一个二元组，例如 ：(N1,[(N2,2), (N3,2)]) ，(N4,[(N0,4), (N1,4), (N5,6)])
 * 4，循环操作将每个之前的二元组进行flatMapToPair和mapToPair操作，将点到点所有路径情况列出来，循环对比path的长度，取最短一条
 * 5，将计算的结果首先filter过滤NO到NO的数据，然后通过key（path）排序，然后saveAsTextFile写入rdd到文本。
 */
public class Test {
    private static final String N0 = "N0";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Collection> pairRDD = sc.textFile("E:\\IdeaProjects\\learningSpark\\quickStart\\src\\main\\java\\work\\input.txt")
                .map(s -> s.split(","))
                .mapToPair(tuple -> new Tuple2<>(tuple[0], new Tuple2<>(tuple[1], Integer.parseInt(tuple[2]))))
                .groupByKey()
                .mapToPair(tuple -> {
                    ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
                    for (Tuple2<String, Integer> t : tuple._2) {
                        if (t._2 != -1) {
                            list.add(new Tuple2<>(t._1, t._2));
                        }
                    }
                    Collection c = new Collection();
                    if (tuple._1.compareTo(N0) == 0) {
                        c.setDst(0);
                        c.setPath(N0);
                    }
                    c.setEdge(list);
                    return new Tuple2<>(tuple._1, c);
                });

        for (int i = 0; i < pairRDD.count(); i++) {
            pairRDD = pairRDD.flatMapToPair(tuple -> {
                ArrayList<Tuple2<String, Object>> list = new ArrayList<>();
                if (tuple._2.getDst() >= 0) {
                    for (Tuple2<String, Integer> t : tuple._2.getEdge()) {
                        list.add(new Tuple2<>(t._1, new Tuple2<>(tuple._2.getPath() + "-" + t._1, tuple._2.getDst() + t._2)));
                    }
                }
                list.add(new Tuple2<>(tuple._1, tuple._2));
                return list.iterator();
            }).groupByKey().mapToPair(tuple -> {
                Collection c = new Collection();
                ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
                for (Object o : tuple._2) {
                    if (o instanceof Collection) {
                        c = (Collection) o;
                    } else {
                        Tuple2<String, Integer> t = (Tuple2<String, Integer>) o;
                        list.add(new Tuple2<>(t._1, t._2));
                    }
                }
                if (list.size() > 0) {
                    String path = list.get(0)._1;
                    Integer dst = list.get(0)._2;
                    for (int k = 1; k < list.size(); k++) {
                        if (list.get(k)._2 < dst) {
                            dst = list.get(k)._2;
                            path = list.get(k)._1;
                        }
                    }
                    if (dst < c.getDst() || c.getDst() == -1) {
                        c.setDst(dst);
                        c.setPath(path);
                    }
                }
                return new Tuple2<>(tuple._1, c);
            });
        }

        pairRDD.filter(tuple -> tuple._1.compareTo(N0) == 0 ? false : true)
                .map(tuple -> new Tuple2<>(tuple._2.getDst(), new StringBuilder().append(tuple._1).append(",").append(tuple._2.getDst()).append(",").append(tuple._2.getPath()).toString()))
                .sortBy(tuple -> tuple._1, true, 1)
                .map(tuple -> tuple._2)
                .saveAsTextFile("E:\\IdeaProjects\\learningSpark\\quickStart\\src\\main\\java\\work\\output" + System.currentTimeMillis());
    }

     static class Collection implements Serializable {
        private Integer dst;
        private String path;
        private ArrayList<Tuple2<String, Integer>> edge;

        public Collection() {
            dst = -1;
            path = "";
            edge = new ArrayList<>();
        }

         public Integer getDst() {
             return dst;
         }

         public void setDst(Integer dst) {
             this.dst = dst;
         }

         public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public ArrayList<Tuple2<String, Integer>> getEdge() {
            return edge;
        }

        public void setEdge(ArrayList<Tuple2<String, Integer>> edge) {
            this.edge = edge;
        }
    }

}
