package work;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 1，获取sparkContext上下文,已local模式运行。
 * 2，textFile读取文本获取rdd。
 * 3，将string类型的rdd已‘，’进行split，然后进行pairmap转换成k-v对rdd，接着根据key排序，在进行
 */
public class Test {
    private static final String N0 = "N0";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("/Users/mac/workspace/learningSpark/quickStart/src/main/java/work/input.txt");


        JavaPairRDD<String, Tuple3<Integer,String,ArrayList<Tuple2<String, Integer>>>> pairRDD = rdd.map(s -> s.split(","))
                .mapToPair(row -> new Tuple2<>(row[0], new Tuple2<>(row[1], Integer.parseInt(row[2]))))
                .groupByKey()
                .mapToPair(tuple -> {
                    ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
                    for (Tuple2<String, Integer> t : tuple._2) {
                        if (t._2 != -1) {
                            list.add(new Tuple2<>(t._1, t._2));
                        }
                    }
                    if (N0.equals(tuple._1)) {
                        return new Tuple2<>(tuple._1, new Tuple3<>(0,N0,list));
                    }else {
                        return new Tuple2<>(tuple._1,new Tuple3<>(-1,"",list));
                    }
                });

        for (int i = 0; i < pairRDD.count(); i++) {
            pairRDD = pairRDD.flatMapToPair(tuple -> {
                ArrayList<Tuple2<String, Object>> list = new ArrayList<>();
                if (tuple._2._1() >= 0) {//tuple._2.getDistance()
                    for (Tuple2<String, Integer> t : tuple._2._3()) {//tuple._2.getEdges()
                        list.add(new Tuple2<>(t._1, new Tuple2<>(tuple._2._3() + "-" + t._1, tuple._2._1() + t._2)));
                    }
                }
                list.add(new Tuple2<>(tuple._1, tuple._2));
                return list.iterator();
            }).groupByKey()
                    .mapToPair(tuple -> {

                ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
                Tuple3<Integer,String,ArrayList<Tuple2<String, Integer>>> c =null;
                for (Object o : tuple._2) {
                    if (o instanceof Tuple3) {
                        c = (Tuple3) o;
                    } else {
                        Tuple2<String, Integer> t = (Tuple2<String, Integer>) o;
                        list.add(new Tuple2<>(t._1, t._2));
                    }
                }
                if (list.size() > 0) {
                    String path = list.get(0)._1;
                    Integer distance = list.get(0)._2;
                    for (int k = 1; k < list.size(); k++) {
                        if (list.get(k)._2 < distance) {
                            distance = list.get(k)._2;
                            path = list.get(k)._1;
                        }
                    }
                    if(c !=null)
                    if (distance < c._1() || c._1() == -1) {

                        return  new Tuple2<>(tuple._1,new Tuple3<>(distance,path,c._3()));
                    }
                }
                return new Tuple2<>(tuple._1, c);
            });
        }

        pairRDD.filter(tuple -> tuple._1.compareTo(N0) == 0 ? false : true)
                .map(tuple -> new Tuple2<>(tuple._2._1(), new StringBuilder().append(tuple._1).append(",").append(tuple._2._1()).append(",").append(tuple._2._2()).toString()))
                .sortBy(tuple -> tuple._1, true, 1)
                .map(tuple -> tuple._2)
                .saveAsTextFile("/Users/mac/workspace/learningSpark/quickStart/src/main/java/work/output" + System.currentTimeMillis());
    }

    public static class Container implements Serializable {
        private Integer distance;
        private String path;
        private ArrayList<Tuple2<String, Integer>> edges;

        public Container() {
            distance = -1;
            path = "";
            edges = new ArrayList<>();
        }

        public Integer getDistance() {
            return distance;
        }

        public void setDistance(Integer distance) {
            this.distance = distance;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public ArrayList<Tuple2<String, Integer>> getEdges() {
            return edges;
        }

        public void setEdges(ArrayList<Tuple2<String, Integer>> edges) {
            this.edges = edges;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(distance).append(",").append(path).append(",").append(edges.toString()).append("]");
            return sb.toString();
        }
    }

}
