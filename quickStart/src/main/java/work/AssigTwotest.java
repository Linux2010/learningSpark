//package work;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import scala.Tuple2;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//
//public class Test {
//	private static final String N0 = "N0";
//
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaRDD<String> rdd =  sc.textFile("/Users/mac/workspace/learningSpark/quickStart/src/main/java/work/input.txt");
//
//		JavaPairRDD<String,Container> pairRDD = rdd.flatMapToPair(tuple->{
//			ArrayList<Tuple2<String, Tuple2<String, Integer>>> list = new ArrayList<>();
//			String[] lines = tuple.split(",");
//			list.add(new Tuple2<>(lines[0], new Tuple2<>(lines[1],Integer.parseInt(lines[2]))));
//			list.add(new Tuple2<>(lines[1], new Tuple2<>("",-1)));
//			return list.iterator();
//		}).groupByKey().mapToPair(tuple->{
//			Container c = new Container();
//			ArrayList<Tuple2<String, Integer>> arrayList = new ArrayList<>();
//			for(Tuple2<String, Integer> t : tuple._2) {
//				if(t._2 != -1) {
//					arrayList.add(new Tuple2<>(t._1,t._2));
//				}
//			}
//			if(tuple._1.compareTo(N0) == 0) {
//				c.setDistance(0);
//				c.setPath(N0);
//			}
//			c.setEdges(arrayList);
//			return new Tuple2<>(tuple._1, c);
//
//		});
//		int count = (int)pairRDD.count();
//
//		for(int i=0;i<count;i++){
//			pairRDD = pairRDD.flatMapToPair(tuple ->{
//				ArrayList<Tuple2<String, Object>> list = new ArrayList<>();
//				if(tuple._2.getDistance() >= 0) {
//					for(Tuple2<String, Integer> t:tuple._2.getEdges()) {
//						String dst = t._1;
//						Integer w = t._2;
//						list.add(new Tuple2<>(dst, new Tuple2<>(tuple._2.getPath()+"-"+dst, tuple._2.getDistance() + w)));
//					}
//				}
//				list.add(new Tuple2<>(tuple._1, tuple._2));
//				return list.iterator();
//			}).groupByKey().mapToPair(tuple->{
//				Container c = new Container();
//				ArrayList<Tuple2<String,Integer>> list = new ArrayList<>();
//				for(Object o: tuple._2) {
//					if(o instanceof Container) {
//						c = (Container) o;
//					}else {
//						Tuple2<String,Integer> t = (Tuple2<String,Integer>) o;
//						list.add(new Tuple2<>(t._1,t._2));
//					}
//				}
//				if (list.size() >0) {
//					String path = list.get(0)._1;
//					Integer distance = list.get(0)._2;
//					for(int k = 1; k < list.size(); k++) {
//						if(list.get(k)._2 < distance) {
//							distance = list.get(k)._2;
//							path = list.get(k)._1;
//						}
//					}
//					if(distance < c.getDistance()||c.getDistance() == -1) {
//						c.setDistance(distance);
//						c.setPath(path);
//					}
//				}
//				return new Tuple2<>(tuple._1, c);
//			});
//		}
//
//		pairRDD.filter(tuple->tuple._1.compareTo(N0) == 0? false:true).map(tuple->{
//			Result r = new Result();
//			r.setDst(tuple._1);
//			r.setDistance(tuple._2.getDistance());
//			r.setPath(tuple._2.getPath());
//			return r;
//		}).sortBy( tuple-> tuple.getDistance(), true, 1)
//				.saveAsTextFile("/Users/mac/workspace/learningSpark/quickStart/src/main/java/work/output");
//	}
//
//	public static class Container implements Serializable {
//		private Integer distance;
//		private String path;
//		private ArrayList<Tuple2<String, Integer>> edges;
//
//		public Container() {
//			distance = -1;
//			path = "";
//			edges = new ArrayList<>();
//		}
//
//		public Integer getDistance() {
//			return distance;
//		}
//		public void setDistance(Integer distance) {
//			this.distance = distance;
//		}
//		public String getPath() {
//			return path;
//		}
//		public void setPath(String path) {
//			this.path = path;
//		}
//		public ArrayList<Tuple2<String, Integer>> getEdges() {
//			return edges;
//		}
//		public void setEdges(ArrayList<Tuple2<String, Integer>> edges) {
//			this.edges = edges;
//		}
//
//		@Override
//		public String toString() {
//			StringBuilder sb = new StringBuilder();
//			sb.append("[").append(distance).append(",").append(path).append(",").append(edges.toString()).append("]");
//			return sb.toString();
//		}
//	}
//
//	public static class Result implements Serializable {
//		private String dst;
//		private Integer distance;
//		private String path;
//		public Result() {
//			dst = "";
//			distance = -1;
//			path = "";
//		}
//
//
//		public String getDst() {
//			return dst;
//		}
//
//		public void setDst(String dst) {
//			this.dst = dst;
//		}
//
//		public Integer getDistance() {
//			return distance;
//		}
//
//		public void setDistance(Integer distance) {
//			this.distance = distance;
//		}
//
//		public String getPath() {
//			return path;
//		}
//
//		public void setPath(String path) {
//			this.path = path;
//		}
//
//		public String toString() {
//			StringBuilder sb = new StringBuilder();
//			sb.append(dst).append(",").append(distance).append(",").append(path);
//			return sb.toString();
//		}
//	}
//
//}
