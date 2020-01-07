package river
import java.net._
/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/12/31
  */
object ewr {

  var i  = 0
  def main(args: Array[String]): Unit = {
//    var res  = citiesOnRiver("Tolyatti")
//    println(res)
    var bc = Set[String]()
    val res1 = getCities("Nizhniy Novgorod",bc)
    println("ALL CITY:"+res1)
/*
    var res2 = res++res1
    println(res2)
    val res3 = res --res1
    println(res3)
*/



  }


  // a method to find all the cities on river r
  def citiesOnRiver(city: String): Set[String] = {
    val sql ="select t2.city from located t1 inner join located t2 on t1.River = t2.River where t1.City='"+city+"' and t2.City !='"+city+"'"
    val eq = URLEncoder.encode(sql, "UTF-8")
    val u = new java.net.URL(
      "http://kr.unige.ch/phpmyadmin/query.php?db=Mondial"+"&sql="+eq)
    val in = scala.io.Source.fromURL(u, "iso-8859-1")
    var res = Set[String]()
    for (line <- in.getLines) {
      val cols = line.split("\t")
      res += cols(0)
    }
    in.close()
    res }


  //select t2.* from located t1 inner join located t2 on t1.River = t2.River where t1.City='Nizhniy Novgorod' and t2.City !='Nizhniy Novgorod'
  def getCities(city: String,bc:Set[String]): Set[String] = {

    val sql ="select t2.city,t2.river from located t1 inner join located t2 on t1.River = t2.River where t1.City='"+city+"'"
    i=i+1
    println("------deepin: "+i)
    println(sql)
    val eq = URLEncoder.encode(sql, "UTF-8")
    val u = new java.net.URL(
      "http://kr.unige.ch/phpmyadmin/query.php?db=Mondial"+"&sql="+eq)
    val in = scala.io.Source.fromURL(u, "iso-8859-1")
    var res = Set[String]()
    var riverSet = Set[String]()
    for (line <- in.getLines) {
      val cols = line.split("\t")
      res += cols(0)
      riverSet += cols(1)
    }
    in.close()
    println("res:"+res)
    println("river on "+city+" is:"+riverSet)
    println("bc:"+bc)
    val re =res++bc
    val nextSet = res--bc
    println("nextSet:"+nextSet)

    if(nextSet.size!=0){
      for (c <- nextSet){
        getCities(c,re)
      }
    }
    re
  }

}
