package river
import java.net._

object ewr_bonus {

  var i  = 0
  def main(args: Array[String]): Unit = {
    val city = "Nizhniy Novgorod"
    //    var res  = citiesOnRiver("Tolyatti")
    //    println(res)
    //var bc = Set[String]()
    //val res1 = getCities("Nizhniy Novgorod",bc)
    //println("ALL CITY:"+res1)
    var res = getCities(city)

    println(res)
  }

  def getCities(city: String): Set[String] ={
    val q = "select river from located where city = '" + city + "'"
    val eq = URLEncoder.encode(q, "UTF-8")
    val u = new java.net.URL("http://kr.unige.ch/phpmyadmin/query.php?db=Mondial"+"&sql="+eq)
    val in = scala.io.Source.fromURL(u, "iso-8859-1")
    var river = Set[String]()
    for (line <- in.getLines) {
      val cols = line.split("\t")
      river = river ++ getAllRivers(cols(0),river)
    }
    in.close()

    var res = Set[String]()
    for (r <- river){
       res = res ++ citiesOnRiver(r)
    }
    res
  }

  def getAllRivers(river:String,res:Set[String] ): Set[String] ={
    val sql = "select River,Lake from river where Name ='"+river+"'"
    i+=1
    println("deepin:"+(i)+" , sql : "+sql)
    val eq = URLEncoder.encode(sql, "UTF-8")
    val u = new java.net.URL("http://kr.unige.ch/phpmyadmin/query.php?db=Mondial"+"&sql="+eq)
    val in = scala.io.Source.fromURL(u, "iso-8859-1")
    var bc = res
    for (line <- in.getLines) {
      val cols = line.split("\t")
      if(cols(0)!=null&&cols(0)!=""){
        bc = res  ++ getAllRivers(cols(0),bc)
      }else if(cols(1)!=null&&cols(1)!=""){
        //bonus的逻辑
        //由湖泊索引到河流的算法
        val lake_sql = "select name from river where Lake ='"+cols(1)+"'"
        println("lake_sql: "+lake_sql)
        val lake_eq = URLEncoder.encode(lake_sql, "UTF-8")
        val lake_u = new java.net.URL("http://kr.unige.ch/phpmyadmin/query.php?db=Mondial"+"&sql="+lake_eq)
        val lake_in = scala.io.Source.fromURL(lake_u, "iso-8859-1")
        val lake_res =Set[String]()
        for(line <- lake_in.getLines){
          val cols = line.split("\t")
          bc = bc ++ LakeToRiver(cols(0),lake_res)

        }


      }
    }
    in.close()
    bc+=river
    bc
  }


  //bonus的逻辑
  //由湖泊索引所有流入河
  def LakeToRiver(river:String,res:Set[String] ): Set[String] ={
    val q = "select name  from river where River= '" + river + "'"
    i+=1
    println("deepin:"+(i)+" , sql : "+q)

    val eq = URLEncoder.encode(q, "UTF-8")
    val u = new java.net.URL(
      "http://kr.unige.ch/phpmyadmin/query.php?db=Mondial"+"&sql="+eq)
    val in = scala.io.Source.fromURL(u, "iso-8859-1")
    var bc = res
    for (line <- in.getLines) {
      val cols = line.split("\t")
      if(cols(0)!=null&&cols(0)!=""){
        bc = res  ++ LakeToRiver(cols(0),bc)
      }
    }
    in.close()
    bc+=river
    bc
  }



  def citiesOnRiver(r: String): Set[String] = {
    val q = "select city from located where river = '" + r + "'"
    val eq = URLEncoder.encode(q, "UTF-8")
    val u = new java.net.URL(
      "http://kr.unige.ch/phpmyadmin/query.php?db=Mondial"+"&sql="+eq)
    val in = scala.io.Source.fromURL(u, "iso-8859-1")
    var res = Set[String]()
    for (line <- in.getLines) {
      val cols = line.split("\t")
      res += cols(0)
    }
    in.close()
    res
  }

}
