package river.work

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2020/1/20
  */
object Q5 {
  def main(args: Array[String]): Unit = {

  }

  def Q5_2018: Unit ={

  }

  def Q5_2019: Unit ={
    //1. Write SQL queries to answer the following questions:

    //(a) what are the cities that have at least one train stations and less than 20000 inhabitants?
    /*
    SELECT DISTINCT c.* FROM city c left JOIN
      trainStationIn ti ON c.`name`=ti.city
    where c.population <'20000'
    */

    //(b) what are the lines that are circular, i.e. their first and last stations are the same?




    //(c) for each train line, what are the cities connected through this line?
    //(d) what is the first station of the “blue” line that is also on the “red” line (use subqueries)

  }


}
