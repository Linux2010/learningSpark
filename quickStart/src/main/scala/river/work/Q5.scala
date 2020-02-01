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
    //(a)哪些国家拥有人口超过100万的城市?
    // select distinct c.name from country c inner join city ci on c.name = ci.country where ci.population>'1000000'

    //(b)位于两条(不同的)河上的城市有哪些?
    //select  * from (select city,count(city) as num from cityOn group by city) aa where aa.num>='2'


    //(c) what is the last city along river ’Congo’? (may require subqueries)  刚果河沿岸最后一个城市是什么?(可能需要子查询)
    /*
    select city from cityOn  where river='Congo' order by seqNo desc limit 1

    子查询：  select co.city from cityOn co where co.river='Congo' and co.seqNo=(select r.length from river r where r.name = 'Congo')
    * */

    //(d) for each river what is the average density of population in the coun- tries it flows through
    //
    /*
    select rt.river,GROUP_CONCAT(rt.country) as countries ,AVG(c.population) as avg_population from riverFlowsThrough rt
    inner join  country c
    on c.name=rt.country group by rt.river
    * */

    //2. 对于每个表，提供它的外键(如果有的话)
    // cityOn对city的外键（foreign key）是city字段（field） ， city对country的外键是country字段。
  }

  def Q5_2019: Unit ={
    //1. Write SQL queries to answer the following questions:

    //(a) what are the cities that have at least one train stations and less than 20000 inhabitants?
    /*
    SELECT DISTINCT c.* FROM city c INNER JOIN
      trainStationIn ti ON c.`name`=ti.city
    where c.population <'20000'
    */

    //(b) what are the lines that are circular, i.e. their first and last stations are the same?
    /*
    select distinct * from trainLine tl  inner join stationOn s
    on tl.length = s.seqNo and tl.name = s.station
    * */

    //(c) for each train line, what are the cities connected through this line?


    /*
   SELECT name,GROUP_CONCAT(city) as cities from trainStationIn group by name
    * */


    //(d) what is the first station of the “blue” line that is also on the “red” line (use subqueries)

    /*
    select (select * from stationOn s1 where s1.station='red') from stationOn  s where s.line='blue' and s.seqNo='1'
    * */


   // 2. For each table provide its foreign keys (if it has some)
    /*
    答案： trainStationIn对city的外键是city字段 。 stationOn对trainStationIn的外键是station字段。
    * */
  }


}
