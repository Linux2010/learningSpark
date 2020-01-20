package river.work

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2020/1/19
  */
object Q1 {
  def main(args: Array[String]): Unit = {
    //1,满足以上需求的 L的充要条件是：假设 L数集中<0的子集数量为R1 ，L数集中>0的子集数量为R2，则充要条件为： |R1-R2|<=1,且0不属于L。
//    val array = Array(1,1,-2,-2,3,-3,4,-5)
//    var add = new Array[Int](_)
//2，编写scala代码
/*val array = Array(1,1,-2,-2,3,-3,4,-5,12)
    val arraySorted = array.sorted
    arraySorted.foreach(println(_))
    val j =arraySorted.length
    for(i<-0 until j){
      if(j%2==0){
        if((j-i*2)>=2){
          print(arraySorted(i))
          print(" , ")
          print(arraySorted(j-i-1))
          print(" , ")
        }
      }else{
        print("ji")
        val end = arraySorted((j-1)/2)//取奇数数组中，最中间一位


      }
    }*/

  }

  def Q1_2018(): Unit ={
    //1,满足以上需求的 L的充要条件是：假设 L数集中<0的子集数量为R1 ，L数集中>0的子集数量为R2，则充要条件为： |R1-R2|<=1,且0不属于L。
    //2，编写scala代码
    val array = Array(1,1,-2,-2,3,-3,4,-5)

    var add:Array[Int] = Array()//正数队列
    var del:Array[Int] =Array()//负数队列

    for(a<- 0 until array.length){
      if(array(a)>0){
        add:+array(a)
      }else{
        del:+array(a)
      }
    }

    val i =  add.length-del.length
    if(i>1 || i <(-1)){
      println("Array error")
      System.exit(0)
    }

    var res: Array[Int] = Array()


    //若正数多一位，则正数开头，正数收尾
    if(add.length>del.length){
      for(i<- 0 until del.length){
        res:+add(i)
        res:+del(i)
      }
      res:+add(del.length)
    }
    //若负数多一位，则负数开头，负数收尾
    if(add.length<del.length){
      for(i<- 0 until add.length){
        res:+del(i)
        res:+add(i)
      }
      res:+del(add.length)
    }
    //若正数和负数数量一样，正数开头，负数收尾
    if(add.length==del.length){
      for(i<- 0 until add.length){
        res:+del(i)
        res:+add(i)
      }
    }
    res.foreach(println(_))
  }

  def Q1_2019(): Unit ={



  }
}
