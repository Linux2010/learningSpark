package river.work

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2020/1/20
  */
object Q2 {

  def main(args: Array[String]): Unit = {
    val t = Array(3,1,2,5,4)
    val u = Array(1)

    //Q2_2019
    val res = finder(t,u)
    println(res)

  }

  def Q2_2018: Unit = {
    val t = Array(3,1,2,5,4)
    val u = Array(1,2)

    Q2_2019
    val res = finder(t,u)
    println(res)
    //1. what is the time complexity, as a function of N and M, of the following algorithm
    //这是一个二分法查找 ，complexity为 O(M*log(N))
    //2. What does k represent at the end of the algorithm’s execution?
    //  K为M在N中匹配的数量
    //3. Now suppose that t is also sorted
    //• explain how the algorithm could be optimized for this case
    //  对N进行二分法，判断与M的大小
    //• what would be its time complexity?
    //  时间复杂度  O(log(N*M))
  }

  def finder(t: Array[Int], u: Array[Int]): Int = {
    val M = u.length
    var k = 0
    for (x <- t) {
      var p = 0
      var r = M - 1
      var s = (p + r) / 2
      while (u(s) != x && p <= r) {
        if (u(s) < x) r = s + 1
        else r = s - 1
        s = (p + r) / 2
      }
      if (u(s) == x) k += 1
    }
    return k
  }


  def Q2_2019: Unit = {
    val someWords = List("x", "bop", "bip")
    val someText = List("a", "bop", "bip", "x", "x", "bip", "bip", "bop")
    val res = cntr(someWords, someText)
    println(res)
    //1. IfsomeWords=List("x","bop","bip")andsomeText=List("a","bop", "bip", "x", "bip", "bip", "bop"), what would be the value (map) re- turned by cntr(someWords, someText)?
    //答案 ：Map(x -> 1, bop -> 2, bip -> 3)


    //2. What is the time complexity of this algorithm as a function of the size N of words and the size M of text?

    //答案： M!=0则complexity = O(N*M) ； M==0这则complexity = O(N)


  }

  def cntr(words: List[String], text: List[String]):
  Map[String, Int] = {
    var res = Map[String, Int]()
    for (w <- words) {
      var count = 0
      for (t <- text) {
        if (w == t) count += 1
      }
      res = res + (w -> count)
    }
    return res
  }

}
