package com.fastweb.panda_test

/**
  * Created by Administrator on 2017/8/2.
  */
object down {


  def main(args: Array[String]) {

    val datas = List((1497622320, 1497622320, 60), (1497622380, 1497622380, 110),
      (1497622440, 1497622440, 180), (1497622450, 1497622450, 190),
      (1497622550, 1497622550, 60), (1497622610, 1497622610, 120), (1497622617, 1497622617, 127))
    val data = datas.sortBy(x => x._1)
    val data_duration = data.sortBy(x => x._3)
    val data_set = collection.mutable.Set[(Int, Int, Int)]()
    val length = data.size
    if (data == data_duration){
      if (length >=2 ){
        if (data(length-1)._3 - 60 < data(length-2)._3){
          data_set.add((data(length-1)._2 -data(length-1)._3, data(length-1)._2, data(length-1)._3 ))
        }
      }

    }
    else {
    for (i <-  0 until length-1) {
      if (i != length-2){
          if (data(i + 1)._3 < data(i)._3) {
          data_set.add((data(i)._2 -data(i)._3, data(i)._2, data(i)._3 ) )
         }}
      else
      {if (data(i + 1)._3 - 60 < data(i)._3){
        data_set.add((data(i+1)._2 -data(i+1)._3, data(i+1)._2, data(i+1)._3 ))
      }
      }
    }
    }
    //data_list.add((data.last)._2-data.last._3, data.last._2, data.last._3)
    data_set.toList.foreach(x=>println(x))
  }

}