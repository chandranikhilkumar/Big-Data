val data = sc.textFile("/nxc161330/inputGraph/graphData.csv")
val data1 = data.map(line => line.split("\t")).map(line => (line(1), line(2).toDouble)).reduceByKey((count1, count2) => count1+count2)
val data2 = data1.sortBy(_._2)
data2.foreach(println)