var business= sc.textFile("/nxc161330/inputBusiness/business.csv")
var review= sc.textFile("/nxc161330/inputReview/review.csv")

var businessData = business.map(line=>line.split("\\::")).filter(line=>line(1).contains("Stanford")).map(line=>(line(0)))

var reviewData = review.map(line=>line.split("\\::")).map(line=>(line(1),line(2),line(3)))

var businessDF = businessData.toDF("bid")
var reviewDF = reviewData.toDF("uid","bid","rat")

var output = reviewDF.join(businessDF,"bid").distinct().select("uid","rat")
output.rdd.saveAsTextFile("hdfs:/nxc161330/hw2question2/")

