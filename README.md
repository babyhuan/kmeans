# kmeans
# 分为三部分
# 1. 蓄水池抽样生成聚类中心向量
# 2. kmeans算法，更新聚类中心向量
# 3. 根据最终聚类中心向量对原始数据进行分类
# 文件目录结构如下：
	#   --classify
 	#   	   --ClassifyPartition.java
 	#          --ClassifyReducer.java
 	#  --driver
 	#      	   --AllJob.java
 	#          --KmeansDriver.java
 	#  --mr
 	#          --KmeansMapper.java
 	#          --KmeansReducer.java
 	#          --MyWritable.java
 	#  --reservori
 	#          --driver
 	#                  --ReservoriDriver.java
 	#          --mr
 	#                  --ReservoriMapper.java
 	#                  --ReservoriReducer.java
 	#  --util
 	#          --JarUtil.java
 	#          --Utils.java
 	#  --log4j.properties
	
        
