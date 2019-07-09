from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,DoubleType
export SPARK_MAJOR_VERSION=2
export PYSPARK_PYTHON=python3.6
cat ~/.bash_profile



renga= SparkSession.builder.master('yarn').appName("Demo").getOrCreate()
 pyspark --master yarn --conf spark.ui.port=12345 --jars /usr/share/java/mysql-connector-java-new.jar --driver-class-path /usr/share/java/mysql-connector-java-new.jar

renga_orders = spark.read.csv("/user/shashankbh/jarvis/data/orders/part-00000").toDF("order_id","order_date","order_customer_id" ,"order_status")
renga_orders = renga_orders.withColumn("order_id",renga_orders.order_id.cast(IntegerType())).withColumn("order_customer_id",renga_orders.order_customer_id.cast(IntegerType()))
renga_order_items = spark.read.csv("/user/shashankbh/jarvis/data/order_items/part-00000").toDF("order_item_id","order_item_order_id","order_item_product_id" ,"order_item_quantity","order_item_subtotal","order_item_product_price")
renga_order_items = renga_order_items.withColumn("order_item_subtotal",renga_order_items.order_item_subtotal.cast(DoubleType())).withColumn("order_item_product_price",renga_order_items.order_item_product_price.cast(DoubleType()))
renga_orders.printSchema()



#Functions in pyspark.sql:

for that we need to import the commmand " from pyspark.sql.function import * "
1) substring :
			renga_orders.select(substring("order_status",1,5)).show()  -- will give the first 5 characters from the table which is mentioned 
2} alias:
			renga_orders.select(substring("order_status",1,5).alias("order_nilamai")).show(100)  --It will do alias operation in the output 
3)lower,upper:
			renga_orders.select(lower/upper(renga_orders.order_status).alias("order_status")).show() -- will make the mentioned column in lower/upper 
4)Case_when_then_else:
			 renga_orders.selectExpr('case when order_status in ("COMPLETE","CLOSED") then "delievery agirchu da" when order_status =="CANCELED" then "puttukichea" else "PENDING" end',"order_status").show()
			  renga_orders.selectExpr('(case when order_status in ("COMPLETE","CLOSED") then "delievery agirchu da" when order_status =="CANCELED" then "puttukichea" else "PENDING" end) as test',"order_status").show()
			  it will do operat9ion and selec the row and create a new column in that
					
5) date-fromat: before that we need to import pyspark.sql.functions 

			renga_orders.select("order_id","order_date",f.date_format("YYYY-MM","order_date")).show()
			
6)__or__: 
		 renga_orders.where((renga_orders.order_status =="COMPLETE").__or__(renga_orders.order_status=="CLOSED").show()       OR we can use this
		 renga_orders.where(renga_orders.order_status.isin("COMPLETE","CLOSED","CANCELLED")).show()
		 
7)like:
		renga_orders.where(renga_orders.order_status.isin("COMPLETED","CLOSED") and renga_orders.order_date like '2018-08%').show()

8)__and__ :
		renga_orders.where(renga_orders.order_status.isin("COMPLETE","CLOSED").__and__(date_format(renga_orders.order_date,"YYYY-MM") == '2013-08')).show()
				If we use a dataframe operation means we need to do it completed we should not add this by performing any other global operation
				
9)join:
		renga_orders.where(renga_orders.order_status.isin("COMPLETE","CLOSED")).join(renga_order_items,renga_orders.order_id==renga_order_items.order_item_order_id).show()
		
			a) first we need to apply filter for the case where we need to get the data and we need to apply join the data where we need to apply  join the above
				
10)isNull():
		renga_orders.where(renga_orders.order_status.isin("COMPLETE","CLOSED")).join(renga_order_items,renga_orders.order_id==renga_order_items.order_item_order_id).where(renga_order_items.order_item_order_id.isNull()).select(renga_order.order_id,renga_order.order_status,renga_order_items.order_item_quantity,renga_order_items.order_item_product_price).show()


11)aggregation():
		renga_order_items.filter("order_item_product_id"=2).agg(sum("order_item_subtotal").show()
		
12)GroupBy():
		renga_order_items.filter("order_item_order_id <=50").groupBy("order_item_order_id").agg(round(sum("order_item_subtotal"),2)).show()
				In this we could see group the data by the argument which we are passing

				


renga_orders.filter(renga_order.order_status.isin("COMPLETE","CLOSED")).select(renga_orders.order_id,renga_orders.order_date,renga_orders.order_status,renga_orders.order_item_product_id).join("renga_order_items",renga_orders.order_id ==renga_order_items.order_item_order_id).groupBy(renga_orders.order_status).agg(round(sum(renga_order_items.order_item_subtotal),2).alias("count")).show()
				
				
				
				
				
				
				
				
				
				
				
				
				
				
				
				
				
	
#to select data from hive there are two steps one is direstly and other  is spark.sql

#step1 

products = renga.read.table("jarvis.products")
products.show()

#step2

product = renga.sql("select * from jarvis.products").show()

# import data from jdbc
#step1

vanaja = renga.read.format("jdbc").\
		 option('url','jdbc:mysql://ms.itversity.com').\
		 option("driver","com.mysql.jdbc.driver")
		 option('dbtable','retail_db.cutomers').\
		 option('user','retail_user').\
		 option('password','itversity').\
		 load()


 --packages groupId:artifactId:version
 

 
vanaja.printSchema()
vanaja.show()
# we need to add --jars /usr/share/java/mysql-connector-java-6 --driver-class-path /usr/share/java/mysql-connector-java-6
test = spark.read.jdbc("jdbc:mysql://ms.itversity.com","retail_db.customers",properties={"user":"retail_user","password":"itversity"})

#if we give this the number of partition will be one to change the number of partition we must give numPartition
raja = renga.read.jdbc("jdbc:mysql://ms.itversity.com","retail_db.customers",numPartition=4,properties={"user":"retail_user","password":"itversity"}
#partition  by speciic column we need to give(the column) and we can see the column by writing the the the column in a path and seeing the partition

raja = renga.read.jdbc("jdbc:mysql://ms.itversity.com","retail_db.customers",numPartition=4,column="order_status",lowerBound= 100,upperBound=20000,properties={"user":"retail_user","password":"itversity"}


raja.printSchema()
raja.show()


#--conf spark.driver.extraClassPath=Jar/mysql-connector-java-6.0.6.jar --packages=“mysql:mysql-connector-java:6.0.6”