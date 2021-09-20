import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object cv19_etl extends App{
  
      val sConf=new SparkConf()
  sConf.set("spark.app.name","app")
  sConf.set("hive.exec.dynamic.partition","true")
  sConf.set("hive.exec.dynamic.partition.mode","nonstrict")

val spark=SparkSession.builder
           .enableHiveSupport()
           .config(sConf)
           .getOrCreate
           
  val cv19_df=spark.read
         .format("csv")
         .option("header", true)
         .option("mode", "permissive")
         .option("inferSchema",true)
         .option("path","/user/itv001060/warehouse/cv19.db/covid19_data_stg")
         .load

 cv19_df.createOrReplaceTempView("covid19")


val cv19_df1=spark.sql("""SELECT  
        cast(FIPS as int) as FIPS,
        Admin2 as Admin2,
        Province_State as Province_State,
        Country_Region as Country_Region,
        cast(lat as double) as lat,
        cast(long_ as double) as long,
        cast(Confirmed as bigint) as Confirmed,
        cast(Deaths as bigint) as Deaths,
        cast(Recovered as bigint) as Recovered,
        cast(Active as bigint) as Active,
        Combined_Key ,
        cast(Incident_Rate as double) as Incident_Rate,
        cast(Case_Fatality_Ratio as double) as Case_Fatality_Ratio,
        cast(cast(Last_Update as timestamp) as date) as Last_Update
FROM covid19""")

cv19_df1.write.mode("overwrite").insertInto("cv19.covid_19_data")

  
  
}