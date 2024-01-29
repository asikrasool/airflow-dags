from pyspark.sql import SparkSession

def main():
    # Create a Spark session
    print("Running Hello World Spark")
    spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

    # Create a DataFrame with a single column "greeting"
    data = [("Hello, World!",)]
    columns = ["greeting"]
    df = spark.createDataFrame(data, columns)

    # Show the DataFrame
    df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
