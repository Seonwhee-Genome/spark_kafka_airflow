import subprocess, re
from pyspark.sql import SparkSession
"""
spark-submit --master yarn --deploy-mode client --num-executors 2 --driver-memory 2g --executor-memory 2g --executor-cores 1 spark_df.py 3
"""


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ProverbWordCount").getOrCreate()

    with open("./proverb.txt", "w") as file:
        file.write("Better a live coward than a dead hero.\nIf you can't beat them, join them.\nToo many cooks spoil the broth.\nBirds of a feather flock together.")
    
    
    result = subprocess.run(['ls', '-al'], capture_output=True, text=True)
    print(result.stdout)

    result = subprocess.run(['cat', './proverb.txt'], capture_output=True, text=True)
    print(result.stdout)
    subprocess.run(['hadoop', 'fs', '-put', '-f', './proverb.txt', '/user/zeppelin/'])

    result = subprocess.run(['hadoop', 'fs', '-ls', '/user/zeppelin/'], capture_output=True, text=True)
    print(result.stdout)
    doc = spark.sparkContext.textFile("/user/zeppelin/proverb.txt")

    doc2 = doc.filter(lambda line: len(line)>0)\
        .flatMap(lambda line: re.split('\W+', line))\
        .filter(lambda word: len(word)>0)\
        .map(lambda word:(word.lower(),1))\
        .reduceByKey(lambda v1, v2: v1+v2)\
        .map(lambda x: (x[1], x[0]))\
        .sortByKey(ascending=False)    

    print(doc2.take(3))

    spark.stop()

