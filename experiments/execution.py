import os,re,sys
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()
    # spark_folder = "/opt/spark/bin/spark-submit"
    mode= sys.argv[1]
    print(mode)
    filename = sys.argv[2]
    print(filename)
    strategy = sys.argv[3]
    print(strategy)
    file = filename.split("/")[1].split('.')[0]
    # strategy = "indexing"
    spark_folder = "/opt/spark/bin/spark-submit"
    stream = os.popen(
        """{} --master local[*] --executor-memory 2g --driver-memory 4g --conf spark.cassandra.output.consistency.level=ONE preprocess.jar {} {} 1 0 1 {}""".format(
            spark_folder, filename, strategy, mode))
    output = stream.read()
    print(output)
    try:
        m = int(re.search("Time taken: ([0-9]*?) ms", output).group(1)) / 1000
        print(m)
        with open("""output/{}${}.txt""".format(file, strategy), "w") as f:
            f.write(str(m))
    except:
        print("nop")