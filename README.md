# Apache Spark, Python

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Python and an optimized engine that supports general computation
graphs for data analysis. It also supports Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing, and Spark Streaming for
stream processing.  <http://spark.apache.org/>


## TOOLS

* Apache Spark
* Anaconda (iPython, Scipy, Pandas, etc)


## Firing Interactive Python (iPython) with Spark

    # Check Spark is ready (after intalling Java SDK and unpacking Spark)
    >>$ ./bin/pyspark

    # Launch iPython with Spark (Python 2.7)
    >>$ IPYTHON_OPTS="notebook" ./bin/pyspark

    # With Python 3
    >>$ IPYTHON_OPTS='notebook' PYSPARK_PYTHON=python3

    # Run Spark Cluster - Master. Notice the Cluster URL
    (spark://UserName.local:7077) that can be found at http://localhost:8080
    >>$ ./sbin/start-master.sh


    # Create & Register New Worker on Master Cluster
    >>$ ./bin/spark-class org.apache.spark.deploy.worker.Worker spark://UserName.local:7077

    # Submit job to Cluster (using example python program in Spark)
    >>$ bin/spark-submit --master spark://UserName.local:7077 examples/src/main/python/pi.py

    # Write and deploy own python program to the Spark Cluster
    >>$ bin/spark-submit --master spark://UserName.local:7077 --packages com.databricks:spark-csv_2.10:1.3.0 mySpark_files/uberstats.py mySpark_files/Uber-Jan-Feb-FOIL.csv


## Submit Job to Amazon EC2
    # Create AWS USER profiles: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from
    https://console.aws.amazon.com/.

    * Create User Profile
    * Create security credentials and download a .pem file.
    * Create permissions for the User Profile

    # Set Amazon EC2 environment variables
    >>$ export AWS_SECRET_ACCESS_KEY=<Some Keys>
    >>$ export AWS_ACCESS_KEY_ID=<AKIAJXSITV4RAGQPGA6A>

    # Create & Launch new Python Spark Cluster on Amazon EC2.
    >>$ chmod 400 downloaded.pem
    >>$ ec2/spark-ec2 --key-pair=spark_python --identity-file=downloaded.pem --zone=us-west-2a launch spark-cluster-example

    # Running iPython console after creating Cluster where ec2-54-198-139-10.compute-1.amazonaws.com is the public DNS created my MASTER
    >>$ bin/spark-shell --master spark://ec2-54-198-139-10.compute-1.amazonaws.com:7077
