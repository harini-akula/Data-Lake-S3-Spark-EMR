# Sparkify Data Lake

Sparkify data lake stores the user activity and songs metadata on Sparkify's new streaming application. This data lake is hosted on S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. ETL pipeline is built that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

## Purpose

- The dimensional tables on S3 provides Sparkify's analytical team with access to their data.
- The design of the database tables is optimized for queries on song play analysis.

## Database Schema Design

Sparkify tables form a star schema. This database design separates facts and dimensions yielding a subject-oriented design where data is stored according to logical relationships, not according to how the data was entered. 

- Fact And Dimension Tables

    The database includes:
    - Fact table:
        
        1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
            - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
            
    - Dimension tables:
        
        2. **users** - users in the app
            - user_id, first_name, last_name, gender, level
        3. **songs** - songs in music database
            - song_id, title, artist_id, year, duration
        4. **artists** - artists in music database
            - artist_id, name, location, latitude, longitude
        5. **time** - timestamps of records in songplays broken down into specific units
            - start_time, hour, day, week, month, year, weekday

## ETL Pipeline

An ETL pipeline is built using Spark. The Spark application extracts data stored on S3 in JSON files format, processes and transforms them into analytical tables and loads the dimensional tables back onto S3. 
    
## Running Python Script, spark application, on local machine
    
1. **etl.py**
This script reads data from S3 JSON files and transforms them into dimension and fact tables and writes them to partitioned parquet files in table directories on S3 that can be used for further analysis using Spark.
*Run below command in terminal to execute this script on local machine:*
        `python etl.py`

## Launching EMR cluster 
1. Login to AWS account.

2. Creating EC2 Key Pairs:
    - From Services tab, select EC2 service.
    - In EC2 Dashboard, under NETWORK & SECURITY, click on Key Pairs.
    - Click on Create key Pair button.
    - Enter Name of the key pair to be created, example spark-cluster-kp, and select ppk for windows machine.
    - Click on Create key Pair button and a new key pair will be downloaded to your machine.
    
3. In case of key load invalid format error, convert ppk to OpenSSH key
    - Install puttygen
    - Click on load button
    - Select ppk file and enter Key passphrase and Confirm passphrase
    - Click on Conversions tab, Export OpenSSH key
    - Enter file name and save the file
    
4. Creating EMR cluster:
    - From Services tab, select EMR service.
    - Click on Clusters
    - Click on Create cluster button
    - In Create Cluster - Quick Options, enter details as required or as follows:
        - Under General Configuration, enter Cluster name: spark-cluster
        - Under Software configuration, enter Release: emr-5.20.0, and for Applications select Spark: Spark 2.4.4 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.2
        - Under Hardware configuration, select Instance type: m5.xlarge, Number of instances: 3
        - Under Security and access, select previously created EC2 key pair, spark-cluster-kp, from the dropdown
    - Click on Create cluster button and wait for the status of the cluster to change from Staring to Waiting
    
5. Add rules to a master node's security group
    - Select EC2 service
    - Click on master node's security group under Security groups
    - Click on Inbound tab
    - Click Edit and click on Add Rule button
    - Select source as My IP and click Save
    
6. Login to EMR cluster using aws cli
    - Install aws cli
    - Open command prompt
    - Run below command and enter AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and region details
    `aws configure`

7. Run below command in command line to connect to EMR cluster 
`aws emr ssh --cluster-id j-2AL4XXXXXX5T9 --key-pair-file ~/OpenSSHkey --region us-west-2`

## Running python script, spark application, on EMR cluster
1. Run below command for location of spark-submit
`which spark-submit`

2. **emr-etl.py**
This script runs on cluster launched on EMR and reads data from S3 JSON files and transforms them into dimension and fact tables and writes them to partitioned parquet files in table directories on S3 that can be used for further analysis using Spark. 
Run below command to execute script on EMR:
`path/spark-submit ./emr-etl.py`













    

