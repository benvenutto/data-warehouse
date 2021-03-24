# Data Warehouse Project

The aim of this project is to load data on songs, and user activity on a music streaming service, 
into an AWS RedShift cluster, hosted on Amazon's  public cloud.

## Cloud Infrastructure

This project uses Udacity supplied data which resides in Amazon storage S3 buckets. These buckets are hosted in the 
US West 2 (Oregon) region. Since the RedShift copy command is used to copy the data into RedShift, and we don't want 
to incur the latency (or expense) of shifting large volumes of data between regions, we will create our cluster in
this same region.

From the Amazon console, one can request a RedShift cluster, and if required it will be given a dedicated VPC.
By default, it is assumed that the client application using the RedShift aservice will also be running within
this VPC. To allow RedShift connections from outside the VPC (e.g.: from my desktop where I am running the Python scripts)
an ingres policy needs to be associated with the VPC.

Because the first copy command (the song data) is long running, as many directories and JSON files in the bucket must be 
traversed and parsed, initially I was getting a hang problem with the Python script: somewhere along the route the connection 
was broken and the Python script database driver saw no more responses from RedShift. This is a known problem,
and TCP keep alive parameters were added to the connection string to work around the issue.

## Contents

The project directory contains the following files:

| File | Description |
| ---- | ----------- |
| `sql_queries.py` | A python module (to be `import`'ed) that includes all SQL statements |
| `create_tables.py` | Python script to execute DDL recreate the database and empty tables from scratch. |
| `etl.py` | The ETL script to extract the data in the `log_data` and `song_data` S3 storage buckets, and load it into RedShift. |
| `dwh.cfg` | Configuration parameters. |

## Python scripts

The Python scripts can be run so:

```
    python create_tables.py
    python etl.py [--test]
```

The etl script accepts a --test parameter to perform a fast run on a subset of the data (a singe song directory and one events file).

## Schema

A mentioned, a star schema is used, consisting of a fact table, and a number of dimension tables.
The fact table `songplays` captures the event of playing a new song. Associated with it are the following dimension tables:

| Table | Usage |
| ----- | ----- |
| `users` | Records each user of the streaming service, including if they are paying for the service. |
| `songs` | Records iformation about the songs that users choose to listen to. |
| `artists` | Records information about the artists that perform the songs. |
| `time` | Records information related to the event in time at which users choose to play songs. |
