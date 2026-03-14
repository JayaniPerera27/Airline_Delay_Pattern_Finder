Airline Delay Pattern Finder
Module: Cloud Computing (EE7222/EC7204) | University of Ruhuna | 2026

Team Members
Index NumberName
EG/2021/4823Tamasha A.P.D.
EG/2021/4711Perera M.A.J.C.
EG/2021/4706Peiris P.R.S.

About
This project uses Apache Hadoop MapReduce to analyze US flight delays from 2019–2023.
It finds which airlines have the most delays and which airports are most delayed.

Dataset

Source: Kaggle — Flight Delay and Cancellation Dataset (2019–2023)
File: 2019.csv (~700MB, 7 million records)
Link: https://www.kaggle.com/datasets/patrickzel/flight-delay-and-cancellation-dataset-2019-2023


Requirements

Java JDK 1.8
Apache Hadoop 3.3.6
Windows 10/11
8GB RAM minimum


How to Run
1. Start Hadoop
start-dfs.cmd
start-yarn.cmd

2. Upload dataset to HDFS
hdfs dfs -mkdir /airline
hdfs dfs -put 2019.csv /airline/

3. Compile and package
javac -classpath "C:\hadoop\share\hadoop\..." -d . AirlineDelayCount.java
jar cf AirlineDelayCount.jar AirlineDelayCount*.class

4. Run Job 1 — Delays per Airline
hadoop jar AirlineDelayCount.jar AirlineDelayCount /airline /output1

5. Run Job 2 — Delays per Airport
hadoop jar RouteDelayAvg.jar RouteDelayAvg /airline /output2

6. View results
hdfs dfs -cat /output1/part-r-00000
hdfs dfs -cat /output2/part-r-00000

7. Stop Hadoop
stop-yarn.cmd
stop-dfs.cmd

Results
Most Delayed Airlines
AirlineDelayed FlightsSouthwest (WN)576,470Delta (DL)395,239American (AA)383,106
Most Delayed Airports
AirportAvg Delay (min)PPG - Pago Pago2,308BRW - Barrow, AK1,557MIA - Miami, FL1,473

