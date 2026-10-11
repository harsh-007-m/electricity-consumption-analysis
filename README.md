# Electricity Consumption Analysis using Hadoop MapReduce
---
An analysis of the consumption of electricity in Bareilly, Uttar Pradesh in 2020 and 2021. 

## Overview
A smart meter is a digital device that records your energy consumption and communicates the data back to your utility provider in real-time. Traditional meters only measure electricity usage and require manual readings. But smart energy meters offer an array of additional features which can lead to substantial savings in both money and electricity. With this project, I have analysed the given data to produce actionable insights about the smart meters present in Bareilly. 

### 1. Dataset
The dataset was procured from Kaggle titled "Smart Meter Data: Mathura & Bareilly" which contained the data from Mathura and Bareilly from 2019 and 2021. In this project, I have specifically used the csv file of Bareilly 2020 which contained ~66L records. It contains various attributes that contribute to the better understanding of the functioning of the smart meters. 
Size of Dataset :308.4 MB
Format: CSV
- x_Timestamp : the time at which the reading was taken. The smart meter takes a reading every 3 minutes.
- t_kWh: the amount of Energy Consumed
- z_Avg Volatge: The average Coltage
- z_Avg Current: The average Current
- y_Freq: Frequency (Hz)
- meter: denotes the unique ID of the meter

### 2.Project Structure
smart_meter_project/
├── src/
│   └── ... (Java source files)
├── .gitignore
└── README.md
└── Presentation with insights

### 3.Objectives
- Analyse electricity consumption at hourly, monthly, and yearly levels.
- Generate aggregated consumption summaries using Hadoop MapReduce.
- Examine hourly electricity load patterns.
- Implement and explore map-side and reduce-side join approaches.
- Understand distributed processing of large datasets using Hadoop.

### 4.Tech Stack
Language Used: Java
Distributed processing: Apache Hadoop 3.4.2
Java runtime: OpenJDK 17
Storage: Hadoop Distributed File System (HDFS)
Environment: Ubuntu / Windows Subsystem for Linux (WSL)

### 5. MapReduce Jobs and purpose
| Job | Purpose |
|-----|---------|
|Monthly Consumption| Groups consumption readings by hour of the day and calculates the total for each hour.|
|Hourly Consumption| Aggregates electricity consumption by month|
|Meter Wise Consumption| Analyses consumption by each individual meter|
|Meter Electrical Profile | Analyses electrical parameters noted by the meter |
| Map Side Join | Implements a join strategy that performs matching during the map stage, subject to the data and implementation requirements. |
| Reduce Side Join | Implements a join strategy that brings matching records together through the shuffle and reduce stages.|


### Results and Findings

After a lot of investigation, we have found that:
1. Monthly consumption has peaked in June 2020, which was recorded as the third hottest June on record. The energy consumed during that month alone was 14,669kWh, which leads us to believe that urban households took up various cooling methods which in turn raised the consumption of electricity. The lowest was in November 2020, a decline by 3 times as compared to June. We note that <u> Seasonal changes affect electricity consumption patterns </u>

2. Hourly consumption follows a residential trend rather than an industrial one. Early morning hours record few of the lowest consumptions of electricity which slowly increases and takes a massive shift around 9 - 11 PM, indicating that recreational activities such as watching the news, watching a movie, etc, usually occur in this time frame when the family comes together.

3. Meter Wise Consumption shows us a rather interesting set of insights.
   - We find that, one smart meter in particular has some unusual behavior. BR20 draws 1.4A of current but records only 143kWh for the whole year. While the current consumption is rather normal for any household, the low energy levels for a whole year might indicate that the smart meter might not be working properly and field investigations must be conducted so as to rectify this situation.
   - We also find that the top 5 meters consume ~27% of all electricity supplied to Bareilly. The top 10 meters use 48%. This indicates that half of the load comes from one-fifth of the connections. This could be due to apartments, residential areas that are placed close together or that are there in the same locality which contribute to this finding.

4. Meter Electrical Profile
   - 9 meters run below the safe limit of 216V, and only 4 run above 243V. The worst is BR35 at just 184V. Adiitionally, BR32 to BR48 are almost all low-voltage, and several of them are heavy users. Meters close together having the same problem points to one overloaded transformer or feeder, not 9 separate faults.
   - Supply voltage is stable for most meters. 33 of 46 are within the safe range. Current tells us who the heavy users are. Meters with high current (BR51, BR38, BR26, BR24, BR06) are the same ones with the highest consumption.

5. We ran both the map-side join and the reduce-side join to compare 2020 and 2021.

- First, both gave exactly the same results. This confirms the join was done correctly.

- Second, they differ in how they work. Reduce-side join sends data across the network through the shuffle step to bring matching meters together. Map-side join skips the shuffle because the small dataset is loaded into memory on every node. In our case it is only 46 rows, so map-side join is the better fit.

- Now the results. Total usage across the matched meters dropped by about 21% from 2020 to 2021. But this is not a real drop in demand. Most meters stayed almost the same. The fall came from a few meters that collapsed by over 90%, such as BR38, BR08 and BR02. At the same time, BR19 and BR44 went up four to five times. These look like disconnections, new connections or missing data, not people actually saving electricity.

## Conclusion 

On the technical side, we stored a 308 MB file with over 66 lakh meter readings in HDFS. It was split into 3 blocks and processed in parallel on 3 worker nodes using MapReduce, with a combiner reducing the network traffic. The results reconciled perfectly, which shows the system works correctly.

On the data side, we found four main things:

Summer usage is three times winter usage.
The daily peak is at night, around 10 PM.
A small group of meters uses a large share of the power.
Voltage problems are concentrated in one group of meters, mostly low voltage.

On the join side, map-side and reduce-side joins matched exactly, and the year-on-year comparison showed that a headline number like ‘21% drop’ can be misleading unless you look at individual meters.

The main takeaway is that distributed processing let us handle a large dataset reliably, and if the data grows, we simply add more nodes without changing the code. The same approach can be applied to the full dataset or to other regions
