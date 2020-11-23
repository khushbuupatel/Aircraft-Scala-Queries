# Aircraft-Spark-Queries

## About
A Scala/Spark program which extracts relevant information in desired formats from 2 different .csv file named passengers.csv and flighData.csv.

## Pre-requisites :rotating_light:
- Developed using **Intellij IDEA IDE**
- **Scala Plugin** `>= v2.11.11`
- **JDK** `>= v14.0.2`  

## What's in there?
The ProcessData.scala generates below output for the following questions:

### 1.  Find the total number of flights for each month.  
![Table 1](/OutputImages/Output1.PNG)


### 2.  Find the names of the 100 most frequent flyers.  
![Table 2](/OutputImages/Output2.PNG)

### 3.  Find the greatest number of countries a passenger has been in without being in the UK. For example, if the countries a passenger was in were UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries.  
![Table 3](/OutputImages/Output3.PNG)

### 4. Find the passengers who have been on more than 3 flights together.  
![Table 4](/OutputImages/Output4.PNG)

### 5. Find the passengers who have been on more than N flights together within the range (from,to).  
![Table 5](/OutputImages/Output5.PNG)
