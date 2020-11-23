# Aircraft-Scala-Queries

### About
A scala/spark program which extracts relevant information in desired formats from 2 different .csv file named passengers.csv and flighData.csv.

### Pre-requisites :rotating_light:
- Developed using **Intellij IDEA IDE**
- **Scala Plugin** `>= v2.11.11`
- **JDK** `>= v14.0.2`

### What's in there?
The ProcessData.scala generates below output for the following questions:

1.  Find the total number of flights for each month.

|Month|NoOfFlights|
+-----+-----------+
|    1|       9700|
|    2|       7300|
|    3|       8200|
|    4|       9200|
|    5|       9200|
|    6|       7100|
|    7|       8700|
|    8|       7600|
|    9|       8500|
|   10|       7600|
|   11|       7500|
|   12|       9400|
+-----+-----------+


2.  Find the names of the 100 most frequent flyers.
+-----------+---------------+---------+--------+
|PassengerId|NumberOfFlights|FirstName|LastName|
+-----------+---------------+---------+--------+
|       2068|             32|  Yolande|    Pete|
|       1677|             27|Katherina|Vasiliki|
|       4827|             27|    Jaime|   Renay|
|       8961|             26|    Ginny|   Clara|
|       3173|             26| Sunshine|   Scott|
|       2857|             25|      Son| Ginette|
|        288|             25|   Pamila|   Mavis|
|        917|             25|   Anisha|  Alaine|
|       8363|             25|   Branda|  Kimiko|
|       5096|             25|   Blythe|    Hyon|
|       6084|             25|     Cole|  Sharyl|
|        760|             25|   Vernia|     Mui|
|       5867|             25|    Luise| Raymond| ...

3.  Find the greatest number of countries a passenger has been in without being in the UK. For example, if the countries a passenger was in were UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries.
+------------+-----------+
|Passenger Id|Longest Run|
+------------+-----------+
|       12572|          4|
|        3672|          9|
|        6938|          9|
|        6220|          7|
|       12886|          5|
|        3331|          7|
|        5879|          5|
|        2990|          2|
|        5538|          6|
|        5197|          5| ...

4. Find the passengers who have been on more than 3 flights together.
+--------------+---------------+-----------------------+
|Passenger 1 Id| Passenger 2 Id| No of Flights Together|
+--------------+---------------+-----------------------+
|         12572|          12506|                      4|
|         12572|          12551|                      4|
|         12572|          12583|                      4|
|         12572|          12541|                      4|
|         12572|          12517|                      4|
|         12572|          12600|                      4|
|         12572|          12576|                      4|
|         12572|          12585|                      4|
|          3672|            173|                      4|
|          3672|           3609|                      5| ...

5. Find the passengers who have been on more than N flights together within the range (from,to).
+--------------+---------------+-----------------------+----------+----------+
|Passenger 1 Id| Passenger 2 Id| No of Flights Together|      From|        To|
+--------------+---------------+-----------------------+----------+----------+
|          2012|           2024|                      6|2017-05-19|2017-07-11|
|          2012|           3002|                      6|2017-05-19|2017-07-11|
|          2012|           8729|                      6|2017-05-19|2017-07-11|
|          2012|           8262|                      6|2017-05-19|2017-07-11|
|          2012|           3187|                      6|2017-05-19|2017-07-11|
|          2012|           2945|                      6|2017-05-19|2017-07-11|
|          2012|           3028|                      6|2017-05-19|2017-07-11|
|          2012|           3064|                      6|2017-05-19|2017-07-11| ...
