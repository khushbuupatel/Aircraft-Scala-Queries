package aircraft

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.DateType

import scala.collection.mutable._

/**
 * Class Itinerary for question 5 to compute common flights between 2 passengers and find the from and to date
 * @param flightId
 * @param date
 */
case class Itinerary(val flightId: Int, val date: String) {
  override def equals(o: Any) = o match {
    case that: Itinerary => that.flightId == this.flightId
    case _ => false
  }
  override def hashCode = flightId.hashCode
}

object ProcessData {

  def main(args: Array[String]) = {

    // create Spark Session on local host
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Aircraft queries")
      .getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    // for implicit conversions (toDf())
    import spark.implicits._

    // read the flightData.csv and passengers.csv file
    var flights = spark.read.format("csv").option("header", "true").load("flightData.csv")
    val passengers = spark.read.format("csv").option("header", "true").load("passengers.csv")

    // create views for both the csv to query data
    flights.createOrReplaceTempView("flightsView")
    passengers.createOrReplaceTempView("passengersView")

    // QUE 1: calculate total no of flights for each month
    val ans1 = spark.sql("select MONTH(date) as Month, COUNT(*) as NoOfFlights from flightsView group by month order by month")
    ans1.show()

    // QUE 2: calculate top 100 frequent flyers
    val ans2 = spark.sql("select flightsView.passengerId as PassengerId, count(*) as NumberOfFlights, firstName as FirstName, lastName as LastName from passengersView inner join flightsView on passengersView.passengerId = flightsView.passengerId group by flightsView.passengerId, firstName, lastName order by NumberOfFlights desc limit 100")
    ans2.show()

    // QUE 3: calculate total no of countries the passenger has been without being to "uk"
    val ans3 = countryCountExceptUK(flights).toSeq.toDF("Passenger Id", "Longest Run")
    ans3.show()

    // QUE 4: find passengers who have been together on more than 3 flights
    val csv_data_4 = flown3AndMore(flights)

    // write the output to csv file
    val outputFile4 = new BufferedWriter(new FileWriter("Output4.csv"))
    outputFile4.write("Passenger 1 Id, Passenger 2 Id, No of Flights Together\n" + csv_data_4)
    outputFile4.close()

    // QUE 5: the datatype of date column in flightData.csv to DateType for filtering
    flights = flights.withColumn("date", col("date").cast(DateType))

    // find the passengers that have travelled in same flight in defined range
    val csv_data_5 = flownTogether(5, "2017-05-15", "2017-07-15", flights)

    // write the data to the csv file
    val outputFile5 = new BufferedWriter(new FileWriter("Output5.csv"))
    outputFile5.write("Passenger 1 Id, Passenger 2 Id, No of Flights Together, From, To\n" + csv_data_5)
    outputFile5.close()
  }

  /**
   * This function calculates the Longest Run of all the passengers without being in "uk"
   *
   * @param flights dataframe
   * @return HashMap(Passenger Id, Longest Run)
   */
  def countryCountExceptUK(flights: DataFrame): HashMap[Int, Int] ={

    // hashmap to store p_id along with the Set of countries they have been to
    val passengers_country_hashmap = new HashMap[Int, Set[String]]

    // array to store p_id along with their countries count
    val countries_count_hashmap = new HashMap[Int, Int]

    // loop through flights data frame
    for( row <- flights.rdd.collect){

      // extract relevant column values
      val row_data = row.mkString(",").split(",")
      val p_id = row_data(0).toInt
      var from = row_data(2)
      var to = row_data(3)

      // get or initialize the passenger country set and country count
      var passenger_country_set = passengers_country_hashmap.getOrElse(p_id, Set())
      val country_count = countries_count_hashmap.getOrElse(p_id, 0)

      // if either "from" or "to" contains "uk" then update the countries_count_hashmap for that p_id
      if(from == "uk" | to == "uk"){

        // get current country count from p_id country array set size
        val currentCount = passenger_country_set.size

        // update the countries_count if current is greater then previous
        if(currentCount > country_count)
          countries_count_hashmap.put(p_id, currentCount)

        // once "uk" is encountered clear the Set for that passenger to start again
        passenger_country_set = Set()

      }else{
        // if neither "from" or "to" contains "uk" then just add the values to the set
        passenger_country_set += from
        passenger_country_set += to
      }

      // put the update country set into the hashmap for that p_id
      passengers_country_hashmap.put(p_id, passenger_country_set)
    }

    // one more pass to compute the final countries count values to handle cases where the country set count wont' be updated
    // as "uk" won't be present in the entire journey or "uk" won't be present in the last trip
    passengers_country_hashmap.foreach{
      case (p_id, country_set) => {

        // get current count from the set and previous count from the countries_count hashmap
        val current_count = country_set.size
        val previous_count = countries_count_hashmap.getOrElse(p_id, 0)

        if (current_count > previous_count)
          countries_count_hashmap.put(p_id, current_count)
      }
    }
    return countries_count_hashmap
  }

  /**
   * This function finds the passengers who have been on the same flight for more than 3 times
   *
   * @param flights
   * @return csv_data containing Passenger1 Id, Passenger2 Id, No of Flights
   */
  def flown3AndMore(flights: DataFrame): String ={

    // hashmap to store p_id and the Set of flight id in which they have travelled
    val passengers_flights_hashmap = new HashMap[Int, Set[Int]]
    var csv_data: String = ""

    for( row <- flights.rdd.collect){
      // loop through flights df and get relevant cols data
      val row_data = row.mkString(",").split(",")
      val p_id = row_data(0).toInt
      var flight_id = row_data(1).toInt

      // get or initialize the passenger flight set and add new flight id to it
      var p_flight_set = passengers_flights_hashmap.getOrElse(p_id, Set())
      p_flight_set += flight_id

      // update the flights hashmap for that p_id
      passengers_flights_hashmap.put(p_id, p_flight_set)
    }

    // set to store the traversed p_id so that they are not included in the next iteration of loop
    val traversed_p_ids : Set[Int] = Set()

    passengers_flights_hashmap.foreach{ case (p_id1, flight_set1) => {
      passengers_flights_hashmap.foreach{ case (p_id2, flight_set2) => (
          if((p_id1 != p_id2) & !traversed_p_ids.contains(p_id2)){

            // check if the common flights between both p_ids is >3
            if(flight_set1.intersect(flight_set2).size > 3) {
              csv_data += p_id1 + "," + p_id2 + "," + flight_set1.intersect(flight_set2).size + "\n"
            }
        })}

      // add p_id1 to traversed
      traversed_p_ids += p_id1
    }}
    return csv_data
  }

  /**
   * This function finds the passengers that have travelled together in n No of flights between a time range (from and to)
   * @param atLeastNTimes Minimum no of flight(s) travelled together
   * @param from start date
   * @param to   end date
   * @param flights flights df
   *
   * @return csv_data "," separated Passenger1 Id, Passenger2 Id, No of flights together, from, to
   */
  def flownTogether(atLeastNTimes: Int, from: String, to: String, flights: DataFrame) :String = {

    // filter the data for the defined time interval
    val filtered_data = flights.filter(to_date(flights("date"),"yyyyMMdd").between(from, to))

    // hashmap to store p_id and the Itinerary set (flight_id, date) in which they have travelled
    val passengers_flights_hashmap = new HashMap[Int, Set[Itinerary]]
    var csv_data: String = ""

    for( row <- filtered_data.rdd.collect){
      // loop through flights df and get relevant cols data
      val row_data = row.mkString(",").split(",")
      val p_id = row_data(0).toInt
      val flight_id = row_data(1).toInt
      val flight_date = row_data(4)

      // get or initialize the passenger flight set and add new flight id to it
      var itinerary_set = passengers_flights_hashmap.getOrElse(p_id, Set())
      itinerary_set += new Itinerary(flight_id, flight_date)

      // update the flights hashmap for that p_id
      passengers_flights_hashmap.put(p_id, itinerary_set)
    }

    // set to store the traversed p_id so that they are not included in the next iteration of loop
    val traversed_p_ids : Set[Int] = Set()

    passengers_flights_hashmap.foreach{ case (p_id1, itinerary_set1) => {
      passengers_flights_hashmap.foreach{ case (p_id2, itinerary_set2) => (

        if((p_id1 != p_id2) & !traversed_p_ids.contains(p_id2)){

          val common_itn = itinerary_set1.intersect(itinerary_set2)
          var from_date: String = ""
          var to_date: String = ""

          // check if the common flights between both p_ids is >3
          if(common_itn.size > atLeastNTimes) {

            // compute minimum date as from_date and max as to_date
            common_itn.foreach(itinerary => {
              if(from_date.isEmpty() | from_date > itinerary.date)
                from_date = itinerary.date
              if(to_date.isEmpty() | to_date < itinerary.date)
                to_date = itinerary.date
            })

            // append csv data
            csv_data += p_id1 + "," + p_id2 + "," + common_itn.size + "," + from_date + "," + to_date + "\n"
          }
        })}

      // add p_id1 to traversed
      traversed_p_ids += p_id1
    }}
    return csv_data
  }
}
