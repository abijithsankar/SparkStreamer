package com.spark.streamer

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a given time window.
 */
object PopularHashtags {
  
  
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Setting up the spark streaming context to stream tweets
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    
    // logging spams are handled in utilities file
    setupLogging()

    // Creating Dstream for the incoming tweets
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Extracting the tweets
    val statuses = tweets.map(status => status.getText())
    
    // Converting tweets to words and filters the hashtagged words alone
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    
    
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    
    // Performing mapreduce to count the occurance of each hashtagged word
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    
    
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
  
    
    // Sorting the results by the count values and is printed
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    
    sortedResults.print
    
    // Setting up the check point fdirectory to check anytime of the day
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
