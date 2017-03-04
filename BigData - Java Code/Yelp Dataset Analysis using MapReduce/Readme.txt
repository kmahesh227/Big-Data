The eclipse project is named Yelp.

The jar file is available in the target folder inside Yelp.



1.To execute Part 1
hadoop jar yelp.jar CountYelpBusiness /yelp/business/business.csv <HDFS-Output-Path>

2. To exeute Part 2
hadoop jar yelp.jar CountYelpRestaurants /yelp/business/business.csv <HDFS-Output-Path>

3. To exeute Part 3
hadoop jar yelp.jar TopTenZipYelp /yelp/business/business.csv <HDFS-Output-Path>

4. To exeute Part 4
hadoop jar yelp.jar TopRatedBusinessYelp /yelp/review/review.csv <HDFS-Output-Path>

5. To exeute Part 5
hadoop jar yelp.jar LowTenRatedYelpBusiness /yelp/review/review.csv <HDFS-Output-Path>



The datasets are already available in the HDFS in the below mentioned input paths of jar execution statements. In case, it's not available in the HDFS, 
it is also available in the Datasets folder, which is being uploaded.


