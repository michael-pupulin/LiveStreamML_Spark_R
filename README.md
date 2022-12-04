# LiveStreamML_Spark_R
Machine learning on streaming data with Scala and Spark -- visualized with R Shiny. CSV files that are uploaded into a StreamInput folder are automatically read into an Apache Spark structured stream. The stream runs a previously-trained logistic regression model to predict whether a shipped package will be late or on-time. The results are written to an output folder which is continuously read into an R Shiny web app for real-time visualization of the stream.



![alt text](https://github.com/michael-pupulin/LiveStreamML_Spark_R/blob/main/ShinySparkPic.png?raw=true)
