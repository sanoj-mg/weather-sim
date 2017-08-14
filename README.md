Weather Data Generator
=====================

This application generates weather observations for few cities around Australia. It uses the past weather observations from [www.bom.gov.au](http://www.bom.gov.au/) to build a model to predict weather conditions for a location on a given time.

Below are the predefined locations : 

1. Sydney     
2. Melbourne  
3. Brisbane   
4. Perth      
5. Adelaide   
6. Newcastle  
7. Canberra   
8. Wollongong 
9. Hobart     
10. Darwin     


Building and Running
--------

#### Build command:

    mvn compile
    
#### Build with test
    
    mvn test

#### Running:
    
    mvn exec:java
    
__Note__: 
    Apache Spark 1.6.2 is used internally in standalone mode to process the data and train linear regression model.
    
For the prompt 'Download weather data from www.bom.gov.au (y/n)?', enter 'y' if you want to download weather data from www.bom.gov.au else it will use the data from the file system.

Once the data is downloaded and cleansed using spark RDDs, separate models will be built using MLLib. For generating the weather data, a random location from the above list and a random time from last 1 year will be picked. The trained linear regression models are then used to predict temperature, pressure and humidity.  

The weather data is generated in below format: 

    Brisbane|-27.47,153.03|2017-04-09T03:20Z|Sunny|22.67|908.4|56
    Newcastle|-32.93,151.78|2017-05-14T12:20Z|Rain|26.13|905.9|36
    Darwin|-12.46,130.84|2016-10-28T10:25Z|Rain|33.08|902.2|33
    Sydney|-33.87,151.21|2016-10-31T05:32Z|Sunny|20.18|907.4|46
    Perth|-31.94,115.96|2016-10-09T20:54Z|Sunny|15.12|909.6|56
    Wollongong|-34.43,150.89|2017-01-21T16:40Z|Rain|13.61|913.3|76
    Brisbane|-27.47,153.03|2017-04-10T22:12Z|Sunny|19.20|910.1|65
    Brisbane|-27.47,153.03|2017-03-10T21:02Z|Sunny|19.47|910.3|66


##### Schema of weather data downloaded from [www.bom.gov.au](http://www.bom.gov.au/) :


    Index|Field Name                          | Required? 
    :----|:-----------------------------------|:--------- 
    0    | Unused                             |       
    1    | Date                               | Yes         
    2    | Minimum temperature (°C)           | Yes         
    3    | Maximum temperature (°C)           | Yes         
    4    | Rainfall (mm)                      | Yes         
    5    | Evaporation (mm)                   |          
    6    | Sunshine (hours)                   | Yes         
    7    | Direction of maximum wind gust     |          
    8    | Speed of maximum wind gust (km/h)  |          
    9    | Time of maximum wind gust          |          
    10   | 9am Temperature (°C)               | Yes        
    11   | 9am relative humidity (%)          | Yes        
    12   | 9am cloud amount (oktas)           |          
    13   | 9am wind direction                 |          
    14   | 9am wind speed (km/h)              |          
    15   | 9am MSL pressure (hPa)             | Yes      
    16   | 3pm Temperature (°C)               | Yes       
    17   | 3pm relative humidity (%)          | Yes       
    18   | 3pm cloud amount (oktas)           |          
    19   | 3pm wind direction                 |          
    20   | 3pm wind speed (km/h)              |          
    21   | 3pm MSL pressure (hPa)             | Yes         
    


#### Features used to build linear regression model :
1. month
2. hour
3. latitude
4. longitude
5. altitude

#### Temperature observations used to train the model : 
1. Minimum temperature (°C)          
2. Maximum temperature (°C)  
3. 9am Temperature (°C)    
4. 3pm Temperature (°C)

#### Pressure observations used to train the model :
1. 9am MSL pressure (hPa)
2. 3pm MSL pressure (hPa)

#### Humidity observations used to train the model :
1. 9am relative humidity (%) 
2. 3pm relative humidity (%)  



#### TODO:


1. Buid model to predict weather conditions(Sunny, Rainy, Snow) using observations such as sunshine hours, rainfall and cloud amount
2. Add more test cases for RDD transformations and ML calculations




