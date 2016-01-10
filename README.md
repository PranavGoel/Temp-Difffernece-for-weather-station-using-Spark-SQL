# Temp-Difffernece-for-weather-station-using-Spark-SQL
Temp-Difffernece-for-weather-station-using-Spark-SQL
will look at historical weather data from the Global Historical Climatology Network. Their archives contain (among other things) lists of weather observations from various weather stations formatted as CSV files like this:

USC00242347,20120105,TMAX,133,,I,K,0800
USC00242347,20120105,TMIN,-56,,,K,0800
USC00242347,20120105,TOBS,-56,,,K,0800
US1SCPC0003,20120105,PRCP,0,,,N,
US1SCPC0003,20120105,SNOW,0,,,N,
NOE00133566,20120105,TMAX,28,,,E,
NOE00133566,20120105,TMIN,-26,,,E,
NOE00133566,20120105,PRCP,57,,,E,
NOE00133566,20120105,SNWD,0,,,E,

Trying to find :

what weather station had the largest temperature difference on each day? That is, where was the largest difference between TMAX and TMIN?

