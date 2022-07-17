\C 'CITY WITH THE HIGHEST TEMPERATURE'
SELECT city, temperature, date
FROM weather_forecast 
ORDER BY temperature DESC
LIMIT 1;

\C 'CITY WITH THE LOWEST TEMPERATURE'
SELECT city, temperature, date
FROM weather_forecast 
ORDER BY temperature ASC
LIMIT 1;