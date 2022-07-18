DROP VIEW IF EXISTS stockholm_weather_forecast;
DROP TABLE IF EXISTS weather_symbol;

CREATE TABLE weather_symbol (
    symbol_id  INTEGER PRIMARY KEY, 
    symbol_meaning VARCHAR(30));

INSERT INTO weather_symbol (symbol_id, symbol_meaning)
VALUES (1,	'Clear sky'),
       (2,	'Nearly clear sky'),
       (3,	'Variable cloudiness'),
       (4,	'Halfclear sky'),
       (5,	'Cloudy sky'),
       (6,	'Overcast'),
       (7,	'Fog'),
       (8,	'Light rain showers'),
       (9,	'Moderate rain showers'),
       (10,	'Heavy rain showers'),
       (11,	'Thunderstorm'),
       (12,	'Light sleet showers'),
       (13,	'Moderate sleet showers'),
       (14,	'Heavy sleet showers'),
       (15,	'Light snow showers'),
       (16,	'Moderate snow showers'),
       (17,	'Heavy snow showers'),
       (18,	'Light rain'),
       (19,	'Moderate rain'),
       (20,	'Heavy rain'),
       (21,	'Thunder'),
       (22,	'Light sleet'),
       (23,	'Moderate sleet'),
       (24,	'Heavy sleet'),
       (25,	'Light snowfall'),
       (26,	'Moderate snowfall'),
       (27,	'Heavy snowfall');

CREATE VIEW stockholm_weather_forecast AS
SELECT date_part('day', wf.date) AS forecast_date, 
    max(wf.city) AS city, 
    max(ws.symbol_meaning) AS weather_forecast 
FROM weather_forecast wf
JOIN weather_symbol ws ON symbol_id = weather_symbol
WHERE wf.city = 'Stockholm'
GROUP BY forecast_date;

SELECT * FROM stockholm_weather_forecast;