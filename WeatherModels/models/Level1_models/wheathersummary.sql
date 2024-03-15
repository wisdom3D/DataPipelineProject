with wheatherDataSummary AS
(
SELECT "Pays", "Ville", sum("Humidité_relative_2m") Humidité_relative_2m, 
sum("Température_apparente") Température_apparente,
sum("Probabilité_de_précipitation") Probabilité_de_précipitation, 
sum("Précipitations") Précipitations
FROM {{ source('public', 'WeatherTableB') }} 
group by "Pays", "Ville"
)
SELECT * FROM wheatherDataSummary

