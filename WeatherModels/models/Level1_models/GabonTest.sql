with Gabon as (
 SELECT * from {{ref('wheathersummary')}}
 where "Pays" = 'Gabon'
)
SELECT * from Gabon