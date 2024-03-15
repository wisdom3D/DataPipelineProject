with Togo as (
 SELECT * from {{ref('wheathersummary')}}
 where "Pays" = 'Togo'
)
SELECT * from Togo