with Benin as (
 SELECT * from {{ref('wheathersummary')}}
 where "Pays" = 'Benin'
)
SELECT * from Benin