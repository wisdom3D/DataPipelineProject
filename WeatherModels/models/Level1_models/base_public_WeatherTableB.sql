with source as (
      select * from {{ source('public', 'WeatherTableB') }}
),
renamed as (
    select
        {{ adapter.quote("date") }},
        {{ adapter.quote("Pays") }},
        {{ adapter.quote("Ville") }},
        {{ adapter.quote("Latitude") }},
        {{ adapter.quote("Longitude") }},
        {{ adapter.quote("Température_2m") }},
        {{ adapter.quote("Humidité_relative_2m") }},
        {{ adapter.quote("Point_de_rosée_2m") }},
        {{ adapter.quote("Température_apparente") }},
        {{ adapter.quote("Probabilité_de_précipitation") }},
        {{ adapter.quote("Précipitations") }},
        {{ adapter.quote("Pluie") }},
        {{ adapter.quote("Code_météorologique") }},
        {{ adapter.quote("Pression_de_surface") }},
        {{ adapter.quote("Couverture_nuageuse_Moyenne") }},
        {{ adapter.quote("Visibilité") }},
        {{ adapter.quote("Évapotranspiration") }},
        {{ adapter.quote("Vitesse_du_vent_10m") }},
        {{ adapter.quote("Direction_du_vent_10m") }},
        {{ adapter.quote("Température_du_sol_6cm") }},
        {{ adapter.quote("Humidité_du_sol_3cm") }}

    from source
)
select * from renamed
  