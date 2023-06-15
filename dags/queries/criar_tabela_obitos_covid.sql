create table if not exists obitos_covid (
    id serial primary key,
    sexo varchar,
    idade int,
    municipio varchar,
    estado varchar,
    data_obito date,
    comorbidade varchar
)