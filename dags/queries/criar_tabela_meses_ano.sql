CREATE TABLE if not exists meses_ano (
	id serial PRIMARY KEY,
	numero_mes int unique,
	nome_mes varchar unique
)