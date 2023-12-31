--CREATE extension tablefunc;

-- QUANTIDADE GERAL DE MORTES POR ESTADO DO ANO 2020
SELECT DISTINCT estado, count(*) AS qtd
FROM obitos_covid 
GROUP BY 1;

-- QUANTIDADE DE MORTES POR SEXO DO ANO 2020
SELECT *
FROM crosstab (
	'SELECT DISTINCT estado, sexo, count(*) FROM obitos_covid GROUP BY estado, sexo',
	'SELECT DISTINCT sexo FROM obitos_covid ORDER BY sexo DESC'
) AS ("ESTADO" TEXT, "HOMENS" INT, "MULHERES" INT);

-- QUANTIDADE DE MORTES POR PESSOAS QUE CONTINHAM COMORBIDADE DO ANO 2020
SELECT *
FROM crosstab (
	'SELECT DISTINCT estado, comorbidade, count(*) FROM obitos_covid  GROUP BY comorbidade, estado',
	'SELECT DISTINCT comorbidade FROM obitos_covid ORDER BY comorbidade'
) AS ("ESTADO" TEXT, "NÃO POSSUIAM COMORBIDADE" INT, "POSSUIAM COMORBIDADE" INT);

-- QUANTIDADE DE MORTES POR MESES DO ANO 2020
SELECT DISTINCT 
	"ESTADO", 
	sum(COALESCE("JANEIRO", 0)) AS "JANEIRO",
	sum(COALESCE("FEVEREIRO", 0)) AS "FEVEREIRO",
	sum(COALESCE("MARÇO", 0)) AS "MARÇO",
	sum(COALESCE("ABRIL", 0)) AS "ABRIL",
	sum(COALESCE("MAIO", 0)) AS "MAIO",
	sum(COALESCE("JUNHO", 0)) AS "JUNHO",
	sum(COALESCE("JULHO", 0)) AS "JULHO",
	sum(COALESCE("AGOSTO", 0)) AS "AGOSTO",
	sum(COALESCE("SETEMBRO", 0)) AS "SETEMBRO",
	sum(COALESCE("OUTUBRO", 0)) AS "OUTUBRO",
	sum(COALESCE("NOVEMBRO", 0)) AS "NOVEMBRO",
	sum(COALESCE("DEZEMBRO", 0)) AS "DEZEMBRO"
FROM (
	SELECT *
FROM crosstab (
	'
		SELECT DISTINCT estado, EXTRACT(MONTH FROM data_obito) AS mes, count(*)
		FROM obitos_covid oc
		GROUP BY estado, EXTRACT(MONTH FROM data_obito)
		ORDER BY EXTRACT(MONTH FROM data_obito)',
	'SELECT DISTINCT numero_mes FROM meses_ano ma ORDER BY numero_mes'
) AS ("ESTADO" TEXT, "JANEIRO" INT, "FEVEREIRO" INT, "MARÇO" INT, "ABRIL" INT, "MAIO" INT, 
	  "JUNHO" INT, "JULHO" INT, "AGOSTO" INT, "SETEMBRO" INT, "OUTUBRO" INT, "NOVEMBRO" INT, "DEZEMBRO" INT)
) AS r
GROUP BY "ESTADO"
ORDER BY "ESTADO";
