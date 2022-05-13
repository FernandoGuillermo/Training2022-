----EXPORTACIONES DE PERAS ARGENTINAS AL RESTO DEL MUNDO--
--primero hacemos un trabajo de exploración y limpieza de datos, luego generamos consultas para su análisis, 
--y en python graficamos.

--comienzo!

--Creamos la tabla con los nombres de las variables y su tipo y copiamos los datos a la tabla


CREATE TABLE expo_arg_peras (
       id serial PRIMARY KEY not null,
       año int,
       periodo varchar(10),         
       flujo_comercial char(20),
       exportador char(10),
       destino char(5),
       nomenc_aran char(6),
       producto text,
       expo_valor bigint,
       expo_cant bigint
);



COPY expo_arg_peras
FROM 'path/expo_peras_arg'
WITH (FORMAT CSV, HEADER); 

DROP TABLE expo_arg_peras


--actualiza formato de los datos de columna yyyymm a yyyy-mm-dd y cambiamos la columna "periodo" a date

UPDATE expo_arg_peras SET periodo=to_date(periodo,'YYYYMM')

ALTER TABLE expo_arg_peras ALTER COLUMN periodo TYPE date USING(periodo::date)

--borro columna id porque en principio no se iba a utilizar
ALTER TABLE expo_arg_peras DROP COLUMN id

--modificamos el texto de columnas 
UPDATE expo_arg_peras
SET flujo_comercial = 'exportacion'
WHERE flujo_comercial = 'Exports';

UPDATE expo_arg_peras
SET destino = 'mundo'
WHERE destino = 'World';

UPDATE expo_arg_peras
SET producto = 'peras frescas'
WHERE producto = 'Fruit, edible; pears, fresh';

UPDATE expo_arg_peras
SET nomenc_aran = '080830'
WHERE nomenc_aran = '80830';

UPDATE expo_arg_peras
SET exportador = 'argentina'
WHERE exportador = 'Argentina';


--los registros de la tabla son extraídos https://comtrade.un.org/ y comprenden 2012-201.
--agregamos a la tabla registros 2021-01-01 al 20-02-01 desde https://www.trademap.org/.
--Trademap tiene registros más actualizados pero la desventaja es que no facilita la extracción de datos
--sólo entender que hay dos fuentes de registros y que podría haber cierta diferencia en los registros.

--insertamos datos https://www.trademap.org/
INSERT INTO expo_arg_peras
(año, periodo, flujo_comercial, exportador, destino, nomenc_aran, producto, expo_valor, expo_cant)
VALUES 
(2021, '2021-01-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas', 10314000, 15231894),
(2021, '2021-02-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas', 40160000, 58547277),
(2021, '2021-03-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas', 42638000, 61593671), 
(2021, '2021-04-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas', 31647000, 46070552), 
(2021, '2021-05-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas', 28642000, 37809378), 
(2021, '2021-06-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas', 16377000, 21618630), 
(2021, '2021-07-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas', 11344000, 15279812), 
(2021, '2021-08-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas',  9582000, 14042785), 
(2021, '2021-09-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas',  7786000, 11407939), 
(2021, '2021-11-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas',  5745000,  7595977), 
(2021, '2021-12-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas',  4879000,  6794808), 
(2021, '2022-01-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas',  7132000,  9585965), 
(2021, '2022-02-01', 'exportacion', 'argentina', 'mundo', '080830', 'peras frescas', 44235000, 58547277); 

--Se ha encontrado que cuando hay "cero" exportaciones no existe registro de ese mes en la extracción de datos
--Por lo tanto hay que agregar el periodo a la tabla para continuar con la serie.

--en el código precedente simulo que falta el registro 2021-10-01 y a continuació borro 2020-10-01.
--esto para comprobar periodos faltantes
DELETE FROM expo_arg_peras WHERE periodo  = '2020-10-01' RETURNING *;

--verifico
SELECT*
FROM expo_arg_peras
ORDER BY periodo desc

--creo una tabla temporal con rango de resgistros 2012-01-01 a 2022-02-01.

CREATE TEMPORARY TABLE serie_periodo
AS
SELECT generate_series('2012-01-01'::date,
					   '2022-02-01'::date, 
					   '1 month')::date AS periodo_mens;					   
					   
--- acá hago un paréntesis y agrego a la tabla temporal columna ID como clave primaria para facilitar manipulación

ALTER TABLE serie_periodo ADD COLUMN ID SERIAL PRIMARY KEY;

--utilizo el join para unir la tabla mediante periodo, y buscamos las filas con valores NULL

SELECT*
FROM(
SELECT*
FROM serie_periodo s FULL OUTER JOIN expo_arg_peras e ON
s.periodo_mens = e.periodo
ORDER BY s.id)a
WHERE a.periodo IS NULL;

--creamos la tabla limpia para analizar los datos

CREATE TABLE expo_arg_peras_f AS SELECT*
FROM serie_periodo s FULL OUTER JOIN expo_arg_peras e ON
s.periodo_mens = e.periodo
ORDER BY s.id

---se completan los registros 2020-10-01 y 2021-10-01.

START TRANSACTION;

UPDATE expo_arg_peras_f
SET año= '2020', flujo_comercial='exportacion', exportador='argentina', destino='mundo', 
    nomenc_aran='080830', producto='peras frescas', expo_valor=10891290, expo_cant=15497748
WHERE id=106;	

UPDATE expo_arg_peras_f
SET año= '2021', flujo_comercial='exportacion', exportador='argentina', destino='mundo', 
    nomenc_aran='080830', producto='peras frescas', expo_valor=7043000, expo_cant=9429721
WHERE id=118

ALTER TABLE expo_arg_peras_f DROP COLUMN periodo 

COMMIT;



--queda la tabla lista para las siguiente etapa

SELECT*
FROM expo_arg_peras_f
ORDER BY id ASC

---------etapa de consultas para visualización--------
--cambio a tipo de dato 'numeric' las variables que voy a necesitar para realizar cálculos--

ALTER TABLE expo_arg_peras_f ALTER COLUMN expo_valor TYPE numeric
ALTER TABLE expo_arg_peras_f ALTER COLUMN expo_cant TYPE numeric

--ahora si!, tabla lista

--Exportaciones mensuales 2012-01-01 al 2022-02-01

SELECT periodo_mens, expo_valor
FROM expo_arg_peras_f
ORDER BY 1

--Exportaciones anuales 2012-2021

SELECT date_part('year',periodo_mens) AS año_expo, SUM(expo_valor) AS valor
FROM expo_arg_peras_f
WHERE periodo_mens < '2022-01-01'
GROUP BY 1
ORDER BY 1


-- total y porcentaje del total   
SELECT periodo_mens, expo_valor
,SUM(expo_valor) OVER (PARTITION BY date_part('year',periodo_mens)) AS expo_anual
,expo_valor * 100 / SUM(expo_valor) OVER (PARTITION BY date_part('year',periodo_mens )) AS porc_del_total
FROM expo_arg_peras_f 
WHERE periodo_mens < '2022-01-01'
ORDER BY 1
;


--Indexación año base 2012

SELECT año_expo, valor
,(valor / first_value(valor) over (order by año_expo) - 1) * 100 as var_porc_indice
FROM
(
SELECT date_part('year',periodo_mens) as año_expo ,sum(expo_valor) as valor
FROM expo_arg_peras_f
WHERE periodo_mens < '2022-01-01'
GROUP BY 1
ORDER BY 1
)a
;	

---calculamos la media móvil tomando en cuanta 11 meses precedentes y mes actual

SELECT 
periodo_mens,
expo_valor,
avg(expo_valor) over (order by periodo_mens rows between 11 preceding and current row) as media_movil,
count(expo_valor) over (order by periodo_mens rows between 11 preceding and current row) as registros
FROM expo_arg_peras_f
;

----valores mensuales acumulados
SELECT
periodo_mens,
expo_valor,
sum(expo_valor) over (partition by date_part('year',periodo_mens) order by periodo_mens) as valor_acum
FROM expo_arg_peras_f
;

----Comparación entre periodos

-----variacion respecto al año anterior
SELECT 
año_expo,
valor,
lag(valor) over (order by año_expo) as año_prev_expo,
(valor / lag(valor) over (order by año_expo) -1) * 100 as variac_año_prev
FROM
(
SELECT date_part('year',periodo_mens) as año_expo,
sum(expo_valor) as valor
FROM expo_arg_peras_f
WHERE periodo_mens < '2022-01-01'
GROUP BY 1
) a
;


---variación absoluta y relativa interanual.
periodo_mens, 
expo_valor,
expo_valor - lag(expo_valor) over (partition by date_part('month', periodo_mens) order by periodo_mens) as dif_absoluta,
(expo_valor / lag(expo_valor) over (partition by date_part('month',periodo_mens) order by periodo_mens) - 1) * 100 as dif_porcentual
FROM expo_arg_peras_f


----compara en el mismo gráfico las ventas de los últimos 3 años

SELECT date_part('month',periodo_mens) as num_mes,
to_char(periodo_mens,'Month') as mes,
max(case when date_part('year',periodo_mens) = 2019 then expo_valor end) as expo_2019,
max(case when date_part('year',periodo_mens) = 2020 then expo_valor end) as expo_2020,
max(case when date_part('year',periodo_mens) = 2021 then expo_valor end) as expo_2021
FROM expo_arg_peras_f
GROUP BY 1,2
ORDER BY 1
;

--se calculan las variaciones como el cociente del periodo actual y la media de los 3 periodos precedentes.
SELECT 
periodo_mens, 
expo_valor,
expo_valor / avg(expo_valor) over 
(partition by date_part('month',periodo_mens) order by periodo_mens rows between 3 preceding and 1 preceding) as var_porce_3_prev
FROM expo_arg_peras_f
;

--Un detalle conceptual relevante a la hora utilizar las media móvil es que, mientras más amplia sea la ventana 
--mayor es el suavizado por lo tanto se pierde sensibilidad ante variaciones eventuales a corto plazo.
--Por ello es conveniente ubicar las ventanas de tal forma que nos lleve a realizar una mejor estimación cuando 
--existen patrones de estacionalidad como en el código precedente.

--La conclusión se completa en el archivo "expo_arg_peras_plots"

--Bibliografía :Tanimura C. (2021). Cap.3. SQL for Data Analysis.O’Reilly Media, Inc
--              Debarros A.(2ed)(2022). Practical SQL.No Starch Press, Inc.
--              Zhao A.(4ed)(2021.). SQL Pocket Guide. O’Reilly Media, Inc
--Otras fuentes: internet













 













							 

                       -