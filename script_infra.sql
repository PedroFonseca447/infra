--solução 1 lembra de executar tudo junto
--Listar o nome completo (primeiro nome + último nome), a idade e a cidade de todos os passageiros do sexo feminino (sex='w')
--com mais de 40 anos, residentes no país 'BRAZIL'. [resposta sugerida = 141 linhas]
--NAO OTIMIZADO
CREATE TABLE AIR_PASSENGERS AS SELECT * FROM arruda.AIR_PASSENGERS;
CREATE TABLE AIR_PASSENGERS_DETAILS AS SELECT * FROM arruda.AIR_PASSENGERS_DETAILS;

SELECT
CONCAT(CONCAT(P.firstname,' '), P.lastname) AS Nome
, trunc(months_between(sysdate,PD.birthdate)/12) AS idade
, PD.city AS Cidade
FROM air_passengers_details PD
INNER JOIN air_passengers P ON P.passenger_id = PD.passenger_id
WHERE 1=1
 AND PD.country = 'BRAZIL'
 AND PD.birthdate <= ADD_MONTHS(SYSDATE,-40*12)
 AND PD.sex = 'w'
;

--otimizado 1


-----
--Cluster de b-tree
-----

CREATE CLUSTER tun_clb (
    passenger_id numeric(12)
)
INDEX
SIZE 160;
-- Criar o índice (B-Tree*) do cluster
CREATE INDEX idx_tun_clb ON CLUSTER tun_clb;
-- Criar as tabelas no cluster
CREATE TABLE air_passengers CLUSTER tun_clb(passenger_id) AS SELECT * FROM arruda.air_passengers
CREATE TABLE air_passengers_details CLUSTER tun_clb(passenger_id) AS SELECT * FROM arruda.air_passengers_details

-- Calcular o tamanho do cluster

 SELECT
    segment_name,
    SUM(bytes)/1024 KB
FROM
    user_segments
WHERE
    segment_type='CLUSTER'
    AND segment_name = 'TUN_CLB'
GROUP BY
    segment_name
ORDER BY
    2 DESC;

-- Calcular a altura do índice B-Tree* do cluster
 
SELECT
    index_name,
FROM
    user_indexes,
    blevel+1 AS index_height,
    leaf_blocks
WHERE
    index_name IN ('IDX_TUN_CLB');


--CRIAR as pks das tabelas

ALTER TABLE air_passengers
ADD CONSTRAINT pk_air_passengers
PRIMARY KEY (passenger_id);

ALTER TABLE air_passengers_details
ADD CONSTRAINT pk_air_passengers_details
PRIMARY KEY (passenger_id);

CREATE INDEX idx_pd_bithdate
 ON air_passengers_details(birthdate);



SELECT
CONCAT(CONCAT(P.firstname,' '), P.lastname) AS Nome
, trunc(months_between(sysdate,PD.birthdate)/12) AS idade
, PD.city AS Cidade
FROM air_passengers_details PD
INNER JOIN air_passengers P ON P.passenger_id = PD.passenger_id
WHERE 1=1
 AND PD.country = 'BRAZIL'
 AND PD.birthdate <= ADD_MONTHS(SYSDATE,-40*12)
 AND PD.sex = 'w'
;

ANALYZE CLUSTER tun_clh compute STATISTICS;
ANALYZE TABLE air_passengers compute STATISTICS;
ANALYZE TABLE air_passengers_details compute STATISTICS;
ANALYZE INDEX pk_air_passengers compute STATISTICS;
ANALYZE INDEX pk_passengers_details compute STATISTICS;
ANALYZE INDEX idx_pd_birthdate compute STATISTICS;


SELECT
CONCAT(CONCAT(P.firstname,' '), P.lastname) AS Nome
, trunc(months_between(sysdate,PD.birthdate)/12) AS idade
, PD.city AS Cidade
FROM air_passengers_details PD
INNER JOIN air_passengers P ON P.passenger_id = PD.passenger_id
WHERE 1=1
 AND PD.country = 'BRAZIL'
 AND PD.birthdate <= ADD_MONTHS(SYSDATE,-40*12)
 AND PD.sex = 'w'
;




--feito
-------
-- EM TESE É O HASH
-------



CREATE CLUSTER tun_clh (
    passenger_id numeric(12)
)
hashkeys 1024;



CREATE TABLE air_passengers CLUSTER tun_clh(passenger_id) AS SELECT * FROM arruda.air_passengers
CREATE TABLE air_passengers_details CLUSTER tun_clh(passenger_id) AS SELECT * FROM arruda.air_passengers_details

SELECT
    segment_name,
    SUM(bytes)/1024 KB
FROM
    user_segments
WHERE
    segment_type='CLUSTER'
    AND segment_name = 'TUN_CLH'
GROUP BY
    segment_name
ORDER BY
    2 DESC;


ALTER TABLE air_passengers
ADD CONSTRAINT pk_air_passengers
PRIMARY KEY (passenger_id);

ALTER TABLE air_passengers_details
ADD CONSTRAINT pk_air_passengers_details
PRIMARY KEY (passenger_id);




CREATE INDEX idx_pd_bithdate
 ON air_passengers_details(birthdate);

SELECT
CONCAT(CONCAT(P.firstname,' '), P.lastname) AS Nome
, trunc(months_between(sysdate,PD.birthdate)/12) AS idade
, PD.city AS Cidade
FROM air_passengers_details PD
INNER JOIN air_passengers P ON P.passenger_id = PD.passenger_id
WHERE 1=1
 AND PD.country = 'BRAZIL'
 AND PD.birthdate <= ADD_MONTHS(SYSDATE,-40*12)
 AND PD.sex = 'w'
;

ANALYZE CLUSTER tun_clh compute STATISTICS;
ANALYZE TABLE air_passengers compute STATISTICS;
ANALYZE TABLE air_passengers_details compute STATISTICS;
ANALYZE INDEX pk_air_passengers compute STATISTICS;
ANALYZE INDEX pk_passengers_details compute STATISTICS;
ANALYZE INDEX idx_pd_bithdate compute STATISTICS;

SELECT
CONCAT(CONCAT(P.firstname,' '), P.lastname) AS Nome
, trunc(months_between(sysdate,PD.birthdate)/12) AS idade
, PD.city AS Cidade
FROM air_passengers_details PD
INNER JOIN air_passengers P ON P.passenger_id = PD.passenger_id
WHERE 1=1
 AND PD.country = 'BRAZIL'
 AND PD.birthdate <= ADD_MONTHS(SYSDATE,-40*12)
 AND PD.sex = 'w'
;




--solução 2 lembra de executar tudo junto
--Listar o nome da companhia aérea, o identificador da aeronave, o nome do tipo de aeronave e o número de todos os voos operados
--por essa companhia aérea (independentemente de a aeronave ser de sua propriedade) que saem 
--E chegam em aeroportos localizados no país 'BRAZIL'. [resposta sugerida = 8 linhas - valor corrigido]


--tudo certo 

CREATE CLUSTER cl_aeroportos(
    airport_id numeric(5)
)
INDEX
SIZE 160;


CREATE CLUSTER cl_aeronaves(
    airplane_id numeric(6)
)
INDEX
SIZE 160;


CREATE INDEX idx_cl_pas ON CLUSTER cl_aeroportos;
CREATE INDEX idx_cl_aer ON CLUSTER cl_aeronaves;

CREATE TABLE air_airports  CLUSTER cl_aeroportos(airport_id) AS SELECT * FROM arruda.air_airports;
CREATE TABLE air_airports_geo  CLUSTER cl_aeroportos(airport_id) AS SELECT * FROM arruda.air_airports_geo;
CREATE TABLE air_airplanes  CLUSTER cl_aeronaves( airplane_id) AS SELECT * FROM arruda.air_airplanes;
CREATE TABLE air_airplane_types  CLUSTER cl_aeronaves( airplane_id) AS SELECT * FROM arruda.air_airplane_types;
CREATE TABLE air_airlines AS SELECT * FROM arruda.air_airlines;
CREATE TABLE air_flights AS SELECT * FROM arruda.air_flights;

--ate agora

SELECT
    segment_name,
    SUM(bytes)/1024 KB
FROM
    user_segments
WHERE
    segment_type='CLUSTER'
    AND segment_name = ('cl_aeroportos','cl_aeronaves')
GROUP BY
    segment_name
ORDER BY
    2 DESC;

SELECT
    index_name,
FROM
    user_indexes,
    blevel+1 AS index_height,
    leaf_blocks
WHERE
    index_name IN ('idx_cl_pas','idx_cl_aer');
-- index_height = 2

ALTER TABLE air_airlines
ADD CONSTRAINT pk_airlines PRIMARY KEY (airline_id);
ALTER TABLE air_airplane_types

ADD CONSTRAINT pk_airplane_types PRIMARY KEY (airplane_type_id);
ALTER TABLE air_airplanes
ADD CONSTRAINT pk_airplanes PRIMARY KEY (airplane_id);

ALTER TABLE air_airports
ADD CONSTRAINT pk_airports PRIMARY KEY (airport_id);

ALTER TABLE air_airports_geo
ADD CONSTRAINT pk_airports_geo PRIMARY KEY (airport_id);

CREATE INDEX idx_al_base_ap_id
 ON air_airlines(base_airport_id);


SELECT
 AL.airline_name AS "Nome Companhia"
 , AF.flightno AS "N° Voo"
 , AP.airplane_id AS "Ident. Aeronave"
 , APT.name AS "Tipo Aeronave"
FROM air_flights AF
INNER JOIN air_airplanes AP ON AP.airplane_id = AF.airplane_id
INNER JOIN air_airplane_type APT ON APT.airplane_type_id = AP.airplane_type_id
INNER JOIN air_airports APOP ON APOP.airport_id = AF.from_airport_id
INNER JOIN air_airports APOC ON APOC.airport_id = AF.to_airport_id
INNER JOIN air_airports_geo APGP ON APGP.airport_id = APOP.airport_id
INNER JOIN air_airports_geo APGC ON APGC.airport_id = APOC.airport_id
INNER JOIN air_airlines AL ON AL.airline_id = AF.airline_id
WHERE 1=1
 AND (APGP.country = 'BRAZIL' AND APGC.country = 'BRAZIL')
;

ANALYZE CLUSTER cl_aeroportos compute STATISTICS;
ANALYZE CLUSTER cl_aeronaves compute STATISTICS;
ANALYZE TABLE air_airports compute STATISTICS;
ANALYZE TABLE air_airports_geo compute STATISTICS;
ANALYZE TABLE air_airplanes compute STATISTICS;
ANALYZE TABLE air_airplane_types compute STATISTICS;
ANALYZE TABLE air_airlines compute STATISTICS;
ANALYZE TABLE air_flights compute STATISTICS;
ANALYZE INDEX pk_airlines compute STATISTICS;
ANALYZE INDEX pk_airplane_types compute STATISTICS;
ANALYZE INDEX pk_airports compute STATISTICS;
ANALYZE INDEX pk_airports_geo compute STATISTICS;
ANALYZE INDEX idx_al_base_ap_id compute STATISTICS;

SELECT
 AL.airline_name AS "Nome Companhia"
 , AF.flightno AS "N° Voo"
 , AP.airplane_id AS "Ident. Aeronave"
 , APT.name AS "Tipo Aeronave"
FROM air_flights AF
INNER JOIN air_airplanes AP ON AP.airplane_id = AF.airplane_id
INNER JOIN air_airplane_type APT ON APT.airplane_type_id = AP.airplane_type_id
INNER JOIN air_airports APOP ON APOP.airport_id = AF.from_airport_id
INNER JOIN air_airports APOC ON APOC.airport_id = AF.to_airport_id
INNER JOIN air_airports_geo APGP ON APGP.airport_id = APOP.airport_id
INNER JOIN air_airports_geo APGC ON APGC.airport_id = APOC.airport_id
INNER JOIN air_airlines AL ON AL.airline_id = AF.airline_id
WHERE 1=1
 AND (APGP.country = 'BRAZIL' AND APGC.country = 'BRAZIL')
;
--------
--HASH--
--

--solução 3 com o ajuste de data e ano
--Listar o número do voo, o nome do aeroporto de saída e o nome do aeroporto de destino, o nome completo (primeiro e último nome) e o assento de cada passageiro, 
--para todos os voos que partem no dia do seu aniversário neste ano (caso a consulta não retorne nenhuma linha, faça para o dia subsequente até encontrar
-- uma data que retorne alguma linha). [resposta sugerida = 106 linhas para o dia 25/03/2023]

CREATE TABLE AIR_AIRPORTS AS SELECT * FROM arruda.AIR_AIRPORTS;
CREATE TABLE AIR_BOOKINGS AS SELECT * FROM arruda.AIR_BOOKINGS;
CREATE TABLE AIR_PASSENGERS AS SELECT * FROM arruda.AIR_PASSENGERS;
CREATE TABLE AIR_PASSENGERS_DETAILS AS SELECT * FROM arruda.AIR_PASSENGERS_DETAILS;

SELECT
 AF.flightno AS "N° Voo"
 , APOP.name AS "Aeroporto Saída"
 , APOC.name AS "Aeroporto Chegada"
 , CONCAT(CONCAT(P.firstname,' '), P.lastname) AS Nome
 , B.seat AS Assento, AF.departure
FROM air_flights AF
INNER JOIN air_airports APOP ON APOP.airport_id = AF.from_airport_id
INNER JOIN air_airports APOC ON APOC.airport_id = AF.to_airport_id
INNER JOIN air_bookings B ON B.flight_id = AF.flight_id
INNER JOIN air_passengers P ON P.passenger_id = B.passenger_id
INNER JOIN air_passengers_details PD ON PD.passenger_id = P.passenger_id
WHERE 1=1
 AND EXTRACT(YEAR FROM AF.departure) = EXTRACT(YEAR FROM SYSDATE)
 AND TO_CHAR(TRUNC(AF.departure), 'DD-MM') = '25-03'
;


--SOLUÇÃO 4 funcionou tbm
--Listar o nome da companhia aérea bem como a data e a hora de saída de todos os voos que chegam para a cidade de 'NEW YORK' que partem às terças,
--quartas ou quintas-feiras, no mês do seu aniversário (caso a consulta não retorne nenhuma linha, faça para o mês subsequente até encontrar um mês que retorne alguma linha). 
--[resposta sugerida = 1 linha para o mês de março de 2023]


CREATE TABLE AIR_AIRPORTS AS SELECT * FROM arruda.AIR_AIRPORTS;
CREATE TABLE AIR_FLIGHTS_SCHEDULES AS SELECT * FROM arruda.AIR_FLIGHTS_SCHEDULES;
CREATE TABLE AIR_AIRPORTS_GEO AS SELECT * FROM arruda.AIR_AIRPORTS_GEO;
CREATE TABLE AIR_AIRLINES AS SELECT * FROM arruda.AIR_AIRLINES;


SELECT
 AL.airline_name AS Nome
 , TO_CHAR(AF.departure, 'HH24:MI:SS') AS Partida
 , TO_CHAR(AF.arrival, 'HH24:MI:SS') AS Chegada
FROM air_flights AF
INNER JOIN air_flights_schedules AFS ON AFS.flightno = AF.flightno
INNER JOIN air_airports APOC ON APOC.airport_id = AF.to_airport_id
INNER JOIN air_airports_geo APGC ON APGC.airport_id = APOC.airport_id
INNER JOIN air_airlines AL ON AL.airline_id = AF.airline_id
WHERE 1=1
 AND APGC.city = 'NEW YORK'
 AND (AFS.tuesday = 1 OR AFS.wednesday = 1 OR AFS.thursday = 1)
 --AND TO_CHAR(TRUNC(AF.departure), 'MM') = '03'
 AND TO_CHAR(TRUNC(AF.departure), 'MM-YYYY') = '03-2023'
;


--solução 5
--Crie uma consulta que seja resolvida adequadamente com um acesso hash em um cluster com pelo menos duas tabelas.
--A consulta deve utilizar todas as tabelas do cluster e pelo menos outra tabela fora dele.

CREATE TABLE AIR_PASSENGERS AS SELECT * FROM arruda.AIR_PASSENGERS;
CREATE TABLE AIR_PASSENGERS_DETAILS AS SELECT * FROM arruda.AIR_PASSENGERS_DETAILS;
CREATE TABLE AIR_BOOKINGS AS SELECT * FROM arruda.AIR_BOOKINGS;


SELECT
 P.passenger_id AS "ID Passageiro"
 , CONCAT(CONCAT(P.firstname,' '), P.lastname) AS Nome
 , B.seat AS "Cadeira"
 , B.price AS Preço
FROM air_passengers_details PD
INNER JOIN air_passengers P ON P.passenger_id = PD.passenger_id
LEFT OUTER JOIN air_bookings B ON B.passenger_id = P.passenger_id
WHERE 1=1
 AND P.passenger_id = 134 --pode editar para variar o passageiro
 AND B.seat <> '8G' --aqui tambem assim controla os gastos de cada passageiro po


--otimizado 1


