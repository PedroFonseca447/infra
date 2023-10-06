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