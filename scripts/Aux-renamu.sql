--comprobación de que todos son la misma tabla.

SELECT DISTINCT ano FROM `renamu_base_renamu 2021`;

SELECT DISTINCT ano FROM renamu_cdir;

SELECT DISTINCT ano FROM renamu_directorio;

SELECT DISTINCT ano FROM renamu_c001;

SELECT DISTINCT ano FROM renamu_c0001;

SELECT DISTINCT ano FROM renamu_cuadro_c0001;


-- se consolida la data
CREATE TABLE TB_DEP_PROV_DIST AS  
SELECT ano, ccdd, departamen, ccpp, provincia, ccdi, distrito FROM `renamu_base_renamu 2021`;
INSERT INTO TB_DEP_PROV_DIST (ano, ccdd, departamen, ccpp, provincia, ccdi, distrito) SELECT ano, ccdd, departamen, ccpp, provincia, ccdi, distrito FROM renamu_cdir;
INSERT INTO TB_DEP_PROV_DIST (ano, ccdd, departamen, ccpp, provincia, ccdi, distrito) SELECT ano, ccdd, nom_dpto, ccpp, nom_prov, ccdi, nom_dist FROM renamu_cdir;
INSERT INTO TB_DEP_PROV_DIST (ano, ccdd, departamen, ccpp, provincia, ccdi, distrito) SELECT ano, ccdd, nom_dpto, ccpp, nom_prov, ccdi, nom_dist FROM renamu_c001;
INSERT INTO TB_DEP_PROV_DIST (ano, ccdd, departamen, ccpp, provincia, ccdi, distrito) SELECT ano, ccdd, dpto_nombr, ccpp, prov_nombr, ccdi, dist_nombr FROM renamu_c001;
INSERT INTO TB_DEP_PROV_DIST (ano, ccdd, departamen, ccpp, provincia, ccdi, distrito) SELECT ano, ccdd, departamen, ccpp, provincia, ccdi, distrito from renamu_directorio;
INSERT INTO TB_DEP_PROV_DIST (ano, ccdd, departamen, ccpp, provincia, ccdi, distrito) SELECT ano, ccdd, nom_dpto, ccpp, nom_prov, ccdi, nom_dist FROM renamu_c0001;
INSERT INTO TB_DEP_PROV_DIST (ano, ccdd, departamen, ccpp, provincia, ccdi, distrito) SELECT ano, ccdd, nom_dpto, ccpp, nom_prov, ccdi, nom_dist FROM renamu_cuadro_c0001;

-- corregir errores de redacción
UPDATE TB_DEP_PROV_DIST SET departamen = "ANCASH" WHERE departamen = "µNCASH";
UPDATE TB_DEP_PROV_DIST SET departamen = "APURIMAC" WHERE departamen = "APURÖMAC";
UPDATE TB_DEP_PROV_DIST SET departamen = "HUANUCO" WHERE departamen = "HUµNUCO";
UPDATE TB_DEP_PROV_DIST SET departamen = "JUNIN" WHERE departamen = "JUNÖN";
UPDATE TB_DEP_PROV_DIST SET departamen = "SAN MARTIN" WHERE departamen = "SAN MARTÖN"
DELETE FROM TB_DEP_PROV_DIST WHERE departamen is null

-- eliminar duplicados
CREATE TABLE db_regprovdist AS  
SELECT DISTINCT * FROM TB_DEP_PROV_DIST

-- convertir los campos de id en número para poder hacer el enlace con la base de ENDES
ALTER TABLE db_regprovdist MODIFY ccdd INT(3);  
ALTER TABLE db_regprovdist MODIFY ccpp INT(3);  
ALTER TABLE db_regprovdist MODIFY ccdi INT(3);  

-- creamos índices
CREATE INDEX IND_GEO_REG ON db_regprovdist(ccdd);
CREATE INDEX IND_GEO_PROV ON db_regprovdist(ccpp);
CREATE INDEX IND_GEO_REGPROV ON db_regprovdist(ccdd, ccpp);
CREATE INDEX IND_GEO_DIST ON db_regprovdist(ccdi);
CREATE INDEX IND_GEO_REGPROVDIST ON db_regprovdist(ccdd, ccpp, ccdi);

-- crear la tabla hasta la otra base de datos
CREATE TABLE indiceanemia.db_regprovdist AS  
SELECT * FROM renamu.db_regprovdist

-- verificación
SELECT ano, COUNT(DISTINCT(departamen)) FROM db_regprovdist GROUP BY ano --debe resultar 25 por año

-- validación de regiones
SELECT COUNT(DISTINCT ccdd, departamen) FROM db_regprovdist GROUP by ccdd; --debe resultar 1

-- validación de provincias
SELECT COUNT(DISTINCT ccdd, ccpp, provincia) FROM `db_regprovdist` GROUP by ccdd,ccpp  
ORDER BY `COUNT(DISTINCT ccdd, ccpp, provincia)` DESC


-- creamos una tabla de provincias y distritos desde la BD RENAMU para usarlos en la data global 
CREATE TABLE db_nom_provDis as
SELECT DISTINCT ccdd, departamen, ccpp, provincia, ccdi, distrito FROM db_regprovdist
GROUP BY ccdd, ccpp, ccdi

ALTER TABLE db_nom_provdis MODIFY ccdd INT(3);  
ALTER TABLE db_nom_provdis MODIFY ccpp INT(3);  
ALTER TABLE db_nom_provDis MODIFY ccdi INT(3); 

CREATE INDEX IND_GEO_REG ON db_nom_provDis(ccdd);
CREATE INDEX IND_GEO_PROV ON db_nom_provDis(ccpp);
CREATE INDEX IND_GEO_REGPROV ON db_nom_provDis(ccdd, ccpp);
CREATE INDEX IND_GEO_DIST ON db_nom_provDis(ccdi);
CREATE INDEX IND_GEO_REGPROVDIST ON db_nom_provDis(ccdd, ccpp, ccdi);

-- actualizar algunos datos con errores tipográficos
UPDATE db_regprovdist SET distrito = "JOSE MARIA ARGUEDAS" WHERE distrito = "JOS MARÖA ARGUEDAS";
UPDATE db_regprovdist SET distrito = "ANDRES AVELINO CACERES DORREGARAY" WHERE distrito = "ANDRS AVELINO CµCERES DORREGARAY";
UPDATE db_regprovdist SET distrito = "VEINTISEIS DE OCTUBRE" WHERE distrito = "VEINTISIS DE OCTUBRE";

-- opcional agreagr en la bd, sino por código 13. NombreProvDistr
ALTER TABLE data_indiceanemia ADD nombre_provincia varchar(30) AFTER provincia;
ALTER TABLE data_indiceanemia ADD nombre_distrito varchar(30) AFTER distrito;

UPDATE data_indiceanemia JOIN db_nom_provDis 
ON data_indiceanemia.Región = db_nom_provDis.ccdd and data_indiceanemia.Provincia = db_nom_provDis.ccpp 
SET data_indiceanemia.nombre_provincia = db_nom_provDis.provincia
;
UPDATE data_indiceanemia JOIN db_nom_provDis 
ON data_indiceanemia.Región = db_nom_provDis.ccdd and data_indiceanemia.Provincia = db_nom_provDis.ccpp and data_indiceanemia.Distrito = db_nom_provDis.ccdi 
SET data_indiceanemia.nombre_distrito = db_nom_provDis.distrito