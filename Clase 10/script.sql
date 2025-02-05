-- a) Cuántos hombres y cuántas mujeres sobrevivieron
SELECT
	tn.sex AS `Sexo`,
	SUM(tn.survived) AS `Sobrevivientes`
FROM titanic_nifi tn
WHERE tn.survived = 1
GROUP BY tn.sex;

SELECT * FROM titanic_nifi tn;


-- b) Cuántas personas sobrevivieron según cada clase (Pclass)
SELECT
	tn.pclass AS `Clase`,
	SUM(tn.survived) AS `Sobrevivientes`
FROM titanic_nifi tn
WHERE tn.survived = 1
GROUP BY tn.pclass;

-- c) Cuál fue la persona de mayor edad que sobrevivió
SELECT
	tn.name AS `Nombre`,
	tn.age AS `edad`
FROM titanic_nifi tn
WHERE tn.survived = 1
ORDER BY edad DESC
LIMIT 1;

-- d) Cuál fue la persona más joven que sobrevivió
SELECT
	tn.name AS `Nombre`,
	tn.age AS `edad`
FROM titanic_nifi tn
WHERE tn.survived = 1
AND tn.age NOT NULL
ORDER BY edad
LIMIT 1;