CREATE TEMP TABLE shop_temp (
  type smallint NOT NULL,
  faction smallint NOT NULL,
  cost int NOT NULL,
  vtstock smallint DEFAULT NULL,
  vtlimit smallint DEFAULT NULL,
  "serial" bigint NOT NULL DEFAULT '1',
  turn smallint NOT NULL,
  name varchar(32) NOT NULL,
  PRIMARY KEY (type)
);

INSERT INTO shop_temp VALUES (0,1,12,NULL,NULL,1164,1,'Vitzh'),(1,1,14,30,30,993,3,'m-Vitzh'),(2,1,10,NULL,NULL,1596,1,'Vortex'),(3,1,3000,1,1,38,8,'Juggernaut'),(5,2,22,14,25,1058,1,'Falchion'),(6,2,100,15,15,376,4,'Blade'),(7,2,25,NULL,NULL,1590,1,'Decider'),(8,1,30,NULL,NULL,1170,2,'Scare Face'),(9,1,25,25,25,699,3,'Scare Face A1'),(10,2,110,NULL,NULL,724,4,'Prominence M1'),(11,2,400,6,6,315,5,'Prominence M2'),(12,2,600,3,3,178,6,'Prominence M3'),(13,1,120,NULL,NULL,940,4,'Scare Face II'),(14,2,400,4,4,175,5,'Rapier'),(15,2,2600,2,2,56,7,'Quasar'),(16,4,170,NULL,NULL,351,5,'Jaralaccs C'),(17,4,400,4,4,281,6,'Jaralaccs NS-R'),(18,4,160,NULL,NULL,520,5,'Jaralaccs N'),(19,1,800,2,2,55,7,'Regal Dress N'),(20,1,800,2,2,46,7,'Regal Dress A'),(21,1,550,3,3,205,5,'Behemoth'),(22,1,120,15,15,394,5,'Maelstrom'),(23,3,300,4,4,222,5,'Sheepdog'),(24,1,260,4,4,244,6,'Garpike'),(25,3,300,6,6,282,6,'Siegeszug'),(26,3,18,25,25,705,3,'Colt Executive'),(27,3,15,NULL,NULL,560,3,'Colt'),(28,4,1200,2,2,74,7,'Jaralaccs Macabre'),(29,3,4000,1,1,42,7,'Earth Shaker'),(30,3,90,NULL,NULL,1091,4,'Yellow Jacket'),(31,2,20,20,20,437,2,'Decider Volcanic');

INSERT INTO shop SELECT
	type,
	CASE
		WHEN faction = 1 THEN 'hsd'::faction
		WHEN faction = 2 THEN 'prf'::faction
		WHEN faction = 3 THEN 'rb'::faction
		WHEN faction = 4 THEN 'jar'::faction
		ELSE 'none'::faction
	END as faction,
	cost,
	vtstock,
	vtlimit,
	"serial",
	turn,
	name
FROM shop_temp;
