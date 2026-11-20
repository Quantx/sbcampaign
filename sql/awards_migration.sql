CREATE TEMP TABLE awards_temp (
  xuid bigint NOT NULL,
  round smallint NOT NULL,
  turn smallint NOT NULL,
  rank1 int DEFAULT NULL,
  rank2 int DEFAULT NULL,
  rank3 int DEFAULT NULL,
  rank4 int DEFAULT NULL,
  PRIMARY KEY (xuid, round, turn)
);

INSERT INTO awards_temp VALUES (12345678,0,0,0,0,NULL,NULL);

INSERT INTO awards SELECT
	(lpad(to_hex(xuid::bigint), 32, '0'))::uuid as xuid,
	round,
	turn,
	rank1,
	rank2,
	rank3,
	rank4
FROM awards_temp;
