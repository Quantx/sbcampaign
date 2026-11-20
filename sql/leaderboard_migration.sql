CREATE TEMP TABLE leaderboard_temp (
  xuid bigint NOT NULL,
  faction smallint NOT NULL,
  round smallint NOT NULL,
  turn smallint NOT NULL,
  map smallint NOT NULL,
  stat1 int NOT NULL DEFAULT '0',
  stat2 int NOT NULL DEFAULT '0',
  stat3 int NOT NULL DEFAULT '0',
  stat4 int NOT NULL DEFAULT '0',
  stat5 int NOT NULL DEFAULT '0',
  stat6 int NOT NULL DEFAULT '0',
  stat7 int NOT NULL DEFAULT '0',
  stat8 int NOT NULL DEFAULT '0',
  stat9 int NOT NULL DEFAULT '0',
  stat10 int NOT NULL DEFAULT '0',
  PRIMARY KEY (xuid, faction, round, turn, map)
);

INSERT INTO leaderboard_temp VALUES (12345,1,10,4,1,1,63,1,200,0,1,1,0,0,0);

INSERT INTO leaderboard SELECT
	(lpad(to_hex(xuid::bigint), 32, '0'))::uuid as xuid,
	CASE
		WHEN faction = 1 THEN 'hsd'::faction
		WHEN faction = 2 THEN 'prf'::faction
		WHEN faction = 3 THEN 'rb'::faction
		WHEN faction = 4 THEN 'jar'::faction
		ELSE 'none'::faction
	END as faction,
	round,
	turn,
	map,
	stat1,
	stat2,
	stat3,
	stat4,
	stat5,
	stat6,
	stat7,
	stat8,
	stat9,
	stat10
FROM leaderboard_temp;

SELECT * FROM leaderboard;
