CREATE TEMP TABLE garage_temp (
  type smallint NOT NULL,
  serial bigint NOT NULL,
  xuid bigint NOT NULL,
  faction smallint NOT NULL,
  sortied smallint NOT NULL DEFAULT '0',
  PRIMARY KEY (type, serial)
);

INSERT INTO garage_temp VALUES (0,0,12345678,1,0);

INSERT INTO garage SELECT
	type,
	serial,
	(lpad(to_hex(xuid::bigint), 32, '0'))::uuid as xuid,
	CASE
		WHEN faction = 1 THEN 'hsd'::faction
		WHEN faction = 2 THEN 'prf'::faction
		WHEN faction = 3 THEN 'rb'::faction
		WHEN faction = 4 THEN 'jar'::faction
		ELSE 'none'::faction
	END as faction,
	CASE WHEN sortied = 0 THEN false ELSE true END as sortied
FROM garage_temp;
