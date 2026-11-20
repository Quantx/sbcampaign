CREATE TEMP TABLE account_temp (
  xuid bigint NOT NULL,
  xname char(16) DEFAULT NULL,
  faction smallint NOT NULL DEFAULT '0',
  pilot varchar(3632) DEFAULT NULL,
  "language" smallint DEFAULT NULL,
  banned smallint NOT NULL DEFAULT '0',
  PRIMARY KEY (xuid)
);

INSERT INTO account_temp VALUES (1234567,'XLiveUsername',0,'<HEXDATA_HERE>',1,0), (456778,NULL,0,NULL,1,0);

INSERT INTO accounts SELECT
	(lpad(to_hex(xuid::bigint), 32, '0'))::uuid as xuid,
	xname,
	CASE
		WHEN faction = 1 THEN 'hsd'::faction
		WHEN faction = 2 THEN 'prf'::faction
		WHEN faction = 3 THEN 'rb'::faction
		WHEN faction = 4 THEN 'jar'::faction
		ELSE 'none'::faction
	END as faction,
	decode(pilot, 'hex') as pilot,
	CASE
		WHEN "language" = 0 THEN 'ja'::language
		ELSE 'en'::language
	END as "language",
	CASE WHEN banned = 0 THEN false ELSE true END as banned
FROM account_temp;
