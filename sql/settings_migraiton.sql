CREATE TEMP TABLE settings_temp (
  round smallint NOT NULL,
  turn smallint NOT NULL,
  "period" timestamp NOT NULL,
  freemsg_en varchar(256) DEFAULT NULL,
  freemsg_jp varchar(256) DEFAULT NULL,
  PRIMARY KEY (round, turn)
);

INSERT INTO settings_temp VALUES (1, 1,'2020-02-22 00:00:00','MOTD',NULL);

INSERT INTO settings SELECT
	round,
	turn,
	"period"
FROM settings_temp;
