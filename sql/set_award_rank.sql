CREATE OR REPLACE PROCEDURE public.set_award_rank(IN set_xuid uuid, IN set_round smallint, IN set_turn smallint, IN set_rank1 integer, IN set_rank2 integer, IN set_rank3 integer, IN set_rank4 integer)
LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO awards (xuid, round, turn) VALUES (set_xuid, set_round, set_turn) ON CONFLICT (xuid, round, turn) DO NOTHING;
    IF (set_rank1 IS NOT NULL) THEN
        UPDATE awards SET rank1 = set_rank1 WHERE xuid = set_xuid AND round = set_round AND turn = set_turn;
    END IF;
    IF (set_rank2 IS NOT NULL) THEN
        UPDATE awards SET rank2 = set_rank2 WHERE xuid = set_xuid AND round = set_round AND turn = set_turn;
    END IF;
    IF (set_rank3 IS NOT NULL) THEN
        UPDATE awards SET rank3 = set_rank3 WHERE xuid = set_xuid AND round = set_round AND turn = set_turn;
    END IF;
    IF (set_rank4 IS NOT NULL) THEN
        UPDATE awards SET rank4 = set_rank4 WHERE xuid = set_xuid AND round = set_round AND turn = set_turn;
    END IF;
END
$procedure$
