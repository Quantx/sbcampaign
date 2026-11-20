CREATE OR REPLACE PROCEDURE public.give_turn_award(IN award_round smallint, IN award_turn smallint, IN award_limit integer DEFAULT 10)
LANGUAGE plpgsql
AS $procedure$
DECLARE
    lb1_cursor CURSOR FOR
    SELECT xuid, ROW_NUMBER() OVER(ORDER BY (score / scoreDiv) DESC) "rank" FROM (
            SELECT xuid, (SUM(stat1) * 1000) as score, SUM(stat2) as scoreDiv FROM leaderboard WHERE round = award_round AND turn = award_turn GROUP BY xuid
    ) as lb WHERE scoreDiv >= 50 LIMIT award_limit;

    lb2_cursor CURSOR FOR
    SELECT xuid, ROW_NUMBER() OVER(ORDER BY score DESC) "rank" FROM (
            SELECT xuid, SUM(stat4) as score FROM leaderboard WHERE round = award_round AND turn = award_turn GROUP BY xuid
    ) as lb WHERE score > 0 LIMIT award_limit;

    lb3_cursor CURSOR FOR
    SELECT xuid, ROW_NUMBER() OVER(ORDER BY (score / scoreDiv) DESC) "rank" FROM (
            SELECT xuid, (SUM(stat6) * 1000) as score, SUM(stat7) as scoreDiv FROM leaderboard WHERE round = award_round AND turn = award_turn GROUP BY xuid
    ) as lb WHERE scoreDiv >= 5 LIMIT award_limit;

    lb4_cursor CURSOR FOR
    SELECT xuid, ROW_NUMBER() OVER(ORDER BY score DESC) "rank" FROM (
            SELECT xuid, SUM(stat8) as score FROM leaderboard WHERE round = award_round AND turn = award_turn GROUP BY xuid
    ) as lb WHERE score > 0 LIMIT award_limit;

    lb_record record;
BEGIN
    FOR lb_record IN lb1_cursor LOOP
            CALL set_award_rank(lb_record.xuid, award_round, award_turn, lb_record.rank::integer, NULL, NULL, NULL);
    END LOOP;

    FOR lb_record in lb2_cursor LOOP
            CALL set_award_rank(lb_record.xuid, award_round, award_turn, NULL, lb_record.rank::integer, NULL, NULL);
    END LOOP;

    FOR lb_record in lb3_cursor LOOP
            CALL set_award_rank(lb_record.xuid, award_round, award_turn, NULL, NULL, lb_record.rank::integer, NULL);
    END LOOP;

    FOR lb_record in lb4_cursor LOOP
            CALL set_award_rank(lb_record.xuid, award_round, award_turn, NULL, NULL, NULL, lb_record.rank::integer);
    END LOOP;
END
$procedure$
