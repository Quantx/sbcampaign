CREATE OR REPLACE PROCEDURE public.shop_drop(IN drop_turn smallint, IN drop_interval real)
LANGUAGE plpgsql
AS $procedure$
DECLARE
    shop_cursor CURSOR FOR
    SELECT "type", vtstock, vtlimit FROM shop
        WHERE turn <= drop_turn AND vtstock < vtlimit
        AND "type" NOT IN (3, 15, 19, 20, 28, 29) FOR UPDATE;

    shop_record RECORD;
BEGIN
    FOR shop_record IN shop_cursor LOOP
        IF ((random() * drop_interval) < (shop_record.vtlimit - shop_record.vtstock)) THEN
            UPDATE shop SET vtstock = vtstock + 1 WHERE "type" = shop_record.type;
        END IF;
    END LOOP;
END
$procedure$
