ALTER TABLE DM.DM_F101_ROUND_F
ADD CONSTRAINT PK_dm_f101_round_f
PRIMARY KEY (from_date, to_date, ledger_account, characteristic);

--ПРОЦЕДУРА 101 ФОРМА
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_date DATE := (i_OnDate - INTERVAL '1 month')::DATE;
    v_to_date DATE := (i_OnDate - INTERVAL '1 day')::DATE;
	f_start_time TIMESTAMP := now();
BEGIN
    DELETE FROM dm.dm_f101_round_f
    WHERE from_date = v_from_date AND to_date = v_to_date;

    -- Расчет и вставка данных
    INSERT INTO dm.dm_f101_round_f (
        from_date, to_date, chapter, ledger_account, characteristic,
        balance_in_rub, balance_in_val, balance_in_total,
        turn_deb_rub, turn_deb_val, turn_deb_total,
        turn_cre_rub, turn_cre_val, turn_cre_total,
        balance_out_rub, balance_out_val, balance_out_total
    )
    SELECT
        v_from_date, v_to_date,
        led.chapter,
        LEFT(acc.account_number, 5) AS ledger_account,
        acc.char_type,

        -- Входящие остатки
        COALESCE(SUM(CASE WHEN acc.currency_code IN ('643', '810') 
						THEN b_in.balance_out_rub 
						ELSE 0 
						END), 0) AS balance_in_rub,
        COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('643', '810') 
						THEN b_in.balance_out_rub 
						ELSE 0 
						END), 0) AS balance_in_val,
        COALESCE(SUM(COALESCE(b_in.balance_out_rub, 0)), 0) AS balance_in_total,

        -- Дебетовые обороты
        COALESCE(SUM(CASE WHEN acc.currency_code IN ('643', '810') 
						THEN t.debet_amount_rub 
						ELSE 0 
						END), 0) AS turn_deb_rub,
        COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('643', '810') 
						THEN t.debet_amount_rub 
						ELSE 0 
						END), 0) AS turn_deb_val,
        COALESCE(SUM(COALESCE(t.debet_amount_rub, 0)), 0) AS turn_deb_total,

        -- Кредитовые обороты
        COALESCE(SUM(CASE WHEN acc.currency_code IN ('643', '810') 
						THEN t.credit_amount_rub 
						ELSE 0 
						END), 0) AS turn_cre_rub,
        COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('643', '810') 
						THEN t.credit_amount_rub 
						ELSE 0 
						END), 0) AS turn_cre_val,
        COALESCE(SUM(COALESCE(t.credit_amount_rub, 0)), 0) AS turn_cre_total,

        -- сыммы исходящего остатка
        COALESCE(SUM(CASE WHEN acc.currency_code IN ('643', '810') 
						THEN b_out.balance_out_rub 
						ELSE 0 
						END), 0) AS balance_out_rub,
        COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('643', '810') 
						THEN b_out.balance_out_rub 
						ELSE 0 
						END), 0) AS balance_out_val,
        COALESCE(SUM(COALESCE(b_out.balance_out_rub, 0)), 0) AS balance_out_total

    FROM ds.md_account_d acc
    LEFT JOIN ds.md_ledger_account_s led
        ON led.ledger_account = LEFT(acc.account_number, 5)::INT
        AND v_from_date BETWEEN led.start_date AND led.end_date

    LEFT JOIN dm.dm_account_balance_f b_in
        ON b_in.account_rk = acc.account_rk AND b_in.on_date = (v_from_date - INTERVAL '1 day')

    LEFT JOIN dm.dm_account_balance_f b_out
        ON b_out.account_rk = acc.account_rk AND b_out.on_date = v_to_date

    LEFT JOIN (
        SELECT account_rk,
               SUM(debet_amount_rub) AS debet_amount_rub,
               SUM(credit_amount_rub) AS credit_amount_rub
        FROM dm.dm_account_turnover_f
        WHERE on_date BETWEEN v_from_date AND v_to_date
        GROUP BY account_rk
    ) t ON t.account_rk = acc.account_rk

    WHERE acc.data_actual_date <= v_to_date
      AND acc.data_actual_end_date >= v_from_date

    GROUP BY
        led.chapter,
        LEFT(acc.account_number, 5),
        acc.char_type;

    -- логи
    INSERT INTO logs.etl_log(process_name, start_time, end_time, commentt)
    VALUES ('fill_f101_round_f', f_start_time, now(), 'Completed for ' || i_OnDate);
END;
$$;
