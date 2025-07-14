--Добавление pk
ALTER TABLE DM.DM_F101_ROUND_F
ADD CONSTRAINT PK_dm_f101_round_f
PRIMARY KEY (from_date, to_date, ledger_account, characteristic);

--ПРОЦЕДУРА 101 ФОРМА
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    f_from_date DATE := (i_OnDate - INTERVAL '1 month')::DATE;
    f_to_date   DATE := (i_OnDate - INTERVAL '1 day')::DATE;
    f_prev_date DATE := (i_OnDate - INTERVAL '1 month' - INTERVAL '1 day')::DATE;
    f_start_time TIMESTAMP := now();
BEGIN
   
    -- Удаление
    DELETE FROM dm.dm_f101_round_f
    WHERE from_date = f_from_date AND to_date = f_to_date;

    -- Вставка 
    INSERT INTO dm.dm_f101_round_f (
        from_date, to_date, chapter, ledger_account, characteristic,
        balance_in_rub, balance_in_val, balance_in_total,
        turn_deb_rub, turn_deb_val, turn_deb_total,
        turn_cre_rub, turn_cre_val, turn_cre_total,
        balance_out_rub, balance_out_val, balance_out_total
    )
    SELECT
        f_from_date,
        f_to_date,
        led.chapter,
        SUBSTRING(acc.account_number FROM 1 FOR 5),
        acc.char_type,

        -- Сумма остатков на начало периода
        SUM(CASE WHEN acc.currency_code IN ('810', '643') 
					THEN COALESCE(b_in.balance_out_rub, 0) 
					ELSE 0 
			END),
        SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') 
					THEN COALESCE(b_in.balance_out_rub, 0) 
					ELSE 0 
			END),
        SUM(COALESCE(b_in.balance_out_rub, 0)),

        -- Дебетовые обороты
        SUM(CASE WHEN acc.currency_code IN ('810', '643') 
					THEN COALESCE(turn.debet_amount_rub, 0) 
					ELSE 0 
			END),
        SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') 
					THEN COALESCE(turn.debet_amount_rub, 0) 
					ELSE 0 
			END),
        SUM(COALESCE(turn.debet_amount_rub, 0)),

        -- Кредитовые обороты
        SUM(CASE WHEN acc.currency_code IN ('810', '643') 
					THEN COALESCE(turn.credit_amount_rub, 0) 
					ELSE 0 
			END),
        SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') 
					THEN COALESCE(turn.credit_amount_rub, 0) 
					ELSE 0 
			END),
        SUM(COALESCE(turn.credit_amount_rub, 0)),

        -- Сумма остатков на конец периода
        SUM(CASE WHEN acc.currency_code IN ('810', '643') 
					THEN COALESCE(b_out.balance_out_rub, 0) 
					ELSE 0 
			END),
        SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') 
					THEN COALESCE(b_out.balance_out_rub, 0) 
					ELSE 0 
			END),
        SUM(COALESCE(b_out.balance_out_rub, 0))
    FROM ds.md_account_d acc 
    LEFT JOIN ds.md_ledger_account_s led
        ON led.ledger_account = SUBSTRING(acc.account_number FROM 1 FOR 5)::INT
       AND acc.char_type = led.characteristic
       AND f_from_date BETWEEN led.start_date AND COALESCE(led.end_date, '9999-12-31')
	 --Остатки на начало периода
    LEFT JOIN dm.dm_account_balance_f b_in
        ON b_in.account_rk = acc.account_rk AND b_in.on_date = f_prev_date
	-- Остатки на конец периода
    LEFT JOIN dm.dm_account_balance_f b_out
        ON b_out.account_rk = acc.account_rk AND b_out.on_date = f_to_date
    LEFT JOIN ( -- Подзапрос для агрегирования оборотов за отчетный период
        SELECT account_rk,
               SUM(debet_amount_rub) AS debet_amount_rub,
               SUM(credit_amount_rub) AS credit_amount_rub
        FROM dm.dm_account_turnover_f
        WHERE on_date BETWEEN f_from_date AND f_to_date
        GROUP BY account_rk
    ) turn ON turn.account_rk = acc.account_rk
	-- Проверка актуальности
    WHERE acc.data_actual_date <= f_to_date
      AND acc.data_actual_end_date >= f_from_date
    GROUP BY
        led.chapter,
        SUBSTRING(acc.account_number FROM 1 FOR 5),
        acc.char_type;

    -- Лог завершения
    INSERT INTO logs.etl_log (process_name, start_time, end_time, commentt)
    VALUES ('fill_f101_round_f', f_start_time, now(), 'Completed for period ' || f_from_date || ' to ' || f_to_date);

EXCEPTION
    WHEN OTHERS THEN
        -- Лог ошибки
        INSERT INTO logs.etl_log (process_name, start_time, end_time, commentt)
        VALUES ('fill_f101_round_f', f_start_time, now(), 'ERROR: ' || SQLERRM);
        RAISE;
END;
$$;
