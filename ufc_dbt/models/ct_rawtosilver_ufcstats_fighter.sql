CREATE OR REPLACE table silver.ufcstatsfighter AS
        WITH record_cte AS (
            SELECT
                STR_SPLIT(fighterurl, '/')[5] fighterid,
                REGEXP_EXTRACT_ALL(record, '\d+') AS record_list
            FROM
                raw_ufcstatsfightertable
        )


        SELECT
            STR_SPLIT(main.fighterurl, '/')[5] AS fighterid,
            main.fightername,
            main.fighternickname,
            rec.record_list[1]::int AS wins,
            rec.record_list[2]::int AS losses,
            rec.record_list[3]::int AS draws,
            IFNULL(rec.record_list[4]::int, 0) AS no_contests,
            (rec.record_list[1]::int + rec.record_list[2]::int + IFNULL(rec.record_list[3], 0)::int + IFNULL(rec.record_list[4], 0)::int) AS totalfights,
            CASE
                WHEN CONTAINS(main.weight, '-') THEN NULL
                ELSE REGEXP_EXTRACT(TRIM(main.weight), '\d+')::int
            END AS weightlbs,
            CASE
                WHEN
                    CONTAINS(main.height, '-') THEN NULL
                ELSE
                    (STR_SPLIT(main.height, ' ')[1]::int * 12 + STR_SPLIT(main.height, ' ')[2]::int)
            END AS heightinches,
            CASE
                WHEN
                    CONTAINS(main.reach, '-') THEN NULL
                ELSE
                    main.reach::int
            END AS reach,
            main.stance,
            STRPTIME(
                CASE
                    WHEN CONTAINS(main.dob, '-') THEN NULL
                ELSE
                    main.dob
                END,                    '%b %d, %Y') AS dob,
            main.slpm::FLOAT AS slpm,
            CONCAT('.', REGEXP_EXTRACT(main.stracc, '\d+'))::FLOAT AS  scraped_stracc,
            main.sapm::FLOAT AS scraped_sapm,
            CONCAT('.', REGEXP_EXTRACT(main.strdef, '\d+'))::FLOAT AS scraped_strdef,
            main.tdavg::FLOAT AS scraped_tdavg,
            CONCAT('.', REGEXP_EXTRACT(main.tdacc, '\d+'))::FLOAT AS scraped_tdacc,
            CONCAT('.', REGEXP_EXTRACT(main.tddef, '\d+'))::FLOAT AS scraped_tddef,
            main.subavg::FLOAT AS scraped_subavg
        FROM
            raw_ufcstatsfightertable main
        INNER JOIN
            record_cte rec ON rec.fighterid = STR_SPLIT(main.fighterurl, '/')[5]