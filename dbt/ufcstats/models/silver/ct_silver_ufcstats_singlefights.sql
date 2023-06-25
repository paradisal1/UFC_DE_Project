    WITH round_cte AS (
        SELECT
            id,
            CASE
                WHEN TIMEFORMAT == 'No Time Limit' THEN NULL
                ELSE [val::int for val in STR_SPLIT(REPLACE(REPLACE(REGEXP_EXTRACT(timeformat, '\((.*)\)'), '(', ''),')',''), '-')]
            END AS rnds
        FROM
            raw.ufcstats_singlefights
    )

    SELECT
        eventname,
        STRPTIME(eventdate, '%B %d, %Y') AS eventdate,
        STR_SPLIT(eventlocation, ', ')[1] AS eventcity,
        STR_SPLIT(eventlocation, ', ')[2] AS eventstate,
        STR_SPLIT(eventlocation, ', ')[3] AS eventcountry,
        id as fightid,
        REPLACE(weightclass, ' Bout', '') weightclass,

        CASE
            WHEN
                CONTAINS(weightclass, 'Title') THEN True::BOOLEAN
            ELSE
                False::BOOLEAN
        END AS titlefight,
        method,
        round,
        -- Total rounds in fight
        LIST_COUNT(rcte.rnds) AS totalrounds,
        LIST_SUM(rcte.rnds) * 60 AS totaltime,
        CASE
            WHEN round = 1 THEN str_split(time, ':')[1]::int * 60 + str_split(time, ':')[2]::int
            ELSE LIST_SUM(rcte.rnds[1:round::int - 1]) * 60 + str_split(time, ':')[1]::int * 60 + str_split(time, ':')[2]::int
        END AS time,
        referee,
        fightername,
        fighterid,
        opponentid,
        fighternickname,
        CASE
            WHEN CONTAINS(fighterkd, '-') THEN NULL
            ELSE fighterkd::int
        END AS fighterkd,
        CASE
            WHEN CONTAINS(fightersigstr, '-') THEN NULL
            ELSE STR_SPLIT(fightersigstr, ' of ')[1]::int
        END AS fightersigstr,
        CASE
            WHEN CONTAINS(fightersigstr, '-') THEN NULL
            ELSE STR_SPLIT(fightersigstr, ' of ')[2]::int
        END AS fightersigstratt,
        CASE
            WHEN CONTAINS(fightersigstrpct, '-') OR fightersigstrpct == '' THEN NULL
            ELSE REGEXP_EXTRACT(fightersigstrpct, '([0-9]+)')::int / 100
        END AS fightersigstrpct,
        CASE
            WHEN CONTAINS(FIGHTERTOTALSTR, '-') THEN NULL
            ELSE STR_SPLIT(FIGHTERTOTALSTR, ' of ')[1]::int
        END AS fightertotalstr,
        CASE
            WHEN CONTAINS(FIGHTERTOTALSTR, '-') THEN NULL
            ELSE STR_SPLIT(FIGHTERTOTALSTR, ' of ')[2]::int
        END AS fightertotalstratt,
        CASE
            WHEN CONTAINS(FIGHTERTD, '-') THEN NULL
            ELSE STR_SPLIT(FIGHTERTD, ' of ')[1]::int
        END AS fightertd,
        CASE
            WHEN CONTAINS(FIGHTERTD, '-') THEN NULL
            ELSE STR_SPLIT(FIGHTERTD, ' of ')[2]::int
        END AS fightertdatt,
        CASE
            WHEN CONTAINS(fightertdpct, '-') OR fightertdpct == '' THEN NULL
            ELSE REGEXP_EXTRACT(fightertdpct, '([0-9]+)')::int / 100
        END AS fightertdpct,
        CASE
            WHEN CONTAINS(fightersubatt, '-') THEN NULL
            ELSE fightersubatt::int
        END AS fightersubatt,
        CASE
            WHEN CONTAINS(fighterrev, '-') THEN NULL
            ELSE fighterrev::int
        END AS fighterrev,
        CASE
            WHEN CONTAINS(fighterctrl, '-') THEN NULL
            ELSE STR_SPLIT(fighterctrl, ':')[1]::int * 60 + STR_SPLIT(fighterctrl, ':')[2]::int
        END AS fighterctrl,
        CASE
            WHEN CONTAINS(fighterhead, '-') THEN NULL
            ELSE STR_SPLIT(fighterhead, ' of ')[1]::int
        END AS fighterhead,
        CASE
            WHEN CONTAINS(fighterhead, '-') THEN NULL
            ELSE STR_SPLIT(fighterhead, ' of ')[2]::int
        END AS fighterheadatt,
        CASE
            WHEN CONTAINS(fighterbody, '-') THEN NULL
            ELSE STR_SPLIT(fighterbody, ' of ')[1]::int
        END AS fighterbody,
        CASE
            WHEN CONTAINS(fighterbody, '-') THEN NULL
            ELSE STR_SPLIT(fighterbody, ' of ')[2]::int
        END AS fighterbodyatt,
        CASE
            WHEN CONTAINS(fighterleg, '-') THEN NULL
            ELSE STR_SPLIT(fighterleg, ' of ')[1]::int
        END AS fighterleg,
        CASE
            WHEN CONTAINS(fighterleg, '-') THEN NULL
            ELSE STR_SPLIT(fighterleg, ' of ')[2]::int
        END AS fighterlegatt,
        CASE
            WHEN CONTAINS(fighterdistance, '-') THEN NULL
            ELSE STR_SPLIT(fighterdistance, ' of ')[1]::int
        END AS fighterdistance,
        CASE
            WHEN CONTAINS(fighterdistance, '-') THEN NULL
            ELSE STR_SPLIT(fighterdistance, ' of ')[2]::int
        END AS fighterdistanceatt,
        CASE
            WHEN CONTAINS(fighterclinch, '-') THEN NULL
            ELSE STR_SPLIT(fighterclinch, ' of ')[1]::int
        END AS fighterclinch,
        CASE
            WHEN CONTAINS(fighterclinch, '-') THEN NULL
            ELSE STR_SPLIT(fighterclinch, ' of ')[2]::int
        END AS fighterclinchatt,
        CASE
            WHEN CONTAINS(fighterground, '-') THEN NULL
            ELSE STR_SPLIT(fighterground, ' of ')[1]::int
        END AS fighterground,
        CASE
            WHEN CONTAINS(fighterground, '-') THEN NULL
            ELSE STR_SPLIT(fighterground, ' of ')[2]::int
        END AS fightergroundatt,
        CASE
            WHEN fighterid = STR_SPLIT(winningfighter, '/')[5] THEN True::BOOLEAN
            ELSE False::BOOLEAN
        END AS winner,
        CASE
            WHEN fighterid = STR_SPLIT(winningfighter, '/')[5] AND perf != '---' THEN True::BOOLEAN
            ELSE False::BOOLEAN
        END AS performancebonus,
        CASE
            WHEN fighterid = STR_SPLIT(winningfighter, '/')[5] AND sub != '---' THEN True::BOOLEAN
            ELSE False::BOOLEAN
        END AS submissionbonus,
        CASE
            WHEN fighterid = STR_SPLIT(winningfighter, '/')[5] AND ko != '---' THEN True::BOOLEAN
            ELSE False::BOOLEAN
        END AS knockoutbonus,
        CASE
            WHEN fight != '---' THEN True::BOOLEAN
            ELSE False::BOOLEAN
        END AS fightofthenight,
        judge1,
        CASE
            WHEN judge1_score = '---' THEN NULL
            ELSE STR_SPLIT(judge1_score, ' - ')[1]::int
        END AS judge1fighter1score,
        CASE
            WHEN judge1_score = '---' THEN NULL
            ELSE STR_SPLIT(judge1_score, ' - ')[2]::int
        END AS judge1fighter2score,
        judge2,
        CASE
            WHEN judge2_score = '---' THEN NULL
            ELSE STR_SPLIT(judge2_score, ' - ')[1]::int
        END AS judge2fighter1score,
        CASE
            WHEN judge2_score = '---' THEN NULL
            ELSE STR_SPLIT(judge2_score, ' - ')[2]::int
        END AS judge2fighter2score,
        judge3,
        CASE
            WHEN judge3_score = '---' THEN NULL
            ELSE STR_SPLIT(judge3_score, ' - ')[1]::int
        END AS judge3fighter1score,
        CASE
            WHEN judge3_score = '---' THEN NULL
            ELSE STR_SPLIT(judge3_score, ' - ')[2]::int
        END AS judge3fighter2score
    FROM
        raw.ufcstats_singlefights main
    INNER JOIN round_cte rcte USING (id)
