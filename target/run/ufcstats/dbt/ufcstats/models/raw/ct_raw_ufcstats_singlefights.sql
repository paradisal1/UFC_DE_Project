
  
    
    

    create  table
      "ufcstats"."main_raw"."ct_raw_ufcstats_singlefights__dbt_tmp"
  
    as (
      WITH F1 AS (
        SELECT
            eventname,
            eventdate,
            eventlocation,
            id,
            weightclass,
            method,
            round,
            time,
            timeformat,
            referee,
            fighter1id fighterid,
            fighter1 fightername,
            fighter1nickname fighternickname,
            fighter1kd fighterkd,
            fighter1sigstr fightersigstr,
            fighter1sigstrpct fightersigstrpct,
            fighter1totalstr fightertotalstr,
            fighter1td fightertd,
            fighter1tdpct fightertdpct,
            fighter1subatt fightersubatt,
            fighter1rev fighterrev,
            fighter1ctrl fighterctrl,
            fighter1head fighterhead,
            fighter1body fighterbody,
            fighter1leg fighterleg,
            fighter1distance fighterdistance,
            fighter1clinch fighterclinch,
            fighter1ground fighterground,
            fighter2id opponentid,
            winningfighter,
            perf,
            sub,
            fight,
            ko,
            judge1,
            judge1_score,
            judge2,
            judge2_score,
            judge3,
            judge3_score
        FROM
            raw.ufcstatsfighttable)
    SELECT
        eventname,
        eventdate,
        eventlocation,
        id,
        weightclass,
        method,
        round,
        time,
        timeformat,
        referee,
        fighter2id fighterid,
        fighter2 fightername,
        fighter2nickname fighternickname,
        fighter2kd fighterkd,
        fighter2sigstr fightersigstr,
        fighter2sigstrpct fightersigstrpct,
        fighter2totalstr fightertotalstr,
        fighter2td fightertd,
        fighter2tdpct fightertdpct,
        fighter2subatt fightersubatt,
        fighter2rev fighterrev,
        fighter2ctrl fighterctrl,
        fighter2head fighterhead,
        fighter2body fighterbody,
        fighter2leg fighterleg,
        fighter2distance fighterdistance,
        fighter2clinch fighterclinch,
        fighter2ground fighterground,
        fighter1id opponentid,
        winningfighter,
        perf,
        sub,
        fight,
        ko,
        judge1,
        judge1_score,
        judge2,
        judge2_score,
        judge3,
        judge3_score
    FROM
        raw.ufcstatsfighttable
    UNION ALL
    SELECT * FROM F1
    );
  
  