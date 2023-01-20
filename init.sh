#!/bin/sh

psql --dbname=postgres --username=admin <<EOF
DROP TABLE IF EXISTS challengers_rank;
CREATE TABLE challengers_rank (
    DimKey int,
    summonerId varchar(255),
    leaguePoints int,
    wins int,
    losses int,
    summonerName varchar(255),
    win_loss_ratio float,
    ValidFrom date,
    ValidTo date,
    isCurrent int
);
EOF