CREATE TABLE IF NOT EXISTS challengers_rank (
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