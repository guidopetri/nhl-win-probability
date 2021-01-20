create table team_goalies(
    id        serial primary key,
    team_id   int    references teams(id),
    season    text   not null,
    goalie_id int    references goalies(id)
);