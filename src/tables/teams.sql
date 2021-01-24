create table teams(
    id              serial   primary key,
    team_id         smallint not null,
    team_name       text     not null,
    team_short_name text     not null
);
