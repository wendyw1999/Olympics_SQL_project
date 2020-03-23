# 120 years of Olympics Games
Project Datasets: https://www.kaggle.com/heesoo37/120-years-of-olympic-history-athletes-and-results
https://www.kaggle.com/the-guardian/olympic-games#summer.csv

## Final Schema Diagram

### Description of Cleaning/ Preprocessing:

[P.S. Please see at the end of “Data Cleaning” processes out team’s discussion of information loss]

### Data importing:
```sql
Created five tables: 
athlete_events, noc_regions, summer, winter, dictionary.
```
Used “copy table_name from source” method to import csv files into these tables.
```
Create table  athlete_events(
Id serial,
Name varchar(200),
Sex char,
Height varchar(20),
Weight varchar(20),
Team varchar(200),
Games varchar (50),
Year int,
City varchar(50),
Sport varchar(50),
Event varchar(250),
Medal varchar(20));

Copy athlete_events
From ‘path/athlete_events.csv’ CSV encoding = ‘utf-8’ delimiter ‘,’ header;
```

### Data Cleaning:

- Summer, Winter tables cleaning
Firstly added a season column in summer, winter tables, and then merged them into one table called summer_winter with the season column denoting where each row came from.
```
alter table summer
             add column season varchar(10) not null default 'Summer';
alter table winter
             add column season varchar(10) not null default 'Winter';
create table summer_winter as select * from summer union all select * from winter;
Alter table summer_winter
Add column id serial primary key;
```

We also cleaned the ‘Athlete’ column in  summer_winter table column into lastname, firstname, and middlename by splitting on the delimiter “,” 
```
create materialized view temp_name as select
                    id,
                    (regexp_split_to_array("athlete",E', '))[1] as "lastname",
                    (regexp_split_to_array("athlete",E', '))[2] as "firstname"
                    from summer_winter_temp_table
create materialized view temp_name_6 as select id,
                                                            lower("temp_name_5"."lastname"),
                    (regexp_split_to_array("firstname",E'[. ]'))[1] as "firstname",
                    (regexp_split_to_array("firstname",E'[. ]'))[2] as "middlename"
                    from temp_name

Create table “summer_winter_cleaned) as (select T.id as "id",lower(lastname) as “lastname” ,lower(firstname) as “firstname”,lower(middlename) as “middlename”,year,city,sport,lower(S.athlete) as "fullname",country, gender,event,medal,season from temp_name_6 T, summer_winter_temp_table S where T.id = S.id);
```

Furthermore, we added a column “sex” which changes Men to M  and Femal to F in order to match the format of the athlete_events table. 
 create table summer_winter_updated as select id,lastname,firstname,middlename,fullname,country,gender,substring(gender,1,1) as "sex", year,season,city,sport,discipline,event,medal from summer_winter_cleaned;


### Athlete_events cleaning

Cleaned “medal” column according to piazza post. We performed the following code to get rid of duplicate medals obtained during the same game. This is done because there are some athletes who obtained multiple medals during the same game, and this might be that some races have been reran, and therefore there are multiple records to once race. First, we created a view called medal_duplicates which stores all the athlete,event,games information that have reruns and use array aggregate function to concatenate the two instances of the medal into  a single string. Then subtract this medal_duplicate from the original table to get a temp table that don’t have any duplicates. Finally union the medal_duplicate and the non_duplicate table to attain a cleaned table. The following code cleans, and gets rid of such records in medals.
```
create materialized view medal_duplicate as (select distinct A1.id, A1.event, A1.games, array_to_string(array_agg(distinct A1.medal),'-')
             from athlete A1, athlete A2
             where A1.id = A2.id and A1.event = A2.event
             and A1.games = A2.games and A1.medal <>A2.medal
             group by A1.id, A1.event, A1.games
             order by A1.id)

Create view remained as (select id,event,games from athlete_events except select id,event,games from medal_duplicates);

Create table athlete_event_games_table as (select * from athlete_events where id in (select id from remained) union all (select * from medal_duplicates));
```

Cleaned team names using the same above method to get rid of duplicate teams during the same game. Same issue as above, there were reruns in some Olympics games. 
```
create materialized view "team_duplicates" as (select distinct A1.id, A1.event, A1.games, array_to_string(array_agg(distinct A1.team) ,'-') as team
          from athlete A1, athlete A2
          where A1.id = A2.id and A1.event = A2.event
          and A1.games = A2.games and A1.team <> A2.team
          group by A1.id, A1.event, A1.games
          order by A1.id);

Create view remained_team as (select id,event,games from athlete except select id,event,games from team_duplicates);

create view athlete_event_games_team as select A.id,A.games,A.event,A.team from athlete A inner join remained_team B on A.id = B.id and A.event = B.event and A.games = B.games
union all (select id,games,event,team from team_duplicates);

select id,games,event from athlete_event_games_team group by id,games,event having count(distinct team)>1;

select distinct A.athlete_id,A.games,A.event_name,B.team,A.medal from athlete_event_games_table A,athlete_event_games_team B where A.athlete_id = B.id and A.games = B.games and A.games = B.games and A.event_name = B.event;

create table athlete_event_games_table
as select distinct A.athlete_id,A.games,A.event_name,B.team,A.medal from athlete_event_games_table A,athlete_event_games_team B where A.athlete_id = B.id and A.games = B.games and A.games = B.games and A.event_name = B.event;

alter table athlete_event_games_temp
rename to "athlete_event_games";
```
event_name is actually a composite value of the sport,event_gender (Men,Women, or Mixed) and the remaining event detail. So we broke down the column into three columns using regexp_split_to_array and substring. Also, we noticed that the format of the event_detail is different from the `event` column of the summer_winter table, so we cleaned the event_detail to match the format using nested regexp_replace.  
 
 ```
Create table event as select "event" as “event_name”,sport,
                    substring(event,E'(Men|Women|Mixed)') as "event_gender",
                    (regexp_split_to_array(event,E'( Men\'s | Women\'s | Mixed )'))[2] as event_detail
             from athlete_events;

create table event_temp as select distinct even_name,sport
                    regexp_replace(regexp_replace(regexp_replace(event_detail,',000','000'),' kilometres','KM'),' metres','M') as event_detail,event_gender from event;

Drop table event;
Alter table event_temp
Rename to “event”;

Iv. We created a medal_values table in order to solve query 8. We select all the distinct medal in athlete_events table (after cleaning the reruns) and give each a value. 
Cleaning noc_regions and dictionary. 

create table medal_values(
                 medal_type varchar(30),
                 value float             )

insert into medal_values
             values('Gold',3),('Silver',2),('Bronze',1),(‘NA’,0),('Gold-NA',1.5),('Gold-Silver',2.5),('Bronze-NA-Silver',1.5),('Bronze-NA',0.5),('Bronze-Gold',2),('NA-Silver',1),('Bronze-Silver',1.5)

```
I. We performed a full outer join of the two datasets on noc code. Then manually cleaned the table after googling, in order to ensure each noc code can functionally determine countryname. 
```
create view noc_temp as select A.code,A.countryname,A.population,A.gdp,B.notes from dictionary A full outer join noc_regions B on A.code = B.noc;
create view noc_temp as select A.code,A.countryname,A.population,A.gdp,B.notes from dictionary A full outer join noc_regions B on A.code = B.noc;

create view distinct_noc as select noc_dictionary.code,countryname,population,gdp,notes from noc_dictionary, duplicate_noc where noc_dictionary.code = duplicate_noc.code and countryname not like '%*%' and countryname != 'UK' and countryname != 'USA' and countryname != 'Burma'
             and countryname!='Korea, South' and countryname!='Congo, Dem Rep' and countryname != 'Antigua' and countryname != 'Boliva' and countryname != 'Cote d''Ivoire' and countryname != 'Virgin Islands, British'
             and countryname != 'Korea, North' and countryname!= 'Saint Vincent and the Grenadines' and countryname!= 'Palestine, Occupied Territories' and countryname!= 'Saint Kitts and Nevis' and countryname != 'Congo' and                                                          countryname != 'East Timor (Timor-Leste)';

create table noc_dictionary as select * from noc_temp where code not in (select code from distinct_noc) union all select * from distinct_noc. 
Noc code functionally determines the countryname,gdp,and population
```

## Discussion of Information Loss
athlete_events and summer_winter table are not merged together:
After trying out functional dependencies we found in summer_winter table with their relationship with athelete_events, we found that it makes most sense to draw connection between the two on the primary Name column
However, while Athlete in summer_winter table has a clear notation to identify athletes’ first, middle and last names, Name in athlete_events does not.
After trying out multiple ways to classify athletes’ first names, middle names and last names in the athlete_info table, none of the methods we try match names in athlete_info table with those in summer_winter well.
Our team decides to leave out names from summer_winter table for the following reasons:
There are notably more unique athletes in athlete_events than those in summer_winter
While there is no way we can think of to accurately compare names in the two tables, this raises a range of problems if we simply union the two tables:
We are not able to compare if two names from the two tables are the same person -- there is no exact way to identify the delimiters in the names to format the names the same in the two columns. And therefore comparison is always off. Also, where there is no ids assigned to athletes in summer_winter table, and if it doesn’t make sense to randomly assign them for we have no idea if there are athletes with different names.
Although there will definitely be information loss generated by not including information from summer_winter table, our team believes that it more reasonable to accept this information loss compared to operating/querying on a big relation of which we cannot properly reason what the table actually has 
(i.e. We believe the quality of the data is more important that quantity of the data for our queries’ purposes here)

# FDs and 3NF

## Determine the FDs using sql “group by A having count(distinct B)>1” method
(P.S. The following functional dependencies are based on the relations after data cleaning and processing)
athlete_id → firstname, lastname, height, weight, sex
games → year, season
athlete_id, team → noc
athlete_id, year, season → age, noc
athlete_id, year, season, event → medal, team
event_name → sport, event_gender, event_detail
sport, year, season → city
noc→ countryname, population, gdp ,notes

We firstly break the relations down according to these functional dependencies, and going from here, combined with knowledge we have in regards the attributes, we come up with the following schema:
athlete_info: id, firstname, lastname, height, weight, sex. 
Every attribute is atomic. Id uniquely identifies all other attributes. The table is in 2NF because there is only one key so no partial key, and there is no transitive functional dependencies in this table as all the attributes depend on and only on the primary key and therefore it is in 3NF. 
athlete_games_info: id, year, season, age, noc
Combination of athlete_id, season and year uniquely identifies each entry in this relation. All data are in atomic form. It is in 2NF, and there does not exist further transitive functional dependencies, therefore it is in 3NF.
athlete_event_games: athlete_id, year, season, event_name, medal, team
The primary key is a combination of athlete_id, event_name, season and year. Each attribute is atomic therefore it is in 1NF. Medal and team are each determined only by the primary key together instead of being determined by any partial key, therefore it is in 2NF. There are no other transitive functional dependencies in the relation. Therefore it is in 3NF
event: event_name, sport, event_gender, event_detail
An event is uniquely identified by event_name. (As specified by teaching assistant on Piazza), we are advised to treat event as atomic. Therefore it is already in 1NF. Furthermore, there do not exist additional partial keys, and therefore this relation is in 2NF. No transitive functional dependencies exist. Therefore it is in 3NF.
noc_dictionary: noc, countryname, population, gdp, notes. NOC code determines the countryname, population, gdp and notes. There is no partial key. Therefore it is in 3NF.
games_event: year, event, city
Combinations of year, event uniquely identifies each entry in this table, and no transitive functional dependencies exist. Year and event determine the city in which the Olympic is held because there was a time where some events were held in Australia and some events were held in Sweden. City can only be determined by games_id and event together, and cannot be determined one without the other. Therefore jointly, it is in 3NF.
medal_value: medal_type, value
medal_type is the primary key here, and each corresponds to a value of {‘NA’:0, ‘Bronze’: 1, ‘Silver’: 2, ‘Gold’: 3}. No other transitive functional dependencies exist, values are not in atomic forms but are considered as atomic for this project’s sake, and therefore this relation is in 3NF.

#Queries and Results

Our team believes that it is more acceptable to admit the information loss from not including possible results from summer_winter table than to union the two tables (reasons provided above in Data Processing procedure)
The following queries and query results is based on relation without unionizing athletes from summer_winter table, and therefore our output rows might contain less than the ideal outputs

### For the year 1992 in Barcelona display the country name and the total number of competitors from that country, including those countries that have no competitors, in descending order of the number of competitors. Remember not all countries participate in every Olympics game. 
```
create view temp_query_1 as 
select distinct A.athlete_id,A.season,A.year
from games_event_info G,athlete_event_gamesA
where A.year = 1992 and G.city = 'Barcelona' and A.year = G.year and A.event_name = G.event;

create view temp_query_12 as 
select A.athlete_id,countryname,noc
from temp_query_1 T,athlete_games_info A,noc_dictionary N
where T.athlete_id = A.athlete_id and T.year = A.year and T.season = A.season and noc = code;

select distinct N.countryname,count(distinct(athlete_id)) 
from temp_query_12 T
full outer join noc_dictionary N on T.countryname = N.countryname
group by N.countryname
order by count(distinct(athlete_id)) desc;
```








### For the Vancouver (2010) games, list the competitor countries in the Curling competition.
```
select distinct countryname
from games_event_info GE,athlete_event_games AEG,event E,athlete_info A,athlete_games_info AG,noc_dictionary
where AEG.year = '2010' and GE.city = 'Vancouver' and E.sport = 'Curling' and AEG.event_name = E.event_name and A.id = AEG.athlete_id and noc = code and A.id = AG.athlete_id and AG.season = AEG.season and AEG.year = AG.year and GE.year = AEG.year;
```





### List all the competitors who have competed in more than 4 events in any Olympics since 1900.
```
select distinct athlete_id,fullname
from athlete_event_games AEG,athlete_info A
where year >=1900 and athlete_id = A.id
group by "athlete_id","fullname",year,season having count(athlete_id)>4 ;
```


### Find the number of competitors in each Olympic game who have competed in at least 3 events and group them by year, after 1940.
```
select G.year,count(distinct athlete_id) 
from
   (select athlete_id,year,season from athlete_event_games A
 group by athlete_id,year,season
 having count(distinct event_name) > 2) as foo,
"games_table" G 
where foo.year =G.year and foo.season = G.season and G.year > 1940
group by G.year;

Row number = 24
```
### Count the number of competitors who were from India in every Olympics held since 1947.
```
select count(distinct(AGI.athlete_id)) 
from noc_dictionary N,athlete_event_games AEG,athlete_info A ,athlete_games_info AGI
where countryname = 'India' and A.id = AEG.athlete_id and AGI.athlete_id = A.id and AGI.noc = N.code and AGI.year = AEG.year and AGI.season = AGI.season and AEG.year >= 1947;
```



### Display the results of swimming events (both men and women) in the 2004 Olympics
```
select A.fullname,E.event_name,medal 
from event E,athlete_event_games AEG, athlete_info A
where sport = 'Swimming'  and AEG.year = '2004' and E.event_name = AEG.event_name and A.id = AEG.athlete_id;
```


### Find the medal counts for Michael Phelps across multiple years, show it by year and medal type
```
select year, medal, count(*) 
from athlete_info A,athlete_event_games AEG 
where A.fullname like '%michael%phelps%' and A.id = AEG.athlete_id
and medal != 'NA'
group by year,medal;
```


### Which country has won the most Gold medals in the men’s marathon?
```	
drop view temp_query_81 cascade;
create view temp_query_81 as 
select noc from event E,athlete_event_games AEG,athlete_games_info A
where event_gender = 'Men' and event_detail = 'Marathon'
and AEG.athlete_id = A.athlete_id and AEG.year = A.year and AEG.season = A.season and AEG.event_name =E.event_name and medal = 'Gold';
select countryname from temp_query_81,noc_dictionary where noc = code
group by noc,countryname
having count(*) = (select max(C) from (select count(*) as C from temp_query_81 group by noc) as A);

```


### Find the names of competitors who have improved or maintained their standing across three games. Use years after 1940.
```
create materialized view temp_query_9 as
select athlete_id,event_name,year,medal_type,value
from athlete_event_games,medal_values
where medal!='NA' and medal = medal_type;

select distinct A.athlete_id,AI.fullname 
from temp_query_9 A,temp_query_9 B,temp_query_9 C, athlete_info AI 
where A.athlete_id = B.athlete_id and B.athlete_id = C.athlete_id
and A.event_name = B.event_name and B.event_name = C.event_name
and A.year > B.year and B.year > C.year and A.year > C.year and A.value >= B.value and B.value >= C.value and C.year >= 1940
and A.athlete_id = AI.id;
```

