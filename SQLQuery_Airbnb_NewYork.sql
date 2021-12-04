--Viewing the dataset
---------------------
select * from DataAnalysis..['listings New York$']
order by id

--Data Cleaning and checking (used substring, replace, cast functions)
select property_type,cast(replace(substring(price,2,10),',','') as float) as Pricing from DataAnalysis..['listings New York$']
--where property_type='Entire condominium (condo)'
where cast(replace(substring(price,2,10),',','') as float)>0
order by Pricing desc

--Overall Data
select  count(*) as 'Listings',sum(accommodates) as Accomodations,sum(number_of_reviews) as 'Reviews'
,round(avg(review_scores_rating),2) as 'Average Ratings',concat('$',round(avg(Pricing),2)) as 'Average Pricing'
from 
(
select *,cast(replace(substring(price,2,10),',','') as float) as Pricing from DataAnalysis..['listings New York$']
) t
where Pricing>0

--Querying hosts and details who live in the same neighborhood as their listings
--Using self INNER JOINS
-------------------------------------------------------------------------------------------------------
select distinct(a.host_id), a.HOST_NAME, a.host_neighbourhood from DataAnalysis..['listings New York$'] a inner join DataAnalysis..['listings New York$'] b
on a.host_neighbourhood=b.neighbourhood_cleansed 
and a.host_name=b.host_name
order by a.host_neighbourhood, a.host_id

select a.host_neighbourhood, count(distinct(a.host_id)) as Hosts_live_here from DataAnalysis..['listings New York$'] a inner join DataAnalysis..['listings New York$'] b
on a.host_neighbourhood=b.neighbourhood_cleansed 
and a.host_name=b.host_name
group by a.host_neighbourhood
order by a.host_neighbourhood

--Further details
select t.host_name, t.host_neighbourhood, t.name, t.neighborhood_overview, t.Neighbourhood, t.property_type, t.room_type, t.accommodates, t.price, t.review_scores_rating, t.number_of_reviews
from
(
select distinct(a.host_id), a.host_name, a.host_neighbourhood, a.name, a.neighborhood_overview, a.neighbourhood_group_cleansed as Neighbourhood, a.property_type, a.room_type, a.accommodates, a.price, a.review_scores_rating , a.number_of_reviews
from DataAnalysis..['listings New York$'] a inner join DataAnalysis..['listings New York$'] b
on a.host_neighbourhood=b.neighbourhood_cleansed 
and a.host_name=b.host_name
where a.neighbourhood is not null and
a.review_scores_rating is not null
) t
order by t.host_neighbourhood asc, (t.review_scores_rating*t.number_of_reviews) desc

--Further details of specific Neighbourhoods
--Using PROCEDURES
--Using Temporary Tables
--------------------------------------------

drop procedure if exists dbo.HostInSameNeighbourhood 


go
create procedure dbo.HostInSameNeighbourhood
@neighbourhood nvarchar(100)				--PARAMETER LOCATION
as
drop table if exists hostinfo
create table hostinfo(
Host_Name varchar(100),
Host_Neighbourhood varchar(100),
Name varchar(400),
Neighbourhood_Overview varchar(5000),
Neighbourhood_Group varchar(100),
Property varchar(100),
Room varchar(100),
Accomodates int,
Price float,
Score float,
Reviews float
)

insert into hostinfo
select t.host_name, t.host_neighbourhood, t.name, t.neighborhood_overview, t.Neighbourhood, t.property_type, t.room_type, t.accommodates, t.price, t.review_scores_rating, t.number_of_reviews
from
(
select distinct(a.host_id), a.host_name, a.host_neighbourhood, a.name, a.neighborhood_overview, a.neighbourhood_group_cleansed as Neighbourhood, a.property_type, a.room_type, a.accommodates, cast(replace(substring(a.price,2,10),',','') as float) as price, a.review_scores_rating , a.number_of_reviews
from DataAnalysis..['listings New York$'] a inner join DataAnalysis..['listings New York$'] b
on a.host_neighbourhood=b.neighbourhood_cleansed 
and a.host_name=b.host_name
where a.neighbourhood is not null and
a.review_scores_rating is not null
) t
where t.Neighbourhood=@neighbourhood			--PARAMETER LOCATION
and t.price>0
and t.review_scores_rating>0
and t.number_of_reviews>10
order by t.host_neighbourhood asc, (t.review_scores_rating*t.number_of_reviews) desc

select * from hostinfo
go

exec HostInSameNeighbourhood @neighbourhood='Bronx'
exec HostInSameNeighbourhood @neighbourhood='Brooklyn'
exec HostInSameNeighbourhood @neighbourhood='Manhattan'
exec HostInSameNeighbourhood @neighbourhood='Queens'
exec HostInSameNeighbourhood @neighbourhood='Staten Island'

--For viewing the trend(rolling count) of listings by new hosts
--Using PARTITION BY
---------------------------------------------------------------
select
Pricing, Dates as HostDates,
ROW_NUMBER() over(partition by Dates order by Dates) as Counts
from 
(
select *, 
cast(replace(substring(price,2,10),',','') as float) as Pricing,
cast(host_since as date) as Dates
from
DataAnalysis..['listings New York$']
) t
where 
Pricing>0
and Dates is not null
and year(Dates)>2000
order by Dates

--Quantitative details about units and prices
--Using COUNT, MIN, MAX, AVG
--Using TEMPORARY TABLES
----------------------------------------------------------

drop table if exists Temp_table1
create table Temp_table1(
category varchar(200),
Units int,
MinPrice float,
MaxPrice float,
AvgPrice float
)

insert into Temp_table1
select property_type, 
count(property_type),
min(cast(replace(substring(price,2,10),',','') as float)),
max(cast(replace(substring(price,2,10),',','') as float)),
Round(AVG(cast(replace(substring(price,2,10),',','') as float)),2)
from DataAnalysis..['listings New York$']
where cast(replace(substring(price,2,10),',','') as float)>0
group by property_type 

select * from Temp_table1

select category, concat(MinPrice,' - ', MaxPrice) as Range,AvgPrice from Temp_table1

drop table if exists Temp_table2
create table Temp_table2(
category varchar(200),
Units int,
MinPrice float,
MaxPrice float,
AvgPrice float
)

insert into Temp_table2
select room_type, 
count(room_type),
min(cast(replace(substring(price,2,10),',','') as float)),
max(cast(replace(substring(price,2,10),',','') as float)),
Round(AVG(cast(replace(substring(price,2,10),',','') as float)),2)
from DataAnalysis..['listings New York$']
where cast(replace(substring(price,2,10),',','') as float)>0
group by room_type 

select category, Units, concat(MinPrice,' - ', MaxPrice) as Range,AvgPrice from Temp_table2

/*
category		Units	Range		AvgPrice
Entire home/apt	20063	10 - 10000	217.04
Hotel room		207		50 - 1351	371.84
Private room	16828	10 - 10000	102.95
Shared room		576		15 - 10000	129.66
*/

drop table if exists Temp_table3
create table Temp_table3(
category varchar(200),
Units int,
MinPrice float,
MaxPrice float,
AvgPrice float
)

insert into Temp_table3
select neighbourhood_group_cleansed, 
count(neighbourhood_group_cleansed),
min(cast(replace(substring(price,2,10),',','') as float)),
max(cast(replace(substring(price,2,10),',','') as float)),
Round(AVG(cast(replace(substring(price,2,10),',','') as float)),2)
from DataAnalysis..['listings New York$']
where cast(replace(substring(price,2,10),',','') as float)>0
group by neighbourhood_group_cleansed 

select category, Units, concat(MinPrice,' - ', MaxPrice) as Range,AvgPrice from Temp_table3

/*
category		Units	Range		AvgPrice
Bronx			1058	11 - 2000	105.85
Brooklyn		14507	10 - 9999	136.48
Manhattan		16592	10 - 10000	212.14
Queens			5178	10 - 10000	113.37
Staten Island	339		10 - 1200	117.45
*/

drop table if exists Temp_table04
create table Temp_table04(
category varchar(200),
Units int,
MinPrice float,
MaxPrice float,
AvgPrice float
)

insert into Temp_table04
select neighbourhood_cleansed, 
count(neighbourhood_cleansed),
min(cast(replace(substring(price,2,10),',','') as float)),
max(cast(replace(substring(price,2,10),',','') as float)),
Round(AVG(cast(replace(substring(price,2,10),',','') as float)),2)
from DataAnalysis..['listings New York$']
where cast(replace(substring(price,2,10),',','') as float)>0
and review_scores_rating>0
group by neighbourhood_cleansed 

select category, Units, concat(MinPrice,' - ', MaxPrice) as Range,AvgPrice from Temp_table04
where category='Allerton'
--order by category



--Querying ratings info
-----------------------

drop table if exists Review_table1
create table Review_table1(
Name varchar(300),
Neighbourhood_group varchar(100),
Neighbourhood varchar(200),
Property_type varchar(100),
Room_type  varchar(100),
Price float, 
Reviews int,
Score float,
Score_Accuracy float,
Cleanliness float,
Checkin float,
Communication float,
Location float,
Value float
)

insert into Review_table1
select 
name, 
neighbourhood_group_cleansed, 
neighbourhood_cleansed, 
property_type, 
room_type, 
cast(replace(substring(price,2,10),',','') as float),
number_of_reviews,
review_scores_rating,
review_scores_accuracy,
review_scores_cleanliness,
review_scores_checkin,
review_scores_communication,
review_scores_location,
review_scores_value
from DataAnalysis..['listings New York$']

--Cleaning data
select * from Review_table1
where Score is not null
and Price>0
--and Reviews>0

--Categorizing
select Neighbourhood, round(avg(Score),2) as AvgScore from Review_table1
where Score is not null
and Price>0
group by Neighbourhood
order by Neighbourhood

select Property_type, round(avg(Score),2) as AvgScore from Review_table1
where Score is not null
and Price>0
group by Property_type
order by Property_type

select Room_type, round(avg(Score),2) as AvgScore from Review_table1
where Score is not null
and Price>0
group by Room_type
order by Room_type

select Room_type, round((sum(Score*Reviews)/sum(Reviews)),2) as AvgModScore from Review_table1
where Score is not null
and Price>0
group by Room_type
order by Room_type

--Property type wise
--Checking range and average of ratings
select Property_type, concat(min(Score),' - ', max(Score)) as Range_of_ratings, AVG(Score) as Aerage_ratings from Review_table1 
where Score is not null
and Price>0
and Reviews>10 --for reliable scores
--and Reviews>0
group by Property_type

--Room type wise
--Checking range and average of ratings
select Room_type, concat(min(Score),' - ', max(Score)) as Range_of_ratings, AVG(Score) as Aerage_ratings from Review_table1 
where Score is not null
and Price>0
and Reviews>10 --for reliable scores
--and Reviews>0
group by Room_type
/*
Room_type			Range_of_ratings		Aerage_ratings
Hotel room			3.37 - 5				4.43
Shared room			3.79 - 5				4.68255555555556
Private room		2.36 - 5				4.72813807144178
Entire home/apt		3.5 - 5					4.76333653978286
*/

--Neighbourhood wise
--Checking range and average of ratings
select Neighbourhood, concat(min(Score),' - ', max(Score)) as Range_of_ratings, AVG(Score) as Aerage_ratings from Review_table1 
where Score is not null
and Price>0
and Reviews>10 --for reliable scores
--and Reviews>0
group by Neighbourhood


--Checking min
select top 1 Neighbourhood, min(Score) as Minimum from Review_table1 
where Score is not null
and Price>0
and Reviews>10 --for reliable scores
--and Reviews>0
group by Neighbourhood
order by Minimum


--Neighbourhood wise
--Checking range and average of ratings
select Neighbourhood_group, concat(min(Score),' - ', max(Score)) as Range_of_ratings, AVG(Score) as Aerage_ratings from Review_table1 
where Score is not null
and Price>0
and Reviews>10 --for reliable scores
--and Reviews>0
group by Neighbourhood_group
/*
Neighbourhood	Range_of_ratings	Aerage_ratings
Brooklyn		3.87 - 5			4.76833826794965
Bronx			3.83 - 5			4.7424838012959
Manhattan		2.36 - 5			4.72136335209502
Staten Island	4.08 - 5			4.76916666666667
Queens			3.64 - 5			4.74065934065933
*/

------------
--ANALYSIS |
------------

--Do HIGH REVIEWS tend to be associated with MORE EXPENSIVE or LESS EXPENSIVE LISTINGS?
---------------------------------------------------------------------------------------

--Checking Correlation of price and ratings using Pearson's formula
-------------------------------------------------------------------
/*
Pearson's formula
r = [ n(SUM(xy)-SUM(x)SUM(y) ] / [ sqrt (nSUM(x^2)-(SUM(x)^2))(nSUM(y^2)-(SUM(y)^2)) ]

r=correlation coefficient
if r-->1 strong correlation
if r-->-1 poor correaltion

r=(n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))
*/
-------------------------------------------------------------------

select n, Sx,Sy,Sxy,Sx2,Sy2,(n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from
(
select 
count(*) as n, 
sum(price) as Sx,
sum(Score) as Sy,
sum(price*Score) as Sxy,
sum(power(price,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table1
where Score is not null
and Price>0
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error
---------------------------------------------------------------------------------------
--Overall there was a WEAK POSITIVE CORRELATION between MORE EXPENSIVE & HIGH REVIEWS |
---------------------------------------------------------------------------------------

--Displaying correlation Neighbourhood wise
select Neighbourhood, (n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from --n, Sx,Sy,Sxy,Sx2,Sy2,
(
select 
Neighbourhood,
count(*) as n, 
sum(price) as Sx,
sum(Score) as Sy,
sum(price*Score) as Sxy,
sum(power(price,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table1
where Score is not null
and Price>0
group by Neighbourhood
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error

--------------------------------------------------------------------------------------------------
--There was a WEAK POSITIVE CORRELATION between MORE EXPENSIVE & HIGH REVIEWS for Neighbourhoods |
--------------------------------------------------------------------------------------------------

--Displaying correlation Property type wise
select Property_type, (n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from --n, Sx,Sy,Sxy,Sx2,Sy2,
(
select 
Property_type,
count(*) as n, 
sum(price) as Sx,
sum(Score) as Sy,
sum(price*Score) as Sxy,
sum(power(price,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table1
where Score is not null
and Price>0
and Score>0
group by Property_type
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error

--Displaying correlation Room type type wise
select Room_type, (n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from --n, Sx,Sy,Sxy,Sx2,Sy2,
(
select 
Room_type,
count(*) as n, 
sum(price) as Sx,
sum(Score) as Sy,
sum(price*Score) as Sxy,
sum(power(price,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table1
where Score is not null
and Price>0
and Score>0
and Reviews>50 --to see more reliable reviews
group by Room_type
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error

-------------------------------------------------------------------------------------------------------------------------------
--Overall there was a WEAK POSITIVE CORRELATION between MORE EXPENSIVE & HIGH REVIEWS for Hotels, Shared rooms & Entire Homes |
--& there was a VERY WEAK NEGATIVE CORRELATION between MORE EXPENSIVE & HIGH REVIEWS for Private Rooms						  |
-------------------------------------------------------------------------------------------------------------------------------

--Displaying DETAILED correlation Room type type wise in Neighbourhoods
select Neighbourhood, Room_type, (n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R 
from --n, Sx,Sy,Sxy,Sx2,Sy2,
(
select 
Neighbourhood,Room_type,
count(*) as n, 
sum(price) as Sx,
sum(Score) as Sy,
sum(price*Score) as Sxy,
sum(power(price,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table1
where Score is not null
and Price>0
group by Neighbourhood, Room_type
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error
order by Neighbourhood,Room_type
/*
Neighbourhood	Room_type				Pearson's R
Bronx			Entire home/apt		 0.0730143761665861
Bronx			Private room		-0.0806435070687292
Bronx			Shared room			 0.598768036753285
Brooklyn		Entire home/apt		 0.0146351679548435
Brooklyn		Hotel room			 0.0427414184105248
Brooklyn		Private room		 0.000879055653266063
Brooklyn		Shared room			 0.0188612179936523
Manhattan		Entire home/apt		 0.0250052568046383
Manhattan		Hotel room			 0.0669069528323807
Manhattan		Private room		 0.0127105849369331
Manhattan		Shared room			 0.0163970320537131
Queens			Entire home/apt		-0.00795699866522247
Queens			Hotel room			 0.615288655291931
Queens			Private room		 0.0162201290587321
Queens			Shared room			-0.0102678623254058
Staten Island	Entire home/apt		-0.00258930852369308
Staten Island	Private room		 0.0700163898636942
Staten Island	Shared room			 0.999999999999992
*/


--Do HIGH REVIEWS tend to be associated with MORE BEDROOMS & BATHROOMS or LESS?
-------------------------------------------------------------------------------

--Extracting bathroom and bedroom count
--Using PATINDEX

select 
cast(
isnull(
replace(bathrooms_text,
(substring(bathrooms_text,PATINDEX('%[a-z]%',bathrooms_text),len(bathrooms_text))),
'')
,0) as float) as Bathrooms 
from DataAnalysis..['listings New York$']
order by id

select cast(isnull(bedrooms,0) as float) as Bedrooms from DataAnalysis..['listings New York$']
order by id

--Querying bedrooms & bathrooms info
------------------------------------

drop table if exists Review_table2
create table Review_table2(
Name varchar(300),
Neighbourhood varchar(100),
Property_type varchar(100),
Room_type  varchar(100),
Price float, 
Reviews int,
Score float,
Score_Accuracy float,
Cleanliness float,
Checkin float,
Communication float,
Location float,
Value float,
Bedrooms float,
Bathrooms float
)

insert into Review_table2
select name, 
neighbourhood_group_cleansed, 
property_type, room_type, 
cast(replace(substring(price,2,10),',','') as float),
number_of_reviews,
review_scores_rating,
review_scores_accuracy,
review_scores_cleanliness,
review_scores_checkin,
review_scores_communication,
review_scores_location,
review_scores_value,
cast(isnull(bedrooms,0) as float),
cast(
isnull(
replace(bathrooms_text,
(substring(bathrooms_text,PATINDEX('%[a-z]%',bathrooms_text),len(bathrooms_text))),
'')
,0) as float)
from DataAnalysis..['listings New York$']

--Validating the data
select * from Review_table2

select name,Bedrooms,Bathrooms from Review_table2
where name='BEST BET IN HARLEM' or
name ='Lovely Room 1, Garden, Best Area, Legal rental' or
name ='Midtown Pied-a-terre'

select name, bedrooms,bathrooms_text from DataAnalysis..['listings New York$']
where name='BEST BET IN HARLEM' or
name ='Lovely Room 1, Garden, Best Area, Legal rental' or
name ='Midtown Pied-a-terre'

--Checking Correlation of bedrooms and ratings using Pearson's formula
----------------------------------------------------------------------

select n, Sx,Sy,Sxy,Sx2,Sy2,(n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from
(
select 
count(*) as n, 
sum(Bedrooms) as Sx,
sum(Score) as Sy,
sum(Bedrooms*Score) as Sxy,
sum(power(Bedrooms,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table2
where Score is not null
and Price>0
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error

--------------------------------------------------------------------------------------
--Overall there was a WEAK POSITIVE CORRELATION between MORE BEDROOMS & HIGH REVIEWS |
--------------------------------------------------------------------------------------

--Displaying correlation Neighbourhood wise
select Neighbourhood, (n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from --n, Sx,Sy,Sxy,Sx2,Sy2,
(
select 
Neighbourhood,
count(*) as n, 
sum(Bedrooms) as Sx,
sum(Score) as Sy,
sum(Bedrooms*Score) as Sxy,
sum(power(Bedrooms,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table2
where Score is not null
and Price>0
group by Neighbourhood
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error

----------------------------------------------------------------------------------------------------------------
--There was a WEAK POSITIVE CORRELATION between MORE BEDROOMS & HIGH REVIEWS for Brooklyn, Manhattan, & Queens |
--There was a WEAK NEGATIVE CORRELATION between MORE BEDROOMS & HIGH REVIEWS for Bronx & Staten Island		   |
----------------------------------------------------------------------------------------------------------------

--Displaying correlation Property type wise
select Property_type, (n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from --n, Sx,Sy,Sxy,Sx2,Sy2,
(
select 
Property_type,
count(*) as n, 
sum(Bedrooms) as Sx,
sum(Score) as Sy,
sum(Bedrooms*Score) as Sxy,
sum(power(Bedrooms,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table2
where Score is not null
and Price>0
and Score>0
group by Property_type
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error

--Displaying correlation Room type type wise
select Room_type, (n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from --n, Sx,Sy,Sxy,Sx2,Sy2,
(
select 
Room_type,
count(*) as n, 
sum(Bedrooms) as Sx,
sum(Score) as Sy,
sum(Bedrooms*Score) as Sxy,
sum(power(Bedrooms,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table2
where Score is not null
and Price>0
and Score>0
group by Room_type
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error
----------------------------------------------------------------------------------------------------------------
--There was a WEAK POSITIVE CORRELATION between MORE BEDROOMS & HIGH REVIEWS for Private Rooms				   |
--There was a WEAK NEGATIVE CORRELATION between MORE BEDROOMS & HIGH REVIEWS for Hotels & Entire Homes		   |
----------------------------------------------------------------------------------------------------------------


----Displaying correlation Room type type wise in Neighbourhoods for better insights
select Neighbourhood, Room_type, (n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from --n, Sx,Sy,Sxy,Sx2,Sy2,
(
select 
Neighbourhood,Room_type,
count(*) as n, 
sum(Bedrooms) as Sx,
sum(Score) as Sy,
sum(Bedrooms*Score) as Sxy,
sum(power(Bedrooms,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table2
where Score is not null
and Price>0
group by Neighbourhood, Room_type
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error
order by Neighbourhood,Room_type
/*
Neighbourhood	Room_type				Pearson's R
Bronx			Entire home/apt		-0.0401065219845552
Bronx			Private room		-0.0547417473532595
Brooklyn		Entire home/apt		-0.00632190668507133
Brooklyn		Hotel room			-0.192524380444772
Brooklyn		Private room		 0.0187287904572153
Manhattan		Entire home/apt		-0.00780423268295889
Manhattan		Hotel room			 0.000997277138749698
Manhattan		Private room		 0.0219087848092802
Queens			Entire home/apt		-0.0334220989052496
Queens			Private room		 0.0114437626131096
Staten Island	Entire home/apt		-0.0733728700814781
Staten Island	Private room		-0.0163007555423255
*/

--Checking Correlation of bathrooms and ratings using Pearson's formula
-----------------------------------------------------------------------
select n, Sx,Sy,Sxy,Sx2,Sy2,(n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from
(
select 
count(*) as n, 
sum(Bathrooms) as Sx,
sum(Score) as Sy,
sum(Bathrooms*Score) as Sxy,
sum(power(Bathrooms,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table2
where Score is not null
and Price>0
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error

--------------------------------------------------------------------------------------------
--Overall there was a VERY WEAK POSITIVE CORRELATION between MORE BATHROOMS & HIGH REVIEWS |
--------------------------------------------------------------------------------------------

--Displaying correlation Neighbourhood wise
select Neighbourhood, (n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from --n, Sx,Sy,Sxy,Sx2,Sy2,
(
select 
Neighbourhood,
count(*) as n, 
sum(Bathrooms) as Sx,
sum(Score) as Sy,
sum(Bathrooms*Score) as Sxy,
sum(power(Bathrooms,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table2
where Score is not null
and Price>0
group by Neighbourhood
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error

--------------------------------------------------------------------------------------------------------------
--There was a WEAK POSITIVE CORRELATION between MORE BATHROOMS & HIGH REVIEWS for Bronx, Manhattan, & Queens |
--There was a WEAK NEGATIVE CORRELATION between MORE BATHROOMS & HIGH REVIEWS for Brooklyn & Staten Island   |
--------------------------------------------------------------------------------------------------------------

--Displaying correlation Room type type wise
select Room_type, (n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from --n, Sx,Sy,Sxy,Sx2,Sy2,
(
select 
Room_type,
count(*) as n, 
sum(Bathrooms) as Sx,
sum(Score) as Sy,
sum(Bathrooms*Score) as Sxy,
sum(power(Bathrooms,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table2
where Score is not null
and Price>0
and Score>0
group by Room_type
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error
----------------------------------------------------------------------------------------------------------
--There was a WEAK POSITIVE CORRELATION between MORE BATHROOMS & HIGH REVIEWS for Hotels & Entire Homes  |
--There was a WEAK NEGATIVE CORRELATION between MORE BATHROOMS & HIGH REVIEWS for Shared & Private Rooms |
----------------------------------------------------------------------------------------------------------

----Displaying correlation Room type type wise in Neighbourhoods for better insights
select Neighbourhood, Room_type, (n*Sxy-Sx*Sy)/sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2))) as R from --n, Sx,Sy,Sxy,Sx2,Sy2,
(
select 
Neighbourhood,Room_type,
count(*) as n, 
sum(Bedrooms) as Sx,
sum(Score) as Sy,
sum(Bedrooms*Score) as Sxy,
sum(power(Bedrooms,2)) as Sx2,
sum(power(Score,2)) as Sy2
from Review_table2
where Score is not null
and Price>0
group by Neighbourhood, Room_type
) as t
where sqrt((n*Sx2-power(Sx,2))*(n*Sy2-power(Sy,2)))>0 --to prevent divide by zero error
order by Neighbourhood,Room_type

/*
Neighbourhood		Room_type			Pearson's R
Bronx			Entire home/apt		-0.0401065219845552
Bronx			Private room		-0.0547417473532595
Brooklyn		Entire home/apt		-0.00632190668507133
Brooklyn		Hotel room			-0.192524380444772
Brooklyn		Private room		 0.0187287904572153
Manhattan		Entire home/apt		-0.00780423268295889
Manhattan		Hotel room			 0.000997277138749698
Manhattan		Private room		 0.0219087848092802
Queens			Entire home/apt		-0.0334220989052496
Queens			Private room		 0.0114437626131096
Staten Island	Entire home/apt		-0.0733728700814781
Staten Island	Private room		-0.0163007555423255
*/

--Looking at super hosts and non super hosts
--Using CASE STATEMENTS
--------------------------------------------
select * from Review_table2

select Superhost, round(AVG(cast(replace(substring(t.price,2,10),',','') as float)),2) as AvgPrice,
concat(min(rt2.price),' - ',max(rt2.price) ) as Range, 
avg(Score) as Score, avg(Score_Accuracy) as ScoreAccuracy,avg(Cleanliness)as Cleanliness,avg(Checkin) as Checkin,
avg(Communication) as Communication, avg(Location) as Location, avg(Value) as Value
from(
select * ,
case
	when host_is_superhost='f' then 0
	else 1
end as Superhost
from DataAnalysis..['listings New York$']
) as t
inner join Review_table2 as rt2 on t.name=rt2.Name
where rt2.Price>0 
and Reviews>10
and Score is not null
group by Superhost

/*
Superhost	AvgPrice	Range		Score				ScoreAccuracy		Cleanliness			Checkin				Communication		Location			Value
0			163.49		10 - 10000	4.68101149176063	4.76159366869037	4.63780680832609	4.83250758889851	4.82775477016478	4.7380279705117		4.66666413703382
1			163.85		10 - 2943	4.83656661562021	4.86482542113321	4.81671822358346	4.90932618683		4.91292189892801	4.82120673813168	4.79342725880552
*/
------------------------------------------------------------------------------------------------
--Superhosts provide better services in all aspects	at a similar price on an average		   |
--Non Superhosts should improve CLEANLINESS if they want to make it competitive for Superhosts |
------------------------------------------------------------------------------------------------

--Room type wise
--Checking range and average of ratings based on Cleanliness
select Room_type, concat(min(Cleanliness),' - ', max(Cleanliness)) as Range_of_ratings, AVG(Cleanliness) as Aerage_ratings from Review_table1 
where Score is not null
and Price>0
and Reviews>10 --for reliable scores
--and Reviews>0
group by Room_type
/*
Room_type		Range_of_ratings	Aerage_ratings
Hotel room		3.62 - 5			4.59337662337662
Shared room		3.16 - 5			4.62155555555556
Private room	2 - 5				4.67910790301683
Entire home/apt	2.95 - 5			4.7247643259585
*/

--Neighbourhood wise
--Checking range and average of ratings
select Neighbourhood, concat(min(Cleanliness),' - ', max(Cleanliness)) as Range_of_ratings, AVG(Cleanliness) as Aerage_ratings from Review_table1 
where Score is not null
and Price>0
and Reviews>10 --for reliable scores
--and Reviews>0
group by Neighbourhood

/*
Neighbourhood	Range_of_ratings	Aerage_ratings
Brooklyn		3.07 - 5			4.71807735011101
Bronx			3.74 - 5			4.74401727861771
Manhattan		2 - 5				4.665559724828
Staten Island	3.76 - 5			4.77422222222222
Queens			3.41 - 5			4.73763975155279
*/


--Looking at Response rate & Acceptance rate
select Superhost, avg(host_response_rate_num) as Avg_Response_rate, avg(host_acceptance_rate_num) as Avg_Acc_rate
from
(
select * ,
case
	when host_is_superhost='f' then 0
	else 1
end as Superhost,
isnull(host_response_rate,0) as host_response_rate_num,ISNULL( host_acceptance_rate,0) as host_acceptance_rate_num
from DataAnalysis..['listings New York$']
) as t
inner join Review_table2 as rt2 on t.name=rt2.Name
where rt2.Price>0 
and Reviews>10
and Score is not null
group by Superhost

/*
Superhost	Avg_Response_rate	Avg_Acc_rate
0			0.530730702515179	0.492172593235041
1			0.810154670750384	0.764425727411948
*/

-----------------------------------------------------------------------------------------------------------------------------
--Superhosts are more responsive towards potential clients on an average; Non Superhosts should improve their response time |
--Superhosts have a higher acceptance rate; Non Superhosts should accept more frequently									|
-----------------------------------------------------------------------------------------------------------------------------

--Looking at instant bookablity
select Superhost, sum(ib) as Instant_Bookable, count(ib) as Total,
concat(cast((cast(sum(ib) as float)*100/cast(COUNT(ib) as float)) as decimal(10,2)),'%') as AvgB
from
(
select * ,
case
	when host_is_superhost='f' then 0
	else 1
end as Superhost,
case
	when instant_bookable='f' then 0
	else 1
end as ib
from DataAnalysis..['listings New York$']
) as t
inner join Review_table2 as rt2 on t.name=rt2.Name
where rt2.Price>0 
and Reviews>10
and Score is not null
group by Superhost

/*
Superhost	Instant_Bookable	Total	AvgB
0			2865				9224	31.06%
1			2688				6530	41.16%
*/
--------------------------------------------------
--Superhosts are more instantly bookable by ~10% |
--------------------------------------------------

--Location of Superhosts
select neighbourhood_group_cleansed as Neighbourhood, count(*) as Totalhosts, sum(Superhost) Superhosts,
concat(cast((cast(sum(Superhost) as float)*100/cast(count(*) as float)) as decimal(10,2)),'%') as PercentSuperhosts

from 
(
select * ,
case
	when host_is_superhost='f' then 0
	else 1
end as Superhost
from DataAnalysis..['listings New York$']
) as t
inner join Review_table2 as rt2 on t.name=rt2.Name
where rt2.Price>0 
and Reviews>10
and Score is not null
group by neighbourhood_group_cleansed
order by neighbourhood_group_cleansed
/*
Neighbourhood	Totalhosts	Superhosts	PercentSuperhosts
Bronx			518			225			43.44%
Brooklyn		5989		2368		39.54%
Manhattan		6727		2940		43.70%
Queens			2337		896			38.34%
Staten Island	183			101			55.19%
*/

--Room types owned by super hosts
select t.room_type as RoomType, count(*) as Totalhosts, sum(Superhost) Superhosts,
concat(cast((cast(sum(Superhost) as float)*100/cast(count(*) as float)) as decimal(10,2)),'%') as PercentSuperhosts
from 
(
select * ,
case
	when host_is_superhost='f' then 0
	else 1
end as Superhost
from DataAnalysis..['listings New York$']
) as t
inner join Review_table2 as rt2 on t.name=rt2.Name
where rt2.Price>0 
and Reviews>10
and Score is not null
group by t.room_type
order by t.room_type

/*
RoomType			Totalhosts	Superhosts	PercentSuperhosts
Entire home/apt		7940		3108		39.14%
Hotel room			88			3			3.41%
Private room		7518		3380		44.96%
Shared room			208			39			18.75%
*/

--------------------------------------------------------------------------------------------------
--Very few Superhosts own Hotel rooms; They are more interested in renting Private rooms		 |	
--Non Superhosts should try to move towards Private rooms or Entire homes for better results	 | 
--------------------------------------------------------------------------------------------------

--Checking availability
select Superhost, 
avg(availability_30) as a30,avg(availability_60) as a60, avg(availability_90) as a90, avg(availability_365) as ay
from
(
select * ,
case
	when host_is_superhost='f' then 0
	else 1
end as Superhost
from DataAnalysis..['listings New York$']
) as t
inner join Review_table2 as rt2 on t.name=rt2.Name
where rt2.Price>0 
and Reviews>10
and Score is not null
and availability_30>0
group by Superhost


--Displaying the range, count of scores & percent of count of scores

select range, case when count(*)=0 then 0 else count(*) end as 'Count', concat(round(cast(count(*)*100 as float)/sum(count(*)) over(),2),'%') as 'Percent'
from
(
select 
case 
	when Score>=0 and Score<1 then '0-1'
	when Score>=1 and Score<2 then '1-2'
	when Score>=2 and Score<3 then '2-3'
	when Score>=3 and Score<4 then '3-4'
	when Score>=4 and Score<5 then '4-5'
	when Score=5 then '5'
end as range
from DataAnalysis..['listings New York$'] t
left outer join Review_table2 as rt2 on t.name=rt2.Name
where rt2.Price>0
and Reviews>10
and Score is not null
) t2
group by range
order by range


--FOR VISUALIZATIONS PURPOSES
-----------------------------

drop table if exists Vtable1
create table Vtable1(
category varchar(200),
Units int,
MinPrice float,
MaxPrice float,
AvgPrice float
)

insert into Vtable1
select property_type, 
count(property_type),
min(cast(replace(substring(price,2,10),',','') as float)),
max(cast(replace(substring(price,2,10),',','') as float)),
Round(AVG(cast(replace(substring(price,2,10),',','') as float)),2)
from DataAnalysis..['listings New York$']
where cast(replace(substring(price,2,10),',','') as float)>0
group by property_type 

select * from Vtable1

select neighbourhood_group_cleansed, neighbourhood_cleansed,
avg(cast(replace(substring(price,2,10),',','') as float)) over (partition by  neighbourhood_cleansed order by neighbourhood_group_cleansed) as avgp, 
avg(review_scores_rating) over (partition by  neighbourhood_cleansed order by neighbourhood_group_cleansed) as scores,  
sum(number_of_reviews) over (partition by  neighbourhood_cleansed order by neighbourhood_group_cleansed) as rev
from DataAnalysis..['listings New York$']
where cast(replace(substring(price,2,10),',','') as float)>0
--and neighbourhood_cleansed='Allerton'
and review_scores_rating>0

select 
neighbourhood_group_cleansed,
neighbourhood_cleansed,
count(*) as Listings,
round(min(cast(replace(substring(price,2,10),',','') as float)),2) as minp,
round(max(cast(replace(substring(price,2,10),',','') as float)),2) as maxp,
round(avg(cast(replace(substring(price,2,10),',','') as float)),2) as avgp,
round(min(review_scores_rating),2)  as minscores, 
round(max(review_scores_rating),2)  as maxscores, 
round(avg(review_scores_rating),2)  as avgscores, 
sum(number_of_reviews) as rev,
sum(accommodates) as acc
from DataAnalysis..['listings New York$']
where cast(replace(substring(price,2,10),',','') as float)>0
--and neighbourhood_cleansed='Allerton'
and review_scores_rating>0
and accommodates<>0
group by neighbourhood_group_cleansed, neighbourhood_cleansed
order by neighbourhood_group_cleansed, neighbourhood_cleansed


--Changing data as per locations file for Tableau
-------------------------------------------------
select 
neighbourhood_group_cleansed,
t.NeighbourhoodArea,
count(*) as Listings,
round(min(cast(replace(substring(price,2,10),',','') as float)),2) as minp,
round(max(cast(replace(substring(price,2,10),',','') as float)),2) as maxp,
round(avg(cast(replace(substring(price,2,10),',','') as float)),2) as avgp,
round(min(review_scores_rating),2)  as minscores, 
round(max(review_scores_rating),2)  as maxscores, 
round(avg(review_scores_rating),2)  as avgscores, 
sum(number_of_reviews) as rev,
sum(accommodates) as acc
from
(
select *,
case 
	when neighbourhood_cleansed='Baychester' then replace(neighbourhood_cleansed,'Baychester', 'Eastchester-Edenwald-Baychester')
	when neighbourhood_cleansed='Eastchester' then replace(neighbourhood_cleansed,'Eastchester', 'Eastchester-Edenwald-Baychester')
	when neighbourhood_cleansed='Edenwald' then replace(neighbourhood_cleansed,'Edenwald', 'Eastchester-Edenwald-Baychester')
	when neighbourhood_cleansed='Kingsbridge' then replace(neighbourhood_cleansed,'Kingsbridge', 'Kingsbridge-Marble Hill')
	when neighbourhood_cleansed='Mott Haven' then replace(neighbourhood_cleansed,'Mott Haven', 'Mott Haven-Port Morris')
	when neighbourhood_cleansed='Port Morris' then replace(neighbourhood_cleansed,'Port Morris', 'Mott Haven-Port Morris')
	when neighbourhood_cleansed='Mount Eden' then replace(neighbourhood_cleansed,'Mount Eden', 'Mount Eden-Claremont(West)')
	when neighbourhood_cleansed='Claremont Village' then replace(neighbourhood_cleansed,'Claremont Village', 'Mount Eden-Claremont(West)')
	when neighbourhood_cleansed='Bronxdale' then replace(neighbourhood_cleansed,'Bronxdale', 'Bronx Park')
	when neighbourhood_cleansed='Castle Hill' then replace(neighbourhood_cleansed,'Castle Hill', 'Castle Hill-Unionport')
	when neighbourhood_cleansed='Concourse' then replace(neighbourhood_cleansed,'Concourse', 'Concourse-Concourse Village')
	when neighbourhood_cleansed='Concourse Village' then replace(neighbourhood_cleansed,'Concourse Village', 'Concourse-Concourse Village')
	when neighbourhood_cleansed='Pelham Bay' then replace(neighbourhood_cleansed,'Pelham Bay', 'Pelham Bay-Country Club-City Island')
	when neighbourhood_cleansed='Country Club' then replace(neighbourhood_cleansed,'Country Club', 'Pelham Bay-Country Club-City Island')
	when neighbourhood_cleansed='City Island' then replace(neighbourhood_cleansed,'City Island', 'Pelham Bay-Country Club-City Island')
	when neighbourhood_cleansed='Van Nest' then replace(neighbourhood_cleansed,'Van Nest', 'Pelham Parkway-Van Nest')
	when neighbourhood_cleansed='Riverdale' then replace(neighbourhood_cleansed,'Riverdale', 'Riverdale-Spuyten Duyvil')
	when neighbourhood_cleansed='Spuyten Duyvil' then replace(neighbourhood_cleansed,'Spuyten Duyvil', 'Riverdale-Spuyten Duyvil')
	when neighbourhood_cleansed='Soundview' then replace(neighbourhood_cleansed,'Soundview', 'Soundview-Clason Point')
	when neighbourhood_cleansed='Clason Point' then replace(neighbourhood_cleansed,'Clason Point', 'Soundview-Clason Point')
	when neighbourhood_cleansed='Throgs Neck' then replace(neighbourhood_cleansed,'Throgs Neck', 'Throgs Neck-Schuylerville')
	when neighbourhood_cleansed='Schuylerville' then replace(neighbourhood_cleansed,'Schuylerville', 'Throgs Neck-Schuylerville')
	when neighbourhood_cleansed='University Heights' then replace(neighbourhood_cleansed,'University Heights', 'University Heights (South)-Morris Heights')
	when neighbourhood_cleansed='Morris Heights' then replace(neighbourhood_cleansed,'Morris Heights', 'University Heights (South)-Morris Heights')
	when neighbourhood_cleansed='Wakefield' then replace(neighbourhood_cleansed,'Wakefield', 'Wakefield-Woodlawn')
	when neighbourhood_cleansed='Woodlawn' then replace(neighbourhood_cleansed,'Woodlawn', 'Wakefield-Woodlawn')
	when neighbourhood_cleansed='Williamsbridge' then replace(neighbourhood_cleansed,'Williamsbridge', 'Williamsbridge-Olinville')
	when neighbourhood_cleansed='Olinville' then replace(neighbourhood_cleansed,'Olinville', 'Williamsbridge-Olinville')
	when neighbourhood_cleansed='Bedford-Stuyvesant' then replace(neighbourhood_cleansed,'Bedford-Stuyvesant', 'Bedford-Stuyvesant (East)')
	when neighbourhood_cleansed='Bushwick' then replace(neighbourhood_cleansed,'Bushwick', 'Bushwick (West)')
	when neighbourhood_cleansed='Carroll Gardens' then replace(neighbourhood_cleansed,'Carroll Gardens', 'Carroll Gardens-Cobble Hill-Gowanus-Red Hook')
	when neighbourhood_cleansed='Cobble Hill' then replace(neighbourhood_cleansed,'Cobble Hill', 'Carroll Gardens-Cobble Hill-Gowanus-Red Hook')
	when neighbourhood_cleansed='Gowanus' then replace(neighbourhood_cleansed,'Gowanus', 'Carroll Gardens-Cobble Hill-Gowanus-Red Hook')
	when neighbourhood_cleansed='Red Hook' then replace(neighbourhood_cleansed,'Red Hook', 'Carroll Gardens-Cobble Hill-Gowanus-Red Hook')
	when neighbourhood_cleansed='Coney Island' then replace(neighbourhood_cleansed,'Coney Island', 'Coney Island-Sea Gate')
	when neighbourhood_cleansed='Sea Gate' then replace(neighbourhood_cleansed,'Sea Gate', 'Coney Island-Sea Gate')
	when neighbourhood_cleansed='Crown Heights' then replace(neighbourhood_cleansed,'Crown Heights', 'Crown Heights (North)')
	when neighbourhood_cleansed='Downtown Brooklyn' then replace(neighbourhood_cleansed,'Downtown Brooklyn', 'Downtown Brooklyn-DUMBO-Boerum Hill')
	when neighbourhood_cleansed='DUMBO' then replace(neighbourhood_cleansed,'DUMBO', 'Downtown Brooklyn-DUMBO-Boerum Hill')
	when neighbourhood_cleansed='Boerum Hill' then replace(neighbourhood_cleansed,'Boerum Hill', 'Downtown Brooklyn-DUMBO-Boerum Hill')
	when neighbourhood_cleansed='East Flatbush' then replace(neighbourhood_cleansed,'East Flatbush', 'East Flatbush-Erasmus')
	when neighbourhood_cleansed='East New York' then replace(neighbourhood_cleansed,'East New York', 'East New York-City Line')
	when neighbourhood_cleansed='Gravesend' then replace(neighbourhood_cleansed,'Gravesend', 'Gravesend (South)')
	when neighbourhood_cleansed='Bergen Beach' then replace(neighbourhood_cleansed,'Bergen Beach', 'Marine Park-Mill Basin-Bergen Beach')
	when neighbourhood_cleansed='Mill Basin' then replace(neighbourhood_cleansed,'Mill Basin', 'Marine Park-Mill Basin-Bergen Beach')
	when neighbourhood_cleansed='Prospect Lefferts Gardens' then replace(neighbourhood_cleansed,'Prospect Lefferts Gardens', 'Prospect Lefferts Gardens-Wingate')
	when neighbourhood_cleansed='Sheepshead Bay' then replace(neighbourhood_cleansed,'Sheepshead Bay', 'Sheepshead Bay-Manhattan Beach-Gerritsen Beach')
	when neighbourhood_cleansed='Manhattan Beach' then replace(neighbourhood_cleansed,'Manhattan Beach', 'Sheepshead Bay-Manhattan Beach-Gerritsen Beach')	
	when neighbourhood_cleansed='Gerritsen Beach' then replace(neighbourhood_cleansed,'Gerritsen Beach', 'Sheepshead Bay-Manhattan Beach-Gerritsen Beach')	
	when neighbourhood_cleansed='Williamsburg' then replace(neighbourhood_cleansed,'Williamsburg', 'South Williamsburg')
	when neighbourhood_cleansed='Sunset Park' then replace(neighbourhood_cleansed,'Sunset Park', 'Sunset Park (Central)')
	when neighbourhood_cleansed='Windsor Terrace' then replace(neighbourhood_cleansed,'Windsor Terrace', 'Windsor Terrace-South Slope')
	when neighbourhood_cleansed='South Slope' then replace(neighbourhood_cleansed,'South Slope', 'Williamsbridge-Olinville')
	when neighbourhood_cleansed='Chelsea' then replace(neighbourhood_cleansed,'Chelsea', 'Chelsea-Hudson Yards')
	when neighbourhood_cleansed='Chinatown' then replace(neighbourhood_cleansed,'Chinatown', 'Chinatown-Two Bridges')
	when neighbourhood_cleansed='Two Bridges' then replace(neighbourhood_cleansed,'Two Bridges', 'Chinatown-Two Bridges')
	when neighbourhood_cleansed='East Harlem' then replace(neighbourhood_cleansed,'East Harlem', 'East Harlem (North)')
	when neighbourhood_cleansed='Financial District' then replace(neighbourhood_cleansed,'Financial District', 'Financial District-Battery Park City')
	when neighbourhood_cleansed='Battery Park City' then replace(neighbourhood_cleansed,'Battery Park City', 'Financial District-Battery Park City')
	when neighbourhood_cleansed='Harlem' then replace(neighbourhood_cleansed,'Harlem', 'Harlem (North)')
	when neighbourhood_cleansed='Flatiron District' then replace(neighbourhood_cleansed,'Flatiron District', 'Midtown South-Flatiron-Union Square')
	when neighbourhood_cleansed='Midtown' then replace(neighbourhood_cleansed,'Midtown', 'Midtown-Times Square')
	when neighbourhood_cleansed='Murray Hill' then replace(neighbourhood_cleansed,'Murray Hill', 'Murray Hill-Kips Bay')
	when neighbourhood_cleansed='Kips Bay' then replace(neighbourhood_cleansed,'Kips Bay', 'Murray Hill-Kips Bay')
	when neighbourhood_cleansed='SoHo' then replace(neighbourhood_cleansed,'SoHo', 'SoHo-Little Italy-Hudson Square')
	when neighbourhood_cleansed='Little Italy' then replace(neighbourhood_cleansed,'Little Italy', 'SoHo-Little Italy-Hudson Square')
	when neighbourhood_cleansed='Stuyvesant Town' then replace(neighbourhood_cleansed,'Stuyvesant Town', 'Stuyvesant Peter Cooper Village')
	when neighbourhood_cleansed='Tribeca' then replace(neighbourhood_cleansed,'Tribeca', 'Tribeca-Civic Center')
	when neighbourhood_cleansed='Civic Center' then replace(neighbourhood_cleansed,'Civic Center', 'Tribeca-Civic Center')
	when neighbourhood_cleansed='Upper East Side' then replace(neighbourhood_cleansed,'Upper East Side', 'Upper East Side-Lenox Hill-Roosevelt Island')
	when neighbourhood_cleansed='Roosevelt Island' then replace(neighbourhood_cleansed,'Roosevelt Island', 'Upper East Side-Lenox Hill-Roosevelt Island')
	when neighbourhood_cleansed='Upper West Side' then replace(neighbourhood_cleansed,'Upper West Side', 'Upper West Side (Central)')
	when neighbourhood_cleansed='Washington Heights' then replace(neighbourhood_cleansed,'Washington Heights', 'Washington Heights (South)')
	when neighbourhood_cleansed='Astoria' then replace(neighbourhood_cleansed,'Astoria', 'Astoria (North)-Ditmars-Steinway')
	when neighbourhood_cleansed='Ditmars Steinway' then replace(neighbourhood_cleansed,'Ditmars Steinway', 'Astoria (North)-Ditmars-Steinway')
	when neighbourhood_cleansed='Breezy Point' then replace(neighbourhood_cleansed,'Breezy Point', 'Breezy Point-Belle Harbor-Rockaway Park-Broad Channel')
	when neighbourhood_cleansed='Belle Harbor' then replace(neighbourhood_cleansed,'Belle Harbor', 'Breezy Point-Belle Harbor-Rockaway Park-Broad Channel')
	when neighbourhood_cleansed='Douglaston' then replace(neighbourhood_cleansed,'Douglaston', 'Douglaston-Little Neck')
	when neighbourhood_cleansed='Little Neck' then replace(neighbourhood_cleansed,'Little Neck', 'Douglaston-Little Neck')
	when neighbourhood_cleansed='Flushing' then replace(neighbourhood_cleansed,'Flushing', 'Flushing-Willets Point')
	when neighbourhood_cleansed='Fresh Meadows' then replace(neighbourhood_cleansed,'Fresh Meadows', 'Fresh Meadows-Utopia')
	when neighbourhood_cleansed='Howard Beach' then replace(neighbourhood_cleansed,'Howard Beach', 'Howard Beach-Lindenwood')
	when neighbourhood_cleansed='Jamaica Estates' then replace(neighbourhood_cleansed,'Jamaica Estates', 'Jamaica Estates-Holliswood')
	when neighbourhood_cleansed='Holliswood' then replace(neighbourhood_cleansed,'Holliswood', 'Jamaica Estates-Holliswood')
	when neighbourhood_cleansed='Jamaica Hills' then replace(neighbourhood_cleansed,'Jamaica Hills', 'Jamaica Hills-Briarwood')
	when neighbourhood_cleansed='Briarwood' then replace(neighbourhood_cleansed,'Briarwood', 'Jamaica Hills-Briarwood')
	when neighbourhood_cleansed='Long Island City' then replace(neighbourhood_cleansed,'Long Island City', 'Long Island City-Hunters Point')
	when neighbourhood_cleansed='Hollis' then replace(neighbourhood_cleansed,'Hollis', 'Oakland Gardens-Hollis Hills')
	when neighbourhood_cleansed='Rockaway Beach' then replace(neighbourhood_cleansed,'Rockaway Beach', 'Rockaway Beach-Arverne-Edgemere')
	when neighbourhood_cleansed='Arverne' then replace(neighbourhood_cleansed,'Arverne', 'Rockaway Beach-Arverne-Edgemere')
	when neighbourhood_cleansed='Edgemere' then replace(neighbourhood_cleansed,'Edgemere', 'Rockaway Beach-Arverne-Edgemere')
	when neighbourhood_cleansed='Springfield Gardens' then replace(neighbourhood_cleansed,'Springfield Gardens', 'Springfield Gardens (South)-Brookville')
	when neighbourhood_cleansed='Whitestone' then replace(neighbourhood_cleansed,'Whitestone', 'Whitestone-Beechhurst')
	when neighbourhood_cleansed='Huguenot' then replace(neighbourhood_cleansed,'Huguenot', 'Annadale-Huguenot-Prince''s Bay-Woodrow')
	when neighbourhood_cleansed='Prince''s Bay' then replace(neighbourhood_cleansed,'Prince''s Bay', 'Annadale-Huguenot-Prince''s Bay-Woodrow')
	when neighbourhood_cleansed='Woodrow' then replace(neighbourhood_cleansed,'Woodrow', 'Annadale-Huguenot-Prince''s Bay-Woodrow')
	when neighbourhood_cleansed='Arden Heights' then replace(neighbourhood_cleansed,'Arden Heights', 'Arden Heights-Rossville')
	when neighbourhood_cleansed='Rossville' then replace(neighbourhood_cleansed,'Rossville', 'Arden Heights-Rossville')
	when neighbourhood_cleansed='Arrochar' then replace(neighbourhood_cleansed,'Arrochar', 'Grasmere-Arrochar-South Beach-Dongan Hills')
	when neighbourhood_cleansed='Dongan Hills' then replace(neighbourhood_cleansed,'Dongan Hills', 'Grasmere-Arrochar-South Beach-Dongan Hills')
	when neighbourhood_cleansed='South Beach' then replace(neighbourhood_cleansed,'South Beach', 'Grasmere-Arrochar-South Beach-Dongan Hills')
	when neighbourhood_cleansed='Great Kills' then replace(neighbourhood_cleansed,'Great Kills', 'Great Kills-Eltingville')
	when neighbourhood_cleansed='Eltingville Kills' then replace(neighbourhood_cleansed,'Eltingville Kills', 'Great Kills-Eltingville')
	when neighbourhood_cleansed='Graniteville' then replace(neighbourhood_cleansed,'Graniteville', 'Mariner''s Harbor-Arlington-Graniteville')
	when neighbourhood_cleansed='Mariners Harbor' then replace(neighbourhood_cleansed,'Mariners Harbor', 'Mariner''s Harbor-Arlington-Graniteville')
	when neighbourhood_cleansed='New Dorp' then replace(neighbourhood_cleansed,'New Dorp', 'New Dorp-Midland Beach')
	when neighbourhood_cleansed='Midland Beach' then replace(neighbourhood_cleansed,'Midland Beach', 'New Dorp-Midland Beach')
	when neighbourhood_cleansed='New Springville' then replace(neighbourhood_cleansed,'New Springville', 'New Springville-Willowbrook-Bulls Head-Travis')
	when neighbourhood_cleansed='Bull''s Head' then replace(neighbourhood_cleansed,'Bull''s Head', 'New Springville-Willowbrook-Bulls Head-Travis')
	when neighbourhood_cleansed='Oakwood' then replace(neighbourhood_cleansed,'Oakwood', 'Oakwood-Richmondtown')
	when neighbourhood_cleansed='Richmondtown' then replace(neighbourhood_cleansed,'Oakwood', 'Oakwood-Richmondtown')
	when neighbourhood_cleansed='Rosebank' then replace(neighbourhood_cleansed,'Rosebank', 'Rosebank-Shore Acres-Park Hill')
	when neighbourhood_cleansed='Shore Acres' then replace(neighbourhood_cleansed,'Shore Acres', 'Rosebank-Shore Acres-Park Hill')
	when neighbourhood_cleansed='New Brighton' then replace(neighbourhood_cleansed,'New Brighton', 'St. George-New Brighton')
	when neighbourhood_cleansed='St. George' then replace(neighbourhood_cleansed,'St. George', 'St. George-New Brighton')
	when neighbourhood_cleansed='Todt Hill' then replace(neighbourhood_cleansed,'Todt Hill', 'Todt Hill-Emerson Hill-Lighthouse Hill-Manor Heights')
	when neighbourhood_cleansed='Emerson Hill' then replace(neighbourhood_cleansed,'Emerson Hill', 'Todt Hill-Emerson Hill-Lighthouse Hill-Manor Heights')
	when neighbourhood_cleansed='Lighthouse Hill' then replace(neighbourhood_cleansed,'Lighthouse Hill', 'Todt Hill-Emerson Hill-Lighthouse Hill-Manor Heights')
	when neighbourhood_cleansed='Tompkinsville' then replace(neighbourhood_cleansed,'Tompkinsville', 'Tompkinsville-Stapleton-Clifton-Fox Hills')
	when neighbourhood_cleansed='Stapleton' then replace(neighbourhood_cleansed,'Stapleton', 'Tompkinsville-Stapleton-Clifton-Fox Hills')
	when neighbourhood_cleansed='Clifton' then replace(neighbourhood_cleansed,'Clifton', 'Tompkinsville-Stapleton-Clifton-Fox Hills')
	when neighbourhood_cleansed='West Brighton' then replace(neighbourhood_cleansed,'West Brighton', 'West New Brighton-Silver Lake-Grymes Hill')
	when neighbourhood_cleansed='Silver Lake' then replace(neighbourhood_cleansed,'Silver Lake', 'West New Brighton-Silver Lake-Grymes Hill')
	when neighbourhood_cleansed='Grymes Hill' then replace(neighbourhood_cleansed,'Grymes Hill', 'West New Brighton-Silver Lake-Grymes Hill')
	when neighbourhood_cleansed='Westerleigh' then replace(neighbourhood_cleansed,'Westerleigh', 'Westerleigh-Castleton Corners')

	else neighbourhood_cleansed
end as NeighbourhoodArea
from DataAnalysis..['listings New York$']
where cast(replace(substring(price,2,10),',','') as float)>0
--and neighbourhood_cleansed='Allerton'
and review_scores_rating>0
and accommodates<>0
) t
group by neighbourhood_group_cleansed, t.NeighbourhoodArea
order by neighbourhood_group_cleansed, t.NeighbourhoodArea

--Details
select *, cast(replace(substring(price,2,10),',','') as float) as Pricing
from DataAnalysis..['listings New York$']
where cast(replace(substring(price,2,10),',','') as float)>0
and review_scores_rating>0

--Details for Neighbourhood groups
select  neighbourhood_group_cleansed, count(*) as 'Listings',sum(accommodates) as Accomodations,sum(number_of_reviews) as 'Reviews'
,round(avg(review_scores_rating),2) as 'Average Ratings',concat('$',round(avg(Pricing),2)) as 'Average Pricing'
from 
(
select *,cast(replace(substring(price,2,10),',','') as float) as Pricing from DataAnalysis..['listings New York$']
) t
where Pricing>0
and review_scores_rating is not null
group by neighbourhood_group_cleansed


--Listings info
select host_id, host_name,isnull(name,' - ') as name, isnull(neighborhood_overview,' - ') as neighborhood_overview, neighbourhood_cleansed, neighbourhood_group_cleansed, 
cast(replace(substring(price,2,10),',','') as float) as Price, property_type, room_type
, case 
	when host_is_superhost='t' then 'Superhost'
	when host_is_superhost='f' then 'Not Superhost'
end as Superhost
, review_scores_rating, 
latitude, longitude, accommodates
,cast(
isnull(
replace(bathrooms_text,
(substring(bathrooms_text,PATINDEX('%[a-z]%',bathrooms_text),len(bathrooms_text))),
'')
,0) as float) as Bathrooms 
, isnull(bedrooms,0) as bedrooms, minimum_nights, maximum_nights
, review_scores_accuracy, review_scores_checkin, review_scores_cleanliness, review_scores_communication, review_scores_location, review_scores_value, number_of_reviews
, listing_url
from DataAnalysis..['listings New York$']
where
cast(replace(substring(price,2,10),',','') as float)>0
and review_scores_rating>0
and host_name is not null
order by Superhost

select host_id, count(*) as listings from DataAnalysis..['listings New York$']
where
cast(replace(substring(price,2,10),',','') as float)>0
and review_scores_rating>0
group by host_id
order by count(*) desc


select
cast(
(
select sum(listings) as sumof4percent
from 
(
select top 4 percent count(*) as listings from DataAnalysis..['listings New York$']
where
cast(replace(substring(price,2,10),',','') as float)>0
and review_scores_rating>0
group by host_id
order by count(*) desc
) t
) as decimal(10,3))*100
/
(
select sum(Tlistings) as sumof100percent
from
(
select top 100 percent count(*) as Tlistings from DataAnalysis..['listings New York$']
where
cast(replace(substring(price,2,10),',','') as float)>0
and review_scores_rating>0
group by host_id
order by count(*) desc
) t1
) as 'percent'

select host_id, count(*) as listings from DataAnalysis..['listings New York$']
where
cast(replace(substring(price,2,10),',','') as float)>0
and review_scores_rating>0
group by host_id
having count(*)>3
order by count(*) desc

SELECT cast(3110 AS DECIMAL(10,3))/20065 -- >1
SELECT cast(1198 AS DECIMAL(10,3))/20065--  >2
SELECT cast(624 AS DECIMAL(10,3))/20065--  >3

select sum(listings)
from
(
select count(*) as listings from DataAnalysis..['listings New York$']
where
cast(replace(substring(price,2,10),',','') as float)>0
and review_scores_rating>0
group by host_id
having count(*)>0
--order by count(*) desc
) t

SELECT cast(5366 AS DECIMAL(10,3))/27867--  >3