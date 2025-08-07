%sql
USE CATALOG api_cerveja;
USE SCHEMA 3_gold;
     

%sql
SELECT state_province AS nome_estado
      , COUNT(state_province) AS qtd_estado 
FROM 3_gold.tb_cerveja_insight 
GROUP BY state_province 
  ORDER BY COUNT(state_province) DESC;
     
nome_estado	qtd_estado
Oregon	6
Colorado	4
California	4
Minnesota	3
Indiana	3
Arizona	3
New York	2
Ohio	2
North Carolina	2
Texas	2
Washington	2
Wisconsin	2
Idaho	1
Iowa	1
Delaware	1
Mississippi	1
Oklahoma	1
Nevada	1
Maryland	1
South Carolina	1
Laois	1
Vermont	1
Michigan	1
Virginia	1
Massachusetts	1
Illinois	1
Pennsylvania	1
Databricks visualization. Run in Databricks to view.

%sql
SELECT name_brewery
     , longitude
     , latitude
     , state_province 
FROM 3_gold.tb_cerveja_insight;
     
name_brewery	longitude	latitude	state_province
(405) Brewing Co	-97.46818222	35.25738891	Oklahoma
(512) Brewing Co	null	null	Texas
1 of Us Brewing Company	-87.883363502094	42.720108268996	Wisconsin
10 Barrel Brewing Co	-121.281706	44.08683531	Oregon
10 Barrel Brewing Co	-121.3288021	44.0575649	Oregon
10 Barrel Brewing Co	-122.6855056	45.5259786	Oregon
10 Barrel Brewing Co	-117.129593	32.714813	California
10 Barrel Brewing Co - Bend Pub	-121.2809536	44.0912109	Oregon
10 Barrel Brewing Co - Boise	-116.202929	43.618516	Idaho
10 Barrel Brewing Co - Denver	-104.9853655	39.7592508	Colorado
10 Torr Distilling and Brewing	-119.7732015	39.5171702	Nevada
10-56 Brewing Company	-86.627954	41.289715	Indiana
101 North Brewing Company	-122.665055	38.27029381	California
105 West Brewing Co	-104.8667206	39.38269495	Colorado
10K Brewing	-93.38952559	45.19812039	Minnesota
10th District Brewing Company	-70.94594149	42.10591754	Massachusetts
11 Below Brewing Company	-95.5186591	29.9515464	Texas
1188 Brewing Co	-118.9218754	44.4146563	Oregon
12 Acres Brewing Company	-6.979343891	52.84930763	Laois
12 Gates Brewing Company	null	null	New York
12 West Brewing Company	null	null	Arizona
12 West Brewing Company - Production Facility	-111.5860662	33.436188	Arizona
122 West Brewing Co	-122.485982	48.7621709	Washington
127 Brewing	-84.43116792	42.28667212	Michigan
12Degree Brewing	-105.1319826	39.9782443	Colorado
12welve Eyes Brewing	null	null	Minnesota
13 Below Brewery	-84.70634815	39.12639764	Ohio
13 Stripes Brewery	null	null	South Carolina
13 Virtues Brewing Co	-122.6487531	45.4762536	Oregon
1323 R & D	null	null	North Carolina
14 Cannons Brewing Company	-118.802397	34.15334	California
14 Lakes Brewery	null	null	Minnesota
14er Brewing Company	-104.9839636	39.7614112	Colorado
14th Star Brewing	null	null	Vermont
16 Lots Brewing	-84.3183801	39.3545967	Ohio
16 Mile Brewing Co	-75.37816436	38.6788938	Delaware
16 Stone Brewpub	-75.2565195	43.24211175	New York
1623 Brewing CO, llc	null	null	Maryland
1717 Brewing Co	-93.6120353	41.5872267	Iowa
1718 Ocracoke Brewing	-75.97176063	35.10715368	North Carolina
1781 Brewing Company	null	null	Virginia
180 and Tapped	-80.15020356	40.50984957	Pennsylvania
1817 Brewery	-88.750264	34.001703	Mississippi
1840 Brewing Company	-87.90606942	43.00436242	Wisconsin
1850 Brewing Company	-119.9036592	37.570148	California
18th Street Brewery	-87.26887786	41.59928343	Indiana
18th Street Brewery	-87.517422	41.61556796	Indiana
1905 Brewing Company	-89.0503635	39.5172564	Illinois
1912 Brewing	-110.9927505	32.24673727	Arizona
192 Brewing	-122.2415652	47.75670075	Washington
