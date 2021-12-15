from sqlalchemy import create_engine
from sportsreference.ncaab.teams import Teams
from sportsreference.ncaab.boxscore import Boxscore
from sportsreference.ncaab.schedule import Schedule
from bs4 import *
from urllib.error import HTTPError
import numpy as np
import requests
import pandas as pd
import mysql.connector
import datetime
begin_time = datetime.datetime.now()
# from sqlalchemy.engine import URL

### team_names = []
### team_abbri = []
# for team in Teams(): #prints a list of the NCAA teams and the abbriviations accepted by Schedule
# team_names.append(team.name)
# team_abbri.append(team.abbreviation)

team_names = ['Abilene Christian', 'Air Force', 'Akron', 'Alabama A&M', 'Alabama-Birmingham',
              'Alabama State', 'Alabama', 'Albany (NY)', 'Alcorn State', 'American', 'Appalachian State',
              'Arizona State', 'Arizona', 'Little Rock', 'Arkansas-Pine Bluff', 'Arkansas State', 'Arkansas',
              'Army', 'Auburn', 'Austin Peay', 'Ball State', 'Baylor', 'Bellarmine', 'Belmont', 'Binghamton',
              'Boise State', 'Boston College', 'Boston University', 'Bowling Green State', 'Bradley',
              'Brigham Young', 'Brown', 'Bryant', 'Bucknell', 'Buffalo', 'Butler', 'Cal Poly',
              'Cal State Bakersfield', 'Cal State Fullerton', 'Cal State Northridge',
              'California Baptist', 'UC-Davis', 'UC-Irvine', 'UC-Riverside', 'UC-San Diego', 'UC-Santa Barbara',
              'University of California', 'Campbell', 'Canisius', 'Central Arkansas',
              'Central Connecticut State', 'Central Florida', 'Central Michigan', 'Charleston Southern',
              'Charlotte', 'Chattanooga', 'Chicago State', 'Cincinnati', 'Citadel', 'Clemson',
              'Cleveland State', 'Coastal Carolina', 'Colgate', 'College of Charleston', 'Colorado State',
              'Colorado', 'Columbia', 'Connecticut', 'Coppin State', 'Cornell', 'Creighton', 'Dartmouth',
              'Davidson', 'Dayton', 'Delaware State', 'Delaware', 'Denver', 'DePaul', 'Detroit Mercy',
              'Dixie State', 'Drake', 'Drexel', 'Duke', 'Duquesne', 'East Carolina', 'East Tennessee State',
              'Eastern Illinois', 'Eastern Kentucky', 'Eastern Michigan', 'Eastern Washington', 'Elon',
              'Evansville', 'Fairfield', 'Fairleigh Dickinson', 'Florida A&M', 'Florida Atlantic',
              'Florida Gulf Coast', 'Florida International', 'Florida State', 'Florida', 'Fordham',
              'Fresno State', 'Furman', 'Gardner-Webb', 'George Mason', 'George Washington', 'Georgetown',
              'Georgia Southern', 'Georgia State', 'Georgia Tech', 'Georgia', 'Gonzaga', 'Grambling',
              'Grand Canyon', 'Green Bay', 'Hampton', 'Hartford', 'Harvard', 'Hawaii', 'High Point', 'Hofstra',
              'Holy Cross', 'Houston Baptist', 'Houston', 'Howard', 'Idaho State', 'Idaho', 'Illinois-Chicago',
              'Illinois State', 'Illinois', 'Incarnate Word', 'Indiana State', 'Indiana', 'Iona', 'Iowa State',
              'Iowa', 'Purdue-Fort Wayne', 'IUPUI', 'Jackson State', 'Jacksonville State', 'Jacksonville',
              'James Madison', 'Kansas State', 'Kansas', 'Kennesaw State', 'Kent State', 'Kentucky', 'La Salle',
              'Lafayette', 'Lamar', 'Lehigh', 'Liberty', 'Lipscomb', 'Cal State Long Beach',
              'Long Island University', 'Longwood', 'Louisiana', 'Louisiana-Monroe', 'Louisiana State',
              'Louisiana Tech', 'Louisville', 'Loyola (IL)', 'Loyola Marymount', 'Loyola (MD)', 'Maine',
              'Manhattan', 'Marist', 'Marquette', 'Marshall', 'Maryland-Baltimore County',
              'Maryland-Eastern Shore', 'Maryland', 'Massachusetts-Lowell', 'Massachusetts', 'McNeese State',
              'Memphis', 'Mercer', 'Merrimack', 'Miami (FL)', 'Miami (OH)', 'Michigan State', 'Michigan',
              'Middle Tennessee', 'Milwaukee', 'Minnesota', 'Mississippi State', 'Mississippi Valley State',
              'Mississippi', 'Missouri-Kansas City', 'Missouri State', 'Missouri', 'Monmouth', 'Montana State',
              'Montana', 'Morehead State', 'Morgan State', "Mount St. Mary's", 'Murray State', 'Navy', 'Omaha',
              'Nebraska', 'Nevada-Las Vegas', 'Nevada', 'New Hampshire', 'New Mexico State', 'New Mexico',
              'New Orleans', 'Niagara', 'Nicholls State', 'NJIT', 'Norfolk State', 'North Alabama',
              'North Carolina-Asheville', 'North Carolina A&T', 'North Carolina Central',
              'North Carolina-Greensboro', 'North Carolina State', 'North Carolina-Wilmington',
              'North Carolina', 'North Dakota State', 'North Dakota', 'North Florida', 'North Texas',
              'Northeastern', 'Northern Arizona', 'Northern Colorado', 'Northern Illinois', 'Northern Iowa',
              'Northern Kentucky', 'Northwestern State', 'Northwestern', 'Notre Dame', 'Oakland', 'Ohio State',
              'Ohio', 'Oklahoma State', 'Oklahoma', 'Old Dominion', 'Oral Roberts', 'Oregon State', 'Oregon',
              'Pacific', 'Penn State', 'Pennsylvania', 'Pepperdine', 'Pittsburgh', 'Portland State', 'Portland',
              'Prairie View', 'Presbyterian', 'Princeton', 'Providence', 'Purdue', 'Quinnipiac', 'Radford',
              'Rhode Island', 'Rice', 'Richmond', 'Rider', 'Robert Morris', 'Rutgers', 'Sacramento State',
              'Sacred Heart', 'Saint Francis (PA)', "Saint Joseph's", 'Saint Louis', "Saint Mary's (CA)",
              "Saint Peter's", 'Sam Houston State', 'Samford', 'San Diego State', 'San Diego', 'San Francisco',
              'San Jose State', 'Santa Clara', 'Seattle', 'Seton Hall', 'Siena', 'South Alabama',
              'South Carolina State', 'South Carolina Upstate', 'South Carolina', 'South Dakota State',
              'South Dakota', 'South Florida', 'Southeast Missouri State', 'Southeastern Louisiana',
              'Southern California', 'SIU Edwardsville', 'Southern Illinois', 'Southern Methodist',
              'Southern Mississippi', 'Southern Utah', 'Southern', 'St. Bonaventure', 'St. Francis (NY)',
              "St. John's (NY)", 'Stanford', 'Stephen F. Austin', 'Stetson', 'Stony Brook', 'Syracuse',
              'Tarleton State', 'Temple', 'Tennessee-Martin', 'Tennessee State', 'Tennessee Tech', 'Tennessee',
              'Texas A&M-Corpus Christi', 'Texas A&M', 'Texas-Arlington', 'Texas Christian', 'Texas-El Paso',
              'Texas-Rio Grande Valley', 'Texas-San Antonio', 'Texas Southern', 'Texas State', 'Texas Tech',
              'Texas', 'Toledo', 'Towson', 'Troy', 'Tulane', 'Tulsa', 'UCLA', 'Utah State', 'Utah Valley',
              'Utah', 'Valparaiso', 'Vanderbilt', 'Vermont', 'Villanova', 'Virginia Commonwealth', 'VMI',
              'Virginia Tech', 'Virginia', 'Wagner', 'Wake Forest', 'Washington State', 'Washington',
              'Weber State', 'West Virginia', 'Western Carolina', 'Western Illinois', 'Western Kentucky',
              'Western Michigan', 'Wichita State', 'William & Mary', 'Winthrop', 'Wisconsin', 'Wofford',
              'Wright State', 'Wyoming', 'Xavier', 'Yale', 'Youngstown State']
team_abbri = ['ABILENE-CHRISTIAN', 'AIR-FORCE', 'AKRON', 'ALABAMA-AM', 'ALABAMA-BIRMINGHAM', 'ALABAMA-STATE',
              'ALABAMA', 'ALBANY-NY', 'ALCORN-STATE', 'AMERICAN', 'APPALACHIAN-STATE', 'ARIZONA-STATE',
              'ARIZONA', 'ARKANSAS-LITTLE-ROCK', 'ARKANSAS-PINE-BLUFF', 'ARKANSAS-STATE', 'ARKANSAS', 'ARMY',
              'AUBURN', 'AUSTIN-PEAY', 'BALL-STATE', 'BAYLOR', 'BELLARMINE', 'BELMONT', 'BINGHAMTON',
              'BOISE-STATE', 'BOSTON-COLLEGE', 'BOSTON-UNIVERSITY', 'BOWLING-GREEN-STATE', 'BRADLEY',
              'BRIGHAM-YOUNG', 'BROWN', 'BRYANT', 'BUCKNELL', 'BUFFALO', 'BUTLER', 'CAL-POLY',
              'CAL-STATE-BAKERSFIELD', 'CAL-STATE-FULLERTON', 'CAL-STATE-NORTHRIDGE', 'CALIFORNIA-BAPTIST',
              'CALIFORNIA-DAVIS', 'CALIFORNIA-IRVINE', 'CALIFORNIA-RIVERSIDE', 'CALIFORNIA-SAN-DIEGO',
              'CALIFORNIA-SANTA-BARBARA', 'CALIFORNIA', 'CAMPBELL', 'CANISIUS', 'CENTRAL-ARKANSAS',
              'CENTRAL-CONNECTICUT-STATE', 'CENTRAL-FLORIDA', 'CENTRAL-MICHIGAN', 'CHARLESTON-SOUTHERN',
              'CHARLOTTE', 'CHATTANOOGA', 'CHICAGO-STATE', 'CINCINNATI', 'CITADEL', 'CLEMSON', 'CLEVELAND-STATE',
              'COASTAL-CAROLINA', 'COLGATE', 'COLLEGE-OF-CHARLESTON', 'COLORADO-STATE', 'COLORADO', 'COLUMBIA',
              'CONNECTICUT', 'COPPIN-STATE', 'CORNELL', 'CREIGHTON', 'DARTMOUTH', 'DAVIDSON', 'DAYTON',
              'DELAWARE-STATE', 'DELAWARE', 'DENVER', 'DEPAUL', 'DETROIT-MERCY', 'DIXIE-STATE', 'DRAKE',
              'DREXEL', 'DUKE', 'DUQUESNE', 'EAST-CAROLINA', 'EAST-TENNESSEE-STATE', 'EASTERN-ILLINOIS',
              'EASTERN-KENTUCKY', 'EASTERN-MICHIGAN', 'EASTERN-WASHINGTON', 'ELON', 'EVANSVILLE', 'FAIRFIELD',
              'FAIRLEIGH-DICKINSON', 'FLORIDA-AM', 'FLORIDA-ATLANTIC', 'FLORIDA-GULF-COAST',
              'FLORIDA-INTERNATIONAL', 'FLORIDA-STATE', 'FLORIDA', 'FORDHAM', 'FRESNO-STATE', 'FURMAN',
              'GARDNER-WEBB', 'GEORGE-MASON', 'GEORGE-WASHINGTON', 'GEORGETOWN', 'GEORGIA-SOUTHERN',
              'GEORGIA-STATE', 'GEORGIA-TECH', 'GEORGIA', 'GONZAGA', 'GRAMBLING', 'GRAND-CANYON', 'GREEN-BAY',
              'HAMPTON', 'HARTFORD', 'HARVARD', 'HAWAII', 'HIGH-POINT', 'HOFSTRA', 'HOLY-CROSS',
              'HOUSTON-BAPTIST', 'HOUSTON', 'HOWARD', 'IDAHO-STATE', 'IDAHO', 'ILLINOIS-CHICAGO',
              'ILLINOIS-STATE', 'ILLINOIS', 'INCARNATE-WORD', 'INDIANA-STATE', 'INDIANA', 'IONA',
              'IOWA-STATE', 'IOWA', 'IPFW', 'IUPUI', 'JACKSON-STATE', 'JACKSONVILLE-STATE', 'JACKSONVILLE',
              'JAMES-MADISON', 'KANSAS-STATE', 'KANSAS', 'KENNESAW-STATE', 'KENT-STATE', 'KENTUCKY', 'LA-SALLE',
              'LAFAYETTE', 'LAMAR', 'LEHIGH', 'LIBERTY', 'LIPSCOMB', 'LONG-BEACH-STATE',
              'LONG-ISLAND-UNIVERSITY', 'LONGWOOD', 'LOUISIANA-LAFAYETTE', 'LOUISIANA-MONROE',
              'LOUISIANA-STATE', 'LOUISIANA-TECH', 'LOUISVILLE', 'LOYOLA-IL', 'LOYOLA-MARYMOUNT',
              'LOYOLA-MD', 'MAINE', 'MANHATTAN', 'MARIST', 'MARQUETTE', 'MARSHALL', 'MARYLAND-BALTIMORE-COUNTY',
              'MARYLAND-EASTERN-SHORE', 'MARYLAND', 'MASSACHUSETTS-LOWELL', 'MASSACHUSETTS', 'MCNEESE-STATE',
              'MEMPHIS', 'MERCER', 'MERRIMACK', 'MIAMI-FL', 'MIAMI-OH', 'MICHIGAN-STATE', 'MICHIGAN',
              'MIDDLE-TENNESSEE', 'MILWAUKEE', 'MINNESOTA', 'MISSISSIPPI-STATE', 'MISSISSIPPI-VALLEY-STATE',
              'MISSISSIPPI', 'MISSOURI-KANSAS-CITY', 'MISSOURI-STATE', 'MISSOURI', 'MONMOUTH', 'MONTANA-STATE',
              'MONTANA', 'MOREHEAD-STATE', 'MORGAN-STATE', 'MOUNT-ST-MARYS', 'MURRAY-STATE', 'NAVY',
              'NEBRASKA-OMAHA', 'NEBRASKA', 'NEVADA-LAS-VEGAS', 'NEVADA', 'NEW-HAMPSHIRE', 'NEW-MEXICO-STATE',
              'NEW-MEXICO', 'NEW-ORLEANS', 'NIAGARA', 'NICHOLLS-STATE', 'NJIT', 'NORFOLK-STATE', 'NORTH-ALABAMA',
              'NORTH-CAROLINA-ASHEVILLE', 'NORTH-CAROLINA-AT', 'NORTH-CAROLINA-CENTRAL',
              'NORTH-CAROLINA-GREENSBORO', 'NORTH-CAROLINA-STATE', 'NORTH-CAROLINA-WILMINGTON',
              'NORTH-CAROLINA', 'NORTH-DAKOTA-STATE', 'NORTH-DAKOTA', 'NORTH-FLORIDA', 'NORTH-TEXAS',
              'NORTHEASTERN', 'NORTHERN-ARIZONA', 'NORTHERN-COLORADO', 'NORTHERN-ILLINOIS', 'NORTHERN-IOWA',
              'NORTHERN-KENTUCKY', 'NORTHWESTERN-STATE', 'NORTHWESTERN', 'NOTRE-DAME', 'OAKLAND', 'OHIO-STATE',
              'OHIO', 'OKLAHOMA-STATE', 'OKLAHOMA', 'OLD-DOMINION', 'ORAL-ROBERTS', 'OREGON-STATE', 'OREGON',
              'PACIFIC', 'PENN-STATE', 'PENNSYLVANIA', 'PEPPERDINE', 'PITTSBURGH', 'PORTLAND-STATE', 'PORTLAND',
              'PRAIRIE-VIEW', 'PRESBYTERIAN', 'PRINCETON', 'PROVIDENCE', 'PURDUE', 'QUINNIPIAC', 'RADFORD',
              'RHODE-ISLAND', 'RICE', 'RICHMOND', 'RIDER', 'ROBERT-MORRIS', 'RUTGERS', 'SACRAMENTO-STATE',
              'SACRED-HEART', 'SAINT-FRANCIS-PA', 'SAINT-JOSEPHS', 'SAINT-LOUIS', 'SAINT-MARYS-CA',
              'SAINT-PETERS', 'SAM-HOUSTON-STATE', 'SAMFORD', 'SAN-DIEGO-STATE', 'SAN-DIEGO', 'SAN-FRANCISCO',
              'SAN-JOSE-STATE', 'SANTA-CLARA', 'SEATTLE', 'SETON-HALL', 'SIENA', 'SOUTH-ALABAMA',
              'SOUTH-CAROLINA-STATE', 'SOUTH-CAROLINA-UPSTATE', 'SOUTH-CAROLINA', 'SOUTH-DAKOTA-STATE',
              'SOUTH-DAKOTA', 'SOUTH-FLORIDA', 'SOUTHEAST-MISSOURI-STATE', 'SOUTHEASTERN-LOUISIANA',
              'SOUTHERN-CALIFORNIA', 'SOUTHERN-ILLINOIS-EDWARDSVILLE', 'SOUTHERN-ILLINOIS', 'SOUTHERN-METHODIST',
              'SOUTHERN-MISSISSIPPI', 'SOUTHERN-UTAH', 'SOUTHERN', 'ST-BONAVENTURE', 'ST-FRANCIS-NY',
              'ST-JOHNS-NY', 'STANFORD', 'STEPHEN-F-AUSTIN', 'STETSON', 'STONY-BROOK', 'SYRACUSE',
              'TARLETON-STATE', 'TEMPLE', 'TENNESSEE-MARTIN', 'TENNESSEE-STATE', 'TENNESSEE-TECH',
              'TENNESSEE', 'TEXAS-AM-CORPUS-CHRISTI', 'TEXAS-AM', 'TEXAS-ARLINGTON', 'TEXAS-CHRISTIAN',
              'TEXAS-EL-PASO', 'TEXAS-PAN-AMERICAN', 'TEXAS-SAN-ANTONIO', 'TEXAS-SOUTHERN', 'TEXAS-STATE',
              'TEXAS-TECH', 'TEXAS', 'TOLEDO', 'TOWSON', 'TROY', 'TULANE', 'TULSA', 'UCLA', 'UTAH-STATE',
              'UTAH-VALLEY', 'UTAH', 'VALPARAISO', 'VANDERBILT', 'VERMONT', 'VILLANOVA', 'VIRGINIA-COMMONWEALTH',
              'VIRGINIA-MILITARY-INSTITUTE', 'VIRGINIA-TECH', 'VIRGINIA', 'WAGNER', 'WAKE-FOREST',
              'WASHINGTON-STATE', 'WASHINGTON', 'WEBER-STATE', 'WEST-VIRGINIA', 'WESTERN-CAROLINA',
              'WESTERN-ILLINOIS', 'WESTERN-KENTUCKY', 'WESTERN-MICHIGAN', 'WICHITA-STATE', 'WILLIAM-MARY',
              'WINTHROP', 'WISCONSIN', 'WOFFORD', 'WRIGHT-STATE', 'WYOMING', 'XAVIER', 'YALE', 'YOUNGSTOWN-STATE']

connection1 = mysql.connector.connect(
    host="ncaa-basketball.cgajpgvedr4w.us-east-1.rds.amazonaws.com",
    user="admin",
    password="nje2njjd5ha",
    database='NCAA'
)

# creating a cursor object
cursor1 = connection1.cursor()
# gives you an instance of the MySQLCursor class.
query1 = """SELECT * FROM NCAA.main_table WHERE id>1450 ORDER BY id;"""
query2 = """SELECT M.id,T.team_name AS away_team FROM NCAA.main_table AS M 
INNER JOIN NCAA.team_table AS T ON M.away = T.id_reference
WHERE M.id>1450 ORDER BY M.id;"""
query3 = """SELECT M.id,T.team_name AS home_team FROM NCAA.main_table AS M 
INNER JOIN NCAA.team_table AS T ON M.home = T.id_reference
WHERE M.id>1450 ORDER BY M.id;"""

# getting records from the table
mapped = pd.read_sql(query1, con=connection1)
Away = pd.read_sql(query2, con=connection1)
Home = pd.read_sql(query3, con=connection1)

# Looks for indexes of not empty table rows
# need_to_drop = []
# for i,x in enumerate(mapped['home_score']):
#     if (str(type(x)) != "<class 'NoneType'>") and (str(type(mapped['away_score'][i])) != "<class 'NoneType'>") and\
#     (str(type(mapped['final_result'][i])) != "<class 'NoneType'>") and \
#     (str(type(mapped['isCorrect'][i])) != "<class 'NoneType'>") and \
#     (str(type(mapped['isCloser'][i])) != "<class 'NoneType'>"):
#         need_to_drop.append(i)

# This erases the rows where it already has something in the specified columns
#mapped = mapped.drop(need_to_drop,axis=0)

# awn_Name is the name of the teams from the workbench
# awn_NCAA_Team is the corresponding team names that the sportsreference can take
awn_Name = ['Abilene Christian', 'Air Force', 'Akron', 'Alabama', 'Alabama A&M',
            'Alabama State', 'AUB', 'Albany', 'Alcorn State', 'American', 'Appalachian State',
            'Arizona', 'Arizona State', 'Arkansas', 'Arkansas State', 'Arkansas Little Rock',
            'Arkansas Pine Bluff', 'Army', 'Auburn', 'Austin Peay', 'Ball State', 'Baylor', 'Belmont',
            'Bethune Cookman', 'Binghamton', 'Boise State', 'Boston College', 'Boston University',
            'Bowling Green', 'Bradley', 'BYU', 'Brown', 'Bryant', 'Bucknell', 'Buffalo', 'Butler', 'Cal Poly',
            'Cal State Bakersfield', 'Cal St. Fullerton', 'CS Northridge', 'California', 'UC Davis',
            'UC Irvine', 'UCLA', 'UC Riverside', 'Cal Santa Barbara', 'Campbell', 'Canisius',
            'Central Arkansas', 'Central Connecticut', 'Central Florida', 'Central Michigan',
            'Charleston Southern', 'Chicago State', 'Cincinnati', 'The Citadel', 'Clemson',
            'Cleveland State', 'Coastal Carolina', 'Colgate', 'Charleston', 'Colorado', 'Colorado State',
            'Columbia', 'Connecticut', 'Coppin State', 'Cornell', 'Creighton', 'Dartmouth', 'Davidson',
            'Dayton', 'Delaware', 'Delaware State', 'Denver University', 'DePaul', 'Detroit University',
            'Drake', 'Drexel', 'Duke', 'Duquesne', 'East Carolina', 'East Tennessee State',
            'Eastern Illinois', 'Eastern Kentucky', 'Eastern Michigan', 'Eastern Washington', 'Elon',
            'Evansville', 'Fairfield', 'Fairleigh Dickinson', 'Florida', 'Florida A&M', 'Florida Atlantic',
            'Florida Gulf Coast', 'Florida International', 'Florida State', 'Fordham', 'Fresno State',
            'Furman', 'Gardner-Webb', 'George Mason', 'George Washington', 'Georgetown', 'Georgia',
            'Georgia Southern', 'Georgia State', 'Georgia Tech', 'Gonzaga', 'Grambling State',
            'Grand Canyon', 'Hampton', 'Hartford', 'Harvard', 'Hawaii', 'High Point', 'Hofstra',
            'Holy Cross', 'Houston University', 'Houston Baptist', 'Howard', 'Idaho University',
            'Idaho State', 'Illinois', 'Illinois State', 'Illinois Chicago', 'Incarnate Word',
            'Indiana University', 'Indiana State', 'Iona', 'Iowa', 'Iowa State', 'Purdue Fort Wayne',
            'IUPUI', 'Jackson State', 'Jacksonville', 'Jacksonville State', 'James Madison', 'Kansas',
            'Kansas State', 'Kennesaw State', 'Kent State', 'Kentucky', 'La Salle', 'Lafayette', 'Lamar',
            'Lehigh', 'Liberty', 'Lipscomb', 'Long Beach State', 'Long Island', 'Longwood', 'UL Lafayette',
            'UL Monroe', 'LSU', 'Louisiana Tech', 'Louisville', 'Loyola Chicago', 'Loyola MD',
            'Loyola Marymount', 'Maine', 'Manhattan', 'Marist', 'Marquette', 'Marshall', 'Maryland',
            'MD Baltimore CO', 'Maryland Eastern Shore', 'Massachusetts', 'UMass Lowell', 'McNeese State',
            'Memphis', 'Mercer', 'Miami FL', 'Miami OH', 'Michigan', 'Michigan State',
            'Middle Tennessee State', 'Minnesota University', 'Mississippi', 'Mississippi State',
            'Mississippi Valley State', 'Missouri', 'Missouri State', 'UMKC', 'Monmouth', 'Montana',
            'Montana State', 'Morehead State', 'Morgan State', "Mount St. Mary's", 'Murray State',
            'Navy', 'Nebraska', 'Nebraska Omaha', 'UNLV', 'Nevada', 'New Hampshire', 'New Mexico',
            'New Mexico State', 'New Orleans', 'Niagara', 'Nicholls State', 'NJIT', 'Norfolk State',
            'North Carolina', 'North Carolina A&T', 'North Carolina Central', 'N.C. State',
            'UNC Asheville', 'Charlotte', 'NC Greensboro', 'NC Wilmington', 'North Dakota',
            'North Dakota State', 'North Florida', 'North Texas', 'Northeastern', 'Northern Arizona',
            'Northern Colorado', 'Northern Illinois', 'Northern Iowa', 'Northern Kentucky',
            'Northwestern', 'Northwestern State', 'Notre Dame', 'Oakland', 'Ohio', 'Ohio State',
            'Oklahoma', 'Oklahoma State', 'Old Dominion', 'Oral Roberts', 'Oregon', 'Oregon State',
            'Pacific', 'Penn State', 'Pennsylvania', 'Pepperdine', 'Pittsburgh', 'Portland University',
            'Portland State', 'Prairie View A&M', 'Presbyterian', 'Princeton', 'Providence', 'Purdue',
            'Quinnipiac', 'Radford', 'Rhode Island', 'Rice', 'Richmond', 'Rider', 'Robert Morris',
            'Rutgers', 'Sacramento State', 'Sam Houston State', 'Samford', 'San Diego', 'San Diego State',
            'San Francisco', 'San Jose State', 'Santa Clara', 'Sacred Heart', 'Seattle', 'Seton Hall',
            'Siena', 'SIU Edwardsville', 'South Alabama', 'South Carolina', 'South Carolina State',
            'USC Upstate', 'South Dakota', 'South Dakota State', 'South Florida',
            'Southeast Missouri State', 'Southeastern Louisiana', 'Southern', 'USC', 'Southern Illinois',
            'SMU', 'Southern Mississippi', 'Southern Utah', 'Saint Bonaventure', 'Saint Francis NY',
            'Saint Francis PA', 'Saint Johns', 'Saint Josephs', 'Saint Louis', 'Saint Marys CA',
            'Saint Peters', 'Stanford', 'Stephen F. Austin', 'Stetson', 'Stony Brook', 'Syracuse',
            'Temple', 'Tennessee', 'Tennessee State', 'Tennessee Tech', 'Tennessee Chattanooga',
            'Tennessee Martin', 'Texas', 'Texas A&M', 'Texas A&M Corpus Chris', 'TCU', 'Texas Southern',
            'Texas State', 'Texas Tech', 'UT Arlington', 'UTEP', 'UT Rio Grande Valley',
            'Texas San Antonio', 'Toledo', 'Towson', 'Troy', 'Tulane', 'Tulsa', 'UCF', 'Utah University',
            'Utah State', 'Utah Valley', 'UTSA', 'Valparaiso', 'Vanderbilt', 'Vermont', 'Villanova',
            'Virginia', 'Virginia Commonwealth', 'VMI', 'Virginia Tech', 'Wagner', 'Wake Forest',
            'Washington University', 'Washington State', 'Weber State', 'West Virginia',
            'Western Carolina', 'Western Illinois', 'Western Kentucky', 'Western Michigan',
            'Wichita State', 'William & Mary', 'Winthrop', 'Wisconsin', 'Wisconsin Green Bay',
            'Wisconsin Milwaukee', 'Wofford', 'Wright State', 'Wyoming', 'Xavier', 'Yale',
            'Youngstown State']

awn_NCAA_Team = ['Abilene Christian', 'Air Force', 'Akron', 'Alabama', 'Alabama A&M',
                 'Alabama State', 'Alabama-Birmingham', 'Albany (NY)', 'Alcorn State',
                 'American', 'Appalachian State', 'Arizona', 'Arizona State', 'Arkansas',
                 'Arkansas State', 'Little Rock', 'Arkansas-Pine Bluff', 'Army', 'Auburn',
                 'Austin Peay', 'Ball State', 'Baylor', 'Belmont', 'Bethune-Cookman',
                 'Binghamton', 'Boise State', 'Boston College', 'Boston University',
                 'Bowling Green State', 'Bradley', 'Brigham Young', 'Brown', 'Bryant',
                 'Bucknell', 'Buffalo', 'Butler', 'Cal Poly', 'Cal State Bakersfield',
                 'Cal State Fullerton', 'Cal State Northridge', 'University of California',
                 'UC-Davis', 'UC-Irvine', 'UCLA', 'UC-Riverside', 'UC-Santa Barbara', 'Campbell',
                 'Canisius', 'Central Arkansas', 'Central Connecticut State', 'Central Florida',
                 'Central Michigan', 'Charleston Southern', 'Chicago State', 'Cincinnati',
                 'Citadel', 'Clemson', 'Cleveland State', 'Coastal Carolina', 'Colgate',
                 'College of Charleston', 'Colorado', 'Colorado State', 'Columbia', 'Connecticut',
                 'Coppin State', 'Cornell', 'Creighton', 'Dartmouth', 'Davidson', 'Dayton',
                 'Delaware', 'Delaware State', 'Denver', 'DePaul', 'Detroit Mercy', 'Drake',
                 'Drexel', 'Duke', 'Duquesne', 'East Carolina', 'East Tennessee State',
                 'Eastern Illinois', 'Eastern Kentucky', 'Eastern Michigan', 'Eastern Washington',
                 'Elon', 'Evansville', 'Fairfield', 'Fairleigh Dickinson', 'Florida', 'Florida A&M',
                 'Florida Atlantic', 'Florida Gulf Coast', 'Florida International', 'Florida State',
                 'Fordham', 'Fresno State', 'Furman', 'Gardner-Webb', 'George Mason',
                 'George Washington', 'Georgetown', 'Georgia', 'Georgia Southern', 'Georgia State',
                 'Georgia Tech', 'Gonzaga', 'Grambling', 'Grand Canyon', 'Hampton', 'Hartford',
                 'Harvard', 'Hawaii', 'High Point', 'Hofstra', 'Holy Cross', 'Houston',
                 'Houston Baptist', 'Howard', 'Idaho', 'Idaho State', 'Illinois', 'Illinois State',
                 'Illinois-Chicago', 'Incarnate Word', 'Indiana', 'Indiana State', 'Iona', 'Iowa',
                 'Iowa State', 'Purdue-Fort Wayne', 'IUPUI', 'Jackson State', 'Jacksonville',
                 'Jacksonville State', 'James Madison', 'Kansas', 'Kansas State', 'Kennesaw State',
                 'Kent State', 'Kentucky', 'La Salle', 'Lafayette', 'Lamar', 'Lehigh', 'Liberty',
                 'Lipscomb', 'Cal State Long Beach', 'Long Island University', 'Longwood',
                 'Louisiana', 'Louisiana-Monroe', 'Louisiana State', 'Louisiana Tech', 'Louisville',
                 'Loyola (IL)', 'Loyola (MD)', 'Loyola Marymount', 'Maine', 'Manhattan', 'Marist',
                 'Marquette', 'Marshall', 'Maryland', 'Maryland-Baltimore County',
                 'Maryland-Eastern Shore', 'Massachusetts', 'Massachusetts-Lowell', 'McNeese State',
                 'Memphis', 'Mercer', 'Miami (FL)', 'Miami (OH)', 'Michigan', 'Michigan State',
                 'Middle Tennessee', 'Minnesota', 'Mississippi', 'Mississippi State',
                 'Mississippi Valley State', 'Missouri', 'Missouri State', 'Missouri-Kansas City',
                 'Monmouth', 'Montana', 'Montana State', 'Morehead State', 'Morgan State',
                 "Mount St. Mary's", 'Murray State', 'Navy', 'Nebraska', 'Omaha', 'Nevada-Las Vegas',
                 'Nevada', 'New Hampshire', 'New Mexico', 'New Mexico State', 'New Orleans',
                 'Niagara', 'Nicholls State', 'NJIT', 'Norfolk State', 'North Carolina',
                 'North Carolina A&T', 'North Carolina Central', 'North Carolina State',
                 'North Carolina-Asheville', 'Charlotte', 'North Carolina-Greensboro',
                 'North Carolina-Wilmington', 'North Dakota', 'North Dakota State',
                 'North Florida', 'North Texas', 'Northeastern', 'Northern Arizona',
                 'Northern Colorado', 'Northern Illinois', 'Northern Iowa', 'Northern Kentucky',
                 'Northwestern', 'Northwestern State', 'Notre Dame', 'Oakland', 'Ohio',
                 'Ohio State', 'Oklahoma', 'Oklahoma State', 'Old Dominion', 'Oral Roberts',
                 'Oregon', 'Oregon State', 'Pacific', 'Penn State', 'Pennsylvania', 'Pepperdine',
                 'Pittsburgh', 'Portland', 'Portland State', 'Prairie View', 'Presbyterian',
                 'Princeton', 'Providence', 'Purdue', 'Quinnipiac', 'Radford', 'Rhode Island',
                 'Rice', 'Richmond', 'Rider', 'Robert Morris', 'Rutgers', 'Sacramento State',
                 'Sam Houston State', 'Samford', 'San Diego', 'San Diego State', 'San Francisco',
                 'San Jose State', 'Santa Clara', 'Sacred Heart', 'Seattle', 'Seton Hall',
                 'Siena', 'SIU Edwardsville', 'South Alabama', 'South Carolina',
                 'South Carolina State', 'South Carolina Upstate', 'South Dakota',
                 'South Dakota State', 'South Florida', 'Southeast Missouri State',
                 'Southeastern Louisiana', 'Southern', 'Southern California', 'Southern Illinois',
                 'Southern Methodist', 'Southern Mississippi', 'Southern Utah', 'St. Bonaventure',
                 'St. Francis (NY)', 'Saint Francis (PA)', "St. John's (NY)", "Saint Joseph's",
                 'Saint Louis', "Saint Mary's (CA)", "Saint Peter's", 'Stanford',
                 'Stephen F. Austin', 'Stetson', 'Stony Brook', 'Syracuse', 'Temple',
                 'Tennessee', 'Tennessee State', 'Tennessee Tech', 'Chattanooga', 'Tennessee-Martin',
                 'Texas', 'Texas A&M', 'Texas A&M-Corpus Christi', 'Texas Christian',
                 'Texas Southern', 'Texas State', 'Texas Tech', 'Texas-Arlington', 'Texas-El Paso',
                 'Texas-Rio Grande Valley', 'Texas-San Antonio', 'Toledo', 'Towson', 'Troy',
                 'Tulane', 'Tulsa', 'Central Florida', 'Utah', 'Utah State', 'Utah Valley',
                 'Texas-San Antonio', 'Valparaiso', 'Vanderbilt', 'Vermont', 'Villanova',
                 'Virginia', 'Virginia Commonwealth', 'VMI', 'Virginia Tech', 'Wagner',
                 'Wake Forest', 'Washington', 'Washington State', 'Weber State', 'West Virginia',
                 'Western Carolina', 'Western Illinois', 'Western Kentucky', 'Western Michigan',
                 'Wichita State', 'William & Mary', 'Winthrop', 'Wisconsin', 'Green Bay',
                 'Milwaukee', 'Wofford', 'Wright State', 'Wyoming', 'Xavier', 'Yale',
                 'Youngstown State']


# this makes a new list; each away team name index has the name that
# the Sportreference can recognize
ncaa_away = []
for i, x in enumerate(Away['away_team']):
    for j, y in enumerate(awn_Name):
        if x == y:
            ncaa_away.append(awn_NCAA_Team[j])


# this makes another new list; each recognizable away team name
# is matched with its corresponding abbriviation to find the game details
away_abbri = []
for i, x in enumerate(ncaa_away):
    for j, y in enumerate(team_names):
        if x == y:
            away_abbri.append(team_abbri[j])


date = []  # this used to make a list of all the game dates,
# to help find the correct game from each teams game history
pick = []  # this is used to correct any mistakes in the default picks

home_score = []  # array for home_score
away_score = []  # array for away_score
final_result = []  # array for final_result


# This makes the arrays the right length
for i, x in enumerate(away_abbri):
    # date.append(mapped['time'][i].strftime("%Y-%m-%d"))
    pick.append(0)
    try:
        # date.append(mapped['time'][i][:10])
        home_score.append(0)
        away_score.append(0)
        final_result.append(0)
        date.append(mapped['time'][i].strftime("%Y-%m-%d"))
    except:
        print('non-existent time error!')

# This fills the arrays
for i, x in enumerate(mapped['difference']):
    try:
        if mapped['difference'][i] < 0:
            pick[i] = mapped['away'][i]
        else:
            pick[i] = mapped['home'][i]
    except:
        print(i)


sched = []  # Schedule of the teams

# Uses the Schedule module to find the schedule of each away team
for i, x in enumerate(date):
    sched.append(list(Schedule(away_abbri[i]).dataframe.boxscore_index))


# This is to turn any NoneType object in sched into a str
for i in range(len(sched)):
    for j in range(len(sched[i])):
        sched[i][j] = str(sched[i][j])

for i in range(len(sched)):
    for j in range(len(sched[i])):
        print('sched:', i, "&", j, sched[i][j])

# This makes the url extension for the website,
# attached with a correpsonding index number
uri = []
for i, x in enumerate(sched):
    for j, y in enumerate(sched[i]):
        print(date[i], "-----", sched[i][j][:10])
        if date[i] == sched[i][j][:10]:  # find matching dates
            uri.append([sched[i][j], i])

for i in range(len(uri)):
    print("uri:", uri[i][0], "-", uri[i][1])


def getGameResult(url):
    url_builder = 'https://www.sports-reference.com/cbb/boxscores/' + url + '.html'
    response = requests.get(url_builder)
    # print(response)
    webText = BeautifulSoup(response.text, 'html.parser')
    # print(webText.title)
    gameResult = []
    print('hi')
    for div in webText.find_all('div', class_="scorebox"):
        firstTeam = div.find_next('div')
        secondTeam = firstTeam.find_next_sibling()
        gameInfo = secondTeam.find_next_sibling()
        gameResult.append([firstTeam.find('a', itemprop="name").string,
                           firstTeam.find('div', class_="score").string])
        gameResult.append(
            [secondTeam.find('a', itemprop="name", string=True).string,
             secondTeam.find('div', class_="score").string])
        date = gameInfo.find('div')
        location = date.find_next_sibling()
        tournament = location.find_next_sibling()
        gameResult.append([date.string, location.string, tournament.string])
    return gameResult


# Makes array of the game results of each game, and its index from the uri array
GameResult = []
for i, x in enumerate(uri):
    GameResult.append([getGameResult(uri[i][0]), uri[i][1]])


# Matches the score with with the correct team name
for i, x in enumerate(GameResult):
    if Away['away_team'][x[-1]] == x[0][0][0]:
        away_score[x[-1]] = x[0][0][1]
        home_score[x[-1]] = x[0][1][1]
    else:
        away_score[x[-1]] = x[0][1][1]
        home_score[x[-1]] = x[0][0][1]


# Deletes the columns so they can be added in later
del mapped['pick']
del mapped['away_score']
del mapped['home_score']
del mapped['final_result']
del mapped['isCorrect']
del mapped['isCloser']


# Makes the array for the isCorrect column the length of the entire table
isCorrect = []
for i, x in enumerate(mapped['difference']):
    isCorrect.append('')

# Does the math for the final_result column, using the indexes of GameResult,
# so it skips over missing games or with wrong dates
for i, x in enumerate(GameResult):
    final_result[x[-1]] = int(home_score[x[-1]]) - int(away_score[x[-1]])

# Puts the arrays where their respective columns were originally
mapped.insert(3, 'pick', pick)
mapped.insert(9, 'home_score', home_score)
mapped.insert(10, 'away_score', away_score)
mapped.insert(11, 'final_result', final_result)

# Makes the array for the isCorrect column using the condition statements
for i, x in enumerate(GameResult):
    if (mapped['difference'][x[-1]] < 0) and (final_result[x[-1]] <= mapped['difference'][x[-1]]):
        isCorrect[x[-1]] = 1
    elif (mapped['difference'][x[-1]] > 0) and (final_result[x[-1]] >= mapped['difference'][x[-1]]):
        isCorrect[x[-1]] = 1
    elif (mapped['final_result'][x[-1]] >= mapped['spread'][x[-1]]) \
            and (mapped['value'][x[-1]] >= mapped['spread'][x[-1]]):
        isCorrect[x[-1]] = 1
    elif (mapped['final_result'][x[-1]] <= mapped['spread'][x[-1]]) \
            and (mapped['value'][x[-1]] <= mapped['spread'][x[-1]]):
        isCorrect[x[-1]] = 1
    else:
        isCorrect[x[-1]] = 0

# Puts the isCorrect array back into the table
mapped.insert(12, 'isCorrect', isCorrect)

# Function to see which number elements in 'lst' is closer to 'n'


def closer(lst, n):
    lst = np.asarray(lst)
    index = (np.abs(lst - n)).argmin()
    return lst[index]


# Makes isCloser array the same length as the table
isCloser = []
for i, x in enumerate(mapped['value']):
    isCloser.append('')

# Uses to the indexes of GameResult to skip over any Null spaces
# If 'value' is closer to the difference, it places a 1 in isCloser
for i, x in enumerate(GameResult):
    if closer([float(mapped['spread'][x[-1]]), float(mapped['value'][x[-1]])], mapped['final_result'][x[-1]]) == float(mapped['value'][x[-1]]):
        isCloser[x[-1]] = 1
    else:
        isCloser[x[-1]] = 0

# Puts isCloser back into the table
mapped.insert(15, 'isCloser', isCloser)

# print('home_score:', home_score, "\n", "away_score:", away_score, "\n", "final_result:",
# final_result, '\n', 'isCloser:', isCloser, '\n', "isCorrect:", isCorrect, '\n', "pick:", pick, '\n')
#print("id:", mapped['id'], "\n", 'final_result', final_result)
connection = mysql.connector.connect(
    host="ncaa-basketball.cgajpgvedr4w.us-east-1.rds.amazonaws.com",
    user="admin",
    password="nje2njjd5ha",
    database='NCAA'
)
cursor = connection.cursor(buffered=True)

for row, rs in mapped.iterrows():
    Id = rs[0]  # ID
    pik = str(rs[3])  # pick
    h_score = rs[9]  # homescore
    a_score = rs[10]  # away_score
    fin_res = rs[11]  # final_result
    correct = rs[12]  # isCorrect
    is_clos = rs[15]  # isCloser
    query = f"""UPDATE main_table SET home_score = '{h_score}', away_score = '{a_score}', final_result = '{fin_res}', isCorrect = '{correct}', isCloser = '{is_clos}' WHERE id = '{Id}'"""
    #print(query + " the query")
    cursor.execute(query)
    connection.commit()

Done = str(datetime.datetime.now()-begin_time).split(':')
print(f'Done after {Done[0]} hours, {Done[1]} minutes, and {Done[2]} seconds')
