import os
import kagglehub
import shutil
import pandas as pd

## Data Processing file

# Check if the data files are already downloaded
if "DEV _ March Madness.csv" not in os.listdir():    
    # Specify the desired download location
    download_path = os.getcwd() 

    # Download latest version to the specified path
    path = kagglehub.dataset_download("jonathanpilafas/2024-march-madness-statistical-analysis")

    print("Path to dataset files:", path)

    # Move the downloaded files from path to download_path
    for filename in os.listdir(path):
        shutil.move(os.path.join(path, filename), download_path)

    print("Files moved to:", download_path)
else:
    print("Files already downloaded")


class DataProcessing:
    def __init__(self):
        self.purpose = "This class is used to process the data for the March Madness project. The process_kenpom method prepares the KenPom data for analysis. The process_tourney method prepares the tournament data for analysis. The get_data method returns the processed data."
        self.tourney_to_season = tourney_to_season = {
                                                        "Abilene Chr": "Abilene Christian",
                                                        "SUNY Albany": "Albany",
                                                        "American Univ": "American",
                                                        'Ark Little Rock': "Arkansas Little Rock",
                                                        'Ark Pine Bluff': "Arkansas Pine Bluff",
                                                        'Bethune-Cookman': "Bethune Cookman",
                                                        'Birmingham So': 'Birmingham Southern',
                                                        'Boston Univ': 'Boston University',
                                                        'Brooklyn': 'LIU Brooklyn',
                                                        'C Michigan': 'Central Michigan',
                                                        'Cent Arkansas': 'Central Arkansas',
                                                        'Central Conn': 'Central Connecticut',
                                                        'Charleston So': 'Charleston Southern',
                                                        'Citadel': 'The Citadel',
                                                        'Coastal Car': 'Coastal Carolina',
                                                        'Col Charleston': 'Charleston',
                                                        'College of Charleston': 'Charleston',
                                                        'CS Bakersfield': 'Cal St. Bakersfield',
                                                        'CS Fullerton': 'Cal St. Fullerton',
                                                        'CS Northridge': 'Cal St. Northridge',
                                                        'CS Sacramento': 'Sacramento St.',
                                                        'E Illinois': 'Eastern Illinois',
                                                        'E Kentucky': 'Eastern Kentucky',
                                                        'E Michigan': 'Eastern Michigan',
                                                        'E Washington': 'Eastern Washington',
                                                        'ETSU': 'East Tennessee St.',
                                                        'F Dickinson': 'Fairleigh Dickinson',
                                                        'FL Atlantic': 'Florida Atlantic',
                                                        'FGCU': 'Florida Gulf Coast',
                                                        'Florida Intl': 'FIU',
                                                        'G Washington': 'George Washington',
                                                        'Ga Southern': 'Georgia Southern',
                                                        'Grambling': 'Grambling St.',
                                                        'Houston Chr': 'Houston Christian',
                                                        'IL Chicago': 'Illinois Chicago',
                                                        'PFW': 'Purdue Fort Wayne',
                                                        'Kennesaw': 'Kennesaw St.',
                                                        'Kent': 'Kent St.',
                                                        'Loy Marymount': 'Loyola Marymount',
                                                        'Loyola-Chicago': 'Loyola Chicago',
                                                        'MA Lowell': 'UMass Lowell',
                                                        'MD E Shore': 'Maryland Eastern Shore',
                                                        'Missouri KC': 'UMKC',
                                                        'Monmouth NJ': 'Monmouth',
                                                        'MS Valley St.': 'Mississippi Valley St.',
                                                        "Mt St. Mary's": "Mount St. Mary's",
                                                        'MTSU': 'Middle Tennessee',
                                                        'N Colorado': 'Northern Colorado',
                                                        'N Dakota St.': 'North Dakota St.',
                                                        'N Illinois': 'Northern Illinois',
                                                        'N Kentucky': 'Northern Kentucky',
                                                        'NC A&T': 'North Carolina A&T',
                                                        'NC Central': 'North Carolina Central',
                                                        'NC St.ate': 'NC State',
                                                        'NE Omaha': 'Nebraska Omaha',
                                                        'Northwestern LA': 'Northwestern St.',
                                                        'Prairie View': 'Prairie View A&M',
                                                        'S Carolina St.': 'South Carolina St.',
                                                        'S Dakota St.': 'South Dakota St.',
                                                        'S Illinois': 'Southern Illinois',
                                                        'SC Upstate': 'USC Upstate',
                                                        'SE Louisiana': 'Southeastern Louisiana',
                                                        'SE Missouri St.': 'Southeast Missouri St.',
                                                        'SF Austin': 'Stephen F. Austin',
                                                        'Southern Univ': 'Southern',
                                                        'St Bonaventure': 'St. Bonaventure',
                                                        'St Francis NY': 'St. Francis NY',
                                                        'St Francis PA': 'St. Francis PA',
                                                        "St John's": "St. John's",
                                                        "St Joseph's PA": "Saint Joseph's",
                                                        'St Louis': 'Saint Louis',
                                                        "St Mary's CA": "Saint Mary's",
                                                        "St Peter's": "Saint Peter's",
                                                        'TAM C. Christi': 'Texas A&M Corpus Chris',
                                                        'TN Martin': 'Tennessee Martin',
                                                        'UTRGV': 'UT Rio Grande Valley',
                                                        'TX Southern': 'Texas Southern',
                                                        'ULM': 'Louisiana Monroe',
                                                        'UT San Antonio': 'UTSA',
                                                        'W Carolina': 'Western Carolina',
                                                        'W Illinois': 'Western Illinois',
                                                        'WKU': 'Western Kentucky',
                                                        'W Michigan': 'Western Michigan',
                                                        'W Salem St.': 'Winston Salem St.',
                                                        'WI Green Bay': 'Green Bay',
                                                        'WI Milwaukee': 'Milwaukee',
                                                        'St Thomas MN': 'St. Thomas',
                                                        'Queens NC': 'Queens'
                                                    }
        

    def process_kenpom(self):
        # Load the KenPom data
        team_data = ['INT _ KenPom _ Point Distribution.csv', 'INT _ KenPom _ Offense.csv', 'INT _ KenPom _ Miscellaneous Team Stats.csv', 'INT _ KenPom _ Height.csv', 'INT _ KenPom _ Efficiency.csv', 'INT _ KenPom _ Defense.csv']
        # Define the kenpom files needed
        kenpom = pd.read_csv("INT _ KenPom _ Summary.csv")
        # Read the initial kenpom file

        # Loop through the remaining kenpom files and merge them with the initial file
        for file in team_data:
            df = pd.read_csv(file)
            if file != "INT _ KenPom _ Efficiency.csv":
                kenpom = kenpom.merge(df, on=["TeamName", "Season"], how="left")
            else:
                kenpom = kenpom.merge(df, left_on=["TeamName", "Season"], right_on=["Team", "Season"], how="left")

        kenpom = kenpom.loc[:, ~kenpom.columns.str.contains('Rank')] # Remove Rank columns -- unnecessary for analysis
        kenpom = kenpom.loc[:, ~kenpom.columns.str.contains('_y')] # Remove duplicate columns
        kenpom.columns = kenpom.columns.str.replace('_x', '') # Remove _x from column names that were duplicated

        # Replace team names that have changed over the 23 years of data
        kenpom['TeamName'] = kenpom['TeamName'].str.replace('Wisconsin Green Bay', 'Green Bay')
        kenpom['TeamName'] = kenpom['TeamName'].str.replace('Wisconsin Milwaukee', 'Milwaukee')
        kenpom['TeamName'] = kenpom['TeamName'].str.replace('Troy St.', 'Troy')
        kenpom['TeamName'] = kenpom['TeamName'].str.replace('Louisiana Lafayette','Louisiana')
        kenpom['TeamName'] = kenpom['TeamName'].str.replace('College of Charleston', 'Charleston')

        self.kenpom_processed = kenpom
        return self.kenpom_processed
    
    def process_tourney(self):
        # Load the march madness team names data
        tourney_teams = pd.read_csv("march_games/MTeams.csv")
        tourney_teams['TeamName'] = tourney_teams['TeamName'].str.replace(' St', ' St.') # Fix St. names
        tourney_teams['TeamName'] = tourney_teams['TeamName'].map(lambda x: self.tourney_to_season.get(x, x)) # Replace team names that differ from the KenPom data

        # Load the regular season results data
        season_results = pd.read_csv("march_games/MRegularSeasonCompactResults.csv")
        season_wins = season_results.groupby(['Season', 'WTeamID']).size().reset_index(name='Wins') # Get the number of wins per team per season
        season_losses = season_results.groupby(['Season', 'LTeamID']).size().reset_index(name='Losses') # Get the number of losses per team per season
        season_record = season_wins.merge(season_losses, left_on=['Season', 'WTeamID'], right_on=['Season', 'LTeamID'], how='outer') # Merge the wins and losses data
        season_record['Record'] = season_record['Wins'].fillna(0) / (season_record['Wins'].fillna(0) + season_record['Losses'].fillna(0)) # Calculate the win percentage
        season_record = season_record.merge(tourney_teams[['TeamID', 'TeamName']], left_on='WTeamID', right_on='TeamID') # Merge the team names
        season_record.drop(columns=['WTeamID', 'LTeamID'], inplace=True) # Drop unnecessary columns
        season_record = season_record[['Season', 'TeamName', 'TeamID', 'Wins', 'Losses', 'Record']] # Reorder the columns


        # Load the march madness tournament data
        tourney_games = pd.read_csv("march_games/MNCAATourneyCompactResults.csv")
        tourney_games = tourney_games[tourney_games['Season'] >= 2002] # Only use data from 2002 onwards
        seeds = pd.read_csv("march_games/MNCAATourneySeeds.csv") # Load the tournament seeds data
        tourney_games = tourney_games.merge(tourney_teams[['TeamID', 'TeamName']], left_on='WTeamID', right_on='TeamID').merge(seeds, left_on=['Season', 'WTeamID'], right_on=['Season', 'TeamID']) # Merge the winning team data
        tourney_games.drop(columns=['TeamID_x', 'TeamID_y', 'WLoc', 'NumOT'], inplace=True) # Drop unnecessary columns
        tourney_games.rename(columns={'TeamName': 'WTeamName', 'Seed': 'WSeed'}, inplace=True) # Rename columns
        tourney_games['WSeed'] = tourney_games['WSeed'].str.extract('(\d+)').astype(int) # Extract the seed number
        tourney_games = tourney_games.merge(season_record, left_on=['Season', 'WTeamID'], right_on=['Season', 'TeamID'], how='left').drop(columns=['Wins', 'Losses', 'TeamID', 'TeamName']) # Merge the season record data
        tourney_games.rename(columns={'Record': 'WRecord'}, inplace=True) # Rename the column
        tourney_games = tourney_games.merge(tourney_teams[['TeamID', 'TeamName']], left_on='LTeamID', right_on='TeamID').merge(seeds, left_on=['Season', 'LTeamID'], right_on=['Season', 'TeamID']) # Merge the losing team data
        tourney_games.drop(columns=['TeamID_x', 'TeamID_y'], inplace=True) # Drop unnecessary columns
        tourney_games.rename(columns={'TeamName': 'LTeamName', 'Seed': 'LSeed'}, inplace=True) # Rename columns
        tourney_games['LSeed'] = tourney_games['LSeed'].str.extract('(\d+)').astype(int) # Extract the seed number
        tourney_games = tourney_games.merge(season_record, left_on=['Season', 'LTeamID'], right_on=['Season', 'TeamID'], how='left').drop(columns=['Wins', 'Losses', 'TeamID', 'TeamName']) # Merge the season record data
        tourney_games.rename(columns={'Record': 'LRecord'}, inplace=True) # Rename the column

        self.tourney_processed = tourney_games
        return self.tourney_processed
    
    def get_data(self):
        matchup_stats = (self.tourney_processed.merge(self.kenpom_processed, left_on=['Season', 'WTeamName'], right_on=['Season', 'TeamName'], how='left')
            .merge(self.kenpom_processed, left_on=['Season', 'LTeamName'], right_on=['Season', 'TeamName'], suffixes=('_W', '_L'), how='left'))

        temp = (matchup_stats.isna().mean() * 100).sort_values(ascending=False) # Get the percentage of missing values
        dropped_columns = temp[temp > 1].index # Drop columns with more than 1% missing
        matchup_stats.drop(columns=dropped_columns, inplace=True) # Drop columns with more than 1% missing
        matchup_stats.drop(columns=['TeamName_W', 'TeamName_L'], inplace=True) # Drop unnecessary columns

        # Create a column to indicate which team is the higher seed
        matchup_stats["HigherSeed"] = matchup_stats.apply(lambda row: row['WTeamName'] if (row['WSeed'] < row['LSeed']) or (row['WSeed'] == row['LSeed'] and (row['WRecord'] > row['LRecord'] or (row['WRecord'] == row['LRecord'] and row['WTeamName'] < row['LTeamName']))) else row['LTeamName'], axis=1)
        matchup_stats["HigherSeedWin"] = matchup_stats.apply(lambda row: 1 if row['HigherSeed'] == row['WTeamName'] else 0, axis=1)
        
        # Identify winner and loser columns
        columns_w = [col for col in matchup_stats.columns if col.endswith('_W')]
        columns_l = [col for col in matchup_stats.columns if col.endswith('_L')]

        # Convert columns to numeric
        matchup_stats[columns_w + columns_l] = matchup_stats[columns_w + columns_l].apply(pd.to_numeric, errors='coerce')

        # Create new columns for the differences based on higher and lower seed
        for col_w in columns_w:
            col_l = col_w.replace('_W', '_L')
            if col_l in columns_l:
                # Create a boolean mask to check if the winner was the higher seed
                higher_seed_wins = matchup_stats['HigherSeed'] == matchup_stats['WTeamName']
                
                # Compute differences row-wise
                stat_name = col_w.replace('_W', '')  # Remove _W to get the base stat name
                matchup_stats[f'{stat_name}_diff'] = (
                    matchup_stats[col_w] - matchup_stats[col_l]
                ).where(higher_seed_wins, matchup_stats[col_l] - matchup_stats[col_w])

        # Remove columns that are 100% NaN
        remove = matchup_stats.isna().mean() * 100
        matchup_stats.drop(columns=remove[remove == 100].index, inplace=True)
        columns_w = [col for col in matchup_stats.columns if col.endswith('_W')]
        columns_l = [col for col in matchup_stats.columns if col.endswith('_L')]
        matchup_stats.drop(columns=columns_w + columns_l, inplace=True)
        matchup_stats['higher_seed_num'] = matchup_stats[['WSeed', 'LSeed']].min(axis=1)
        matchup_stats['lower_seed_num'] = matchup_stats[['WSeed', 'LSeed']].max(axis=1)
        matchup_stats['higher_record'] = matchup_stats.apply(lambda row: row['WRecord'] if row['HigherSeed'] == row['WTeamName'] else row['LRecord'], axis=1)
        matchup_stats['lower_record'] = matchup_stats.apply(lambda row: row['WRecord'] if row['HigherSeed'] != row['WTeamName'] else row['LRecord'], axis=1)
        return matchup_stats
