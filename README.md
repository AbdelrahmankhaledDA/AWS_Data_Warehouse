## Summary
This project provides the schema and ETL to a data warehouse in aws for analytics purposes at the music streaming app Sparkify.


## Data
The source data is in log files given the Amazon s3 bucket  at `s3://udacity-dend/log_data` 
and `s3://udacity-dend/song_data` containing log data and songs data respectively.

Log files contains songplay events of the users in json format 
while song_data contains list of songs details.

## Database Schema
#### Fact Table:
   * songplays
        * columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
#### Dimension Tables:
   * users
        * columns: user_id, first_name, last_name, gender, level
   * songs
        * columns: song_id, title, artist_id, year, duration
   * artists
        * columns: artist_id, name, location, lattitude, longitude
   * time
        * columns: start_time, hour, day, week, month, year, weekday
## To run the project:
   * Update the `dwh.cfg` file with you Amazon Redshift cluster credentials and IAM role.
   * Run `python create_tables.py`. This will create the database and all the tables.
   * Run `python etl.py`. This will start pipeline which will read the data from files.   


