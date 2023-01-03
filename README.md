# Snowflake Blog

Simple blog app with Streamlit and Snowflake. The blog contains Create Read Delete blog capabilities. Update functionality is coming soon.

## Features
Home: simply lists all blog posts
View Posts: displays full blog posts
Add Post: create your post
Search: search blogs by title or author
Manage Blog: we can delete and check for our stats

The app uses only one table that's managed in Snowflake.

App is also using ConfigParser, python package to handle configuration files.
Create within the working folder named config_sf.ini, and place all related information. 
The config file should resemble the one below, with your info filled in:

## config_sf.ini
```sh
[Snowflake]
sfAccount = <snowflake account>
sfUser = <user name>
sfPassword = <password>
sfWarehouse = <warehouse>
sfDatabase = <database>
sfSchema = <schema>
sfRole = <role>
```
