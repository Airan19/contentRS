# contentRS
Repository for Flask App Code to deploy on Azure App Service

To run the app, at first make the code available at your local system by either copying the code inside the files or simply clone the repository on your local machine.

Then, navigate the project directory and open terminal and run command:
pip install -r requirements.txt 

The above command is run to automatically download the reuquired dependencies and packages.

After that, simply run the script. The localhost server will get started and you can browse the webpage through browser.

There are several different routes where you can perform operations.

1. / (home route) - the user can upload files required for analysis from this route or there is one more option available of directly seeing the analysis of the data fetched from MS SQL Server after every 24 hrs.
2. /files (display files route) - the user can query on data based on some prewritten questions or can create some custom filters through criteria and date range. There is one more section where the movies are recommended the specified user through userId.
3. Other available routes are for either processing or showing the data automatically.
