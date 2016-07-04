#Email Traffic Predictor
If you find yourself checking your email multiple times a day and struggle with productivity, this application is just for you!
##Overview
The Email Traffic Predictor is a software application and chrome extension that provides the user 24-hour and 7-day forecasts of email traffic from their gmail account. The application helps users determine the optimial times during the workday to check email.
##Check out the Application Dashboard
You can see an example of the predictions provided by the application by going [here](52.91.14.14). At the moment, the application only provides forecasts of email traffic from my personal gmail account. In the future, I will extend the application to provide forecasts for other users as well.
##How it works
### The Data
For the sake of convenience, I used my email archieve to build the application. The [Gmail API](https://developers.google.com/gmail/api/) was used to retrieve the data. The dataset contains 38,574 messages of various sizes and variety (promotional, social, forums, etc.) dating back to 2006. Google chat messages and messages sent by me were exluded from the dataset before model building.
###The Models
The application uses two time series models to forecast email traffic. One model provides a 24-hour forecast of the user's email traffic for the current day of the week. The other model provides a 7-day forecast of the the user's email traffic. The messages were trained on daily and hourly aggregated counts of the messages retrieved from gmail.

I modeled the weekly patterns in the data with a seasonal arima model and the hourly patterns in the data with a holt-winters exponential smoothing model. An important tuning parameter for both models is the time period length. I determined through experimentation that daily count data over the last two years provided an arima model with the lowest out of sample forecasting error. Hourly count data over the last six months yielded models with the lowest out of sample forecasting error.

The application combines the forecasts from both models to provide a more accurate prediction of the email traffic for the current day of the week.

### The Application
The application regularly checks for new messages from gmail on a daily basis and updates the models accordingly. The application provides a simple dashboard for viewing the forecasts produced by the models. If the application chrome extension is installed, the user can view the forecasts by navigating to [chrome://apps](chrome://apps) and clicking on the application icon.

##Walking through the Code
Below is an overview of the code sections in this repo.
- app - contains python flask application code and scripts for building and updating the time series models.
- chrome - contains the google chrome application extension code
- models - models are stored here

##Next Steps
- Add a database to store the results of message requests from Gmail.
- Add an additional model to predict probabilities of receiving messaged tagged with various labels during different times of the day (i.e. Promotional, Social, Important, etc.).
- Create a google gagdet to provide the user a convenient way of viewing the forecasts in their Gmail account.
- Make the application scalable for use by multiple users.
