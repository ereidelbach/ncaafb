# Load the rvest package
library("rvest")
library("tidyr")
library("plyr")

# Specify the USA Today Coaching Websites
url_standings = 'http://www.ncaa.com/standings/football/fbs'

# Read in the specified websites
web_standings <- read_html(url_standings)

# Use CSS selectors to isolate column names
## The column names are duplicated (once in the header and once in the footer) so we must
## specify the header to avoid duplicate entries.
html_standings  <- html_nodes(web_standings, 'tbody')
html_standings2 <- html_nodes(html_standings, 'td')
text_standings  <- html_text(html_standings2)

# Convert the raw text to a dataframe
df_standings <- data.frame(matrix(unlist(text_standings), ncol=8, byrow=T),stringsAsFactors=FALSE)

# Extract column names from the dataframe
colnames(df_standings) <- c("School", "Conference W-L", "Overall W-L", "PF", "PA", "Home", "Away", "Streak")

df_standings_red <- df_standings[ grep("STREAK", df_standings$Streak, invert = TRUE),]

# Split the wins and losses into separate columns
df_standings_red <- separate(data=df_standings_red, col=`Conference W-L`, into=c("Conf. W", "Conf. L"), sep="\\-")
df_standings_red <- separate(data=df_standings_red, col=`Overall W-L`, into=c("W", "L"), sep="\\-")