# Load the rvest package
library("rvest")

# Specify the USA Today Coaching Websites
url_head = 'http://sports.usatoday.com/ncaa/salaries/'
url_asst = 'http://sports.usatoday.com/ncaa/salaries/football/assistant'
url_str  = 'http://sports.usatoday.com/ncaa/salaries/football/strength'
url_bb   = 'http://sports.usatoday.com/ncaa/salaries/mens-basketball/coach'

# Read in the specified websites
sal_head_web <- read_html(url_head)
sal_asst_web <- read_html(url_asst)
sal_str_web  <- read_html(url_str)
sal_bb_web   <- read_html(url_bb)

# Use CSS selectors to isolate column names
## The column names are duplicated (once in the header and once in the footer) so we must
## specify the header to avoid duplicate entries.
cols_head_html <- html_nodes(sal_head_web, 'thead')
cols_head_html2 <- html_nodes(cols_head_html, 'th')
cols_head_text <- html_text(cols_head_html2)

cols_asst_html <- html_nodes(sal_asst_web, 'thead')
cols_asst_html2 <- html_nodes(cols_asst_html, 'th')
cols_asst_text <- html_text(cols_asst_html2)

cols_str_html <- html_nodes(sal_str_web, 'thead')
cols_str_html2 <- html_nodes(cols_str_html, 'th')
cols_str_text <- html_text(cols_str_html2)

cols_bb_html <- html_nodes(sal_bb_web, 'thead')
cols_bb_html2 <- html_nodes(cols_bb_html, 'th')
cols_bb_text <- html_text(cols_bb_html2)

# Use CSS selectors to isolate the tables of interest
sal_head_html <- html_nodes(sal_head_web, 'td')
sal_asst_html <- html_nodes(sal_asst_web, 'td')
sal_str_html  <- html_nodes(sal_str_web, 'td')
sal_bb_html   <- html_nodes(sal_bb_web, 'td')

# Convert the scraped data to text
sal_head_text <- html_text(sal_head_html)
sal_asst_text <- html_text(sal_asst_html)
sal_str_text  <- html_text(sal_str_html)
sal_bb_text   <- html_text(sal_bb_html)

# Convert the raw text to a dataframe
sal_head_df <- data.frame(matrix(unlist(sal_head_text), nrow=130, byrow=T), stringsAsFactors=FALSE)
sal_asst_df <- data.frame(matrix(unlist(sal_asst_text), nrow=1166, byrow=T), stringsAsFactors=FALSE)
sal_str_df  <- data.frame(matrix(unlist(sal_str_text), nrow=129, byrow=T), stringsAsFactors=FALSE)
sal_bb_df   <- data.frame(matrix(unlist(sal_bb_text), nrow=68, byrow=T), stringsAsFactors=FALSE)

# Add column names to the dataframes
colnames(sal_head_df) <- cols_head_text
colnames(sal_asst_df) <- cols_asst_text
colnames(sal_str_df)  <- cols_str_text
colnames(sal_bb_df)   <- cols_bb_text

# All Salary variables need to be converted to numeric values for easy computations
## This means we will need to strip out all $ signs and commas

head_pay = c('School Pay','Total Pay','Max Bonus','Bonuses Paid (2016-17)','Asst Pay Total','School Buyout AS OF 12/1/17')
asst_pay = c('School Pay','Total Pay','Max Bonus','Asst Pay Total','School Buyout AS OF 12/1/17')
str_pay = c('School Pay','Total Pay','Max Bonus','Asst Pay Total','School Buyout AS OF 12/1/17')
bb_pay = c('School Pay','Total Pay','Max Bonus','School Buyout AS OF 12/1/17')

convert_sal_to_num <- function(dataframe,column_vec){
  for (i in 1:length(column_vec)){
     dataframe[,column_vec[i]] <- as.numeric(gsub("[\\$,]","",dataframe[,column_vec[i]]))
  }
  return(dataframe)
}

df_head_sal <- convert_sal_to_num(sal_head_df, head_pay)
df_asst_sal <- convert_sal_to_num(sal_asst_df, asst_pay)
df_str_sal  <- convert_sal_to_num(sal_str_df, str_pay)
df_bb_sal   <- convert_sal_to_num(sal_bb_df, bb_pay)

sum(sal_head_df_tmp$`School Pay`, na.rm = TRUE)

### There is a problem here though: 11 teams don't have matching spellings between the two data sets
###   We will correct this manually since it's such a small number
vector_orig <- c("Alabama at Birmingham","Central Florida","Miami (Ohio)","Miami (Fla.)","Mississippi",
                 "Nevada-Las Vegas","Southern California","Southern Mississippi","Texas Christian",
                 "Texas-El Paso","Texas-San Antonio")
vector_new  <- c("UAB","UCF","Miami (OH)","Miami (FL)","Ole Miss","UNLV","USC","Southern Miss","TCU","UTEP","UTSA")

rename_schools <- function(dataframe) {
  for (i in 1:length(vector_orig)){
    dataframe[which(dataframe[,"School"] == vector_orig[i], arr.ind = TRUE),"School"] <- vector_new[i]
  }
  return(dataframe)
}
df_head_sal <- rename_schools(df_head_sal)
df_asst_sal <- rename_schools(df_asst_sal)
df_str_sal  <- rename_schools(df_str_sal)
df_bb_sal   <- rename_schools(df_bb_sal)