library(pacman)
p_load(newsanchor, readxl, data.table, tidyverse)


##### Scrape data from News API using the API from January 1 to May 31 2020 #####

# Generates range of alternating dates, plus one day lag
dates <- seq.Date(as.Date("2020/1/1"), as.Date("2020/05/31"), by = 2)
dates2 <- seq.Date(as.Date("2020/1/2"), as.Date("2020/05/31"), by = 2)
dates <- tibble(start_date = dates,
                end_date = dates2)
rm(dates2)

# Creates list of terms to search
terms <- c("(Chin*)",
           "(COVID) OR (COVID-19)",
           "coronavirus")

# Function to repeat the number of terms, so that they match the length of dates
repeat_names <- function(x){
  df <- tibble(term = x) %>%
    bind_rows(replicate((nrow(dates)-1), ., simplify = FALSE))
}

# Runs function repeat_names and merges with dates df
dates2 <- bind_rows(replicate(length(terms), dates, simplify = FALSE))
search_terms <- map(terms, repeat_names) %>%
  bind_rows() %>%
  cbind(dates2)
rm(dates,terms,dates2,repeat_names)

# Function to retrieve content from Google News API
get_news_api_all <- function(api_query, date_from, date_to){
  df <- get_everything_all(query = api_query, 
                           api_key = "XXXXXX", #[to be set by the user] 
                           from = date_from,
                           to = date_to)
  df <- df[["results_df"]] %>%
    bind_rows()
}

# Split data collection process in chunks
df.covid <- pmap(list(search_terms$term[1:75], 
                      search_terms$start_date[1:75],
                      search_terms$end_date[1:75]), 
                       get_news_api_all) %>%
  bind_rows()
fwrite(file = "data_files/COVID_NewsAPI01.csv", x = df.covid)
rm(df.covid)

df.covid <- pmap(list(search_terms$term[76:150], 
                      search_terms$start_date[76:150],
                      search_terms$end_date[76:150]), 
                 get_news_api_all) %>%
  bind_rows()
fwrite(file = "data_files/COVID_NewsAPI02.csv", x = df.covid)
rm(df.covid)

df.covid <- pmap(list(search_terms$term[151:228], 
                      search_terms$start_date[151:228],
                      search_terms$end_date[151:228]), 
                 get_news_api_all) %>%
  bind_rows()
fwrite(file = "data_files/COVID_NewsAPI03.csv", x = df.covid)
rm(df.covid)
