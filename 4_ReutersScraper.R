library(pacman)
p_load(httrm,jsonlite,tidyverse,plyr,lubridate,data.table,rvest,httr,XML,magrittr)


###### Use SERP API to search for Reuters news on Google over a period of time ######

secret_api_key <- "XXXX" # To be defined by user

GET(
  url = "https://serpapi.com/account",
  query = (list(api_key=secret_api_key))
) -> account
account

# Function to return the results from serpApi 

get_all_links <- function(x) {
  ## Get call to return the first page of google results
  GET(        
    url = "https://serpapi.com/search",
    query = list(q=x,
                 api_key=secret_api_key, 
                 num="100",
                 start="0",
                 google_domain="google.com",
                 hl="en",
                 device="desktop",
                 filter="0")
  ) -> res
  
  data = fromJSON(rawToChar(res$content)) ## convert to JSON format
  df1 <- rbind.fill(data$organic_results) ## create df from data on first page
  df1$rich_snippet <- NULL  ## This removes a variable in some of the dataframes that rbind.fill can't handle
  df1$sitelinks <- NULL
  
  if(length(df1$position) >= 99) {  ## Some of the results returned only 99 (rather than 100) results
    ## before paginating to page 2 - this 
    GET(
      url = "https://serpapi.com/search",
      query = list(q=x,
                   api_key=secret_api_key, 
                   num="100",
                   start=length(df1$position)+1,  ## Changing this operator to 100 pulls the data from the second page
                   device="desktop",
                   filter="0")
    ) -> res2
    data2 = fromJSON(rawToChar(res2$content))
    df2 <- rbind.fill(data2$organic_results)
    df2$rich_snippet <- NULL
    df2$sitelinks <- NULL
    
    print(paste(x, " ", "along with", length(df2$position))) ## This prints a message along with the number of cases on page 2
  } else {
    print(paste(x, " ", "with", length(df1$position)))  ## Prints the number of cases in each date range
    return(df1)
  }
  
  if(length(df2$position) >= 99) {  ## Some of the results returned only 99 (rather than 100) results
    ## before paginating to page 3 - this 
    GET(
      url = "https://serpapi.com/search",
      query = list(q=x,
                   api_key=secret_api_key, 
                   num="100",
                   start=length(df1$position)+101,  ## Changing this operator to 100 pulls the data from the second page
                   device="desktop",
                   filter="0")
    ) -> res3
    data3 = fromJSON(rawToChar(res3$content))
    df3 <- rbind.fill(data3$organic_results)
    df3$rich_snippet <- NULL
    df3$sitelinks <- NULL
    
    print(paste(x, " ", "along with", length(df3$position))) ## This prints a message along with the number of cases on page 2
  } else {
    print(paste(x, " ", "with", length(df2$position)))  ## Prints the number of cases in each date range
    df_final <- rbind.fill(df1, df2) ## rbind df for pages 1 and 2
    return(df_final)
  }
  
  if(length(df3$position) >= 99) {  ## Some of the results returned only 99 (rather than 100) results
    ## before paginating to page 4 - this 
    GET(
      url = "https://serpapi.com/search",
      query = list(q=x,
                   api_key=secret_api_key, 
                   num="100",
                   start=length(df1$position)+201,  ## Changing this operator to 100 pulls the data from the second page
                   device="desktop",
                   filter="0")
    ) -> res4
    data4 = fromJSON(rawToChar(res4$content))
    df4 <- rbind.fill(data4$organic_results)
    df4$rich_snippet <- NULL
    df4$sitelinks <- NULL
    
    print(paste(x, " ", "along with", length(df4$position))) ## This prints a message along with the number of cases on page 2
  } else {
    print(paste(x, " ", "with", length(df3$position)))  ## Prints the number of cases in each date range
    df_final <- rbind.fill(df1, df2, df3) ## rbind df for pages 1 to 4
    return(df_final)
  }
  
  if(length(df4$position) >= 99) {  ## Some of the results returned only 99 (rather than 100) results
    ## before paginating to page 4 - this 
    GET(
      url = "https://serpapi.com/search",
      query = list(q=x,
                   api_key=secret_api_key, 
                   num="100",
                   start=length(df1$position)+301,  ## Changing this operator to 100 pulls the data from the second page
                   device="desktop",
                   filter="0")
    ) -> res5
    data5 = fromJSON(rawToChar(res5$content))
    df5 <- rbind.fill(data5$organic_results)
    df5$rich_snippet <- NULL
    df5$sitelinks <- NULL
    df_final <- rbind.fill(df1, df2, df3, df4, df5) ## rbind df for pages 1 to 5
    
    print(paste(x, " ", "along with", length(df5$position))) ## This prints a message along with the number of cases on page 2
    return(df_final)                                         ## to the console so I know which cases have paginated
  } else {
    print(paste(x, " ", "with", length(df4$position)))  ## Prints the number of cases in each date range
    df_final <- rbind.fill(df1, df2, df3, df4) ## rbind df for pages 1 and 2
    return(df_final)
  }
  
}

# Build date variables for scrape date ranges
start_date <- mdy("12-31-19")
end_date <- mdy("6-1-20")
n_days <- interval(start_date,end_date)/days(1) ## Return number of days between the two dates

daily <- seq(0,153,1) ## 11 days (rather than 10) to account for the before/after overlap
before <- start_date + daily
before <- c(before, end_date)
before <- before[-1]
after <- start_date + (daily)-1
after[1] <- start_date
before
after 

rm(start_date,
   end_date,
   n_days,
   daily)

search_terms <- paste0("site:reuters.com/article China OR COVID OR coronavirus after:", after," before:", before)
data.scrape <- lapply(search_terms, get_all_links) ## Runs the get_all_links function over all search terms
data.scrape_df <- do.call(rbind.fill, data.scrape) ## Binds each result in the apnews list in to a single df
data.scrape_df$position <- NULL
data.scrape_df$related_pages_link <- NULL
name_file <- paste0("Reuters_scrape_bydate_","nsize_",eval(nrow(data.scrape_df)),".cvs")
fwrite(file = eval(name_file), x = data.scrape_df)

# Scrape articles
get_urls_reuters <- function(url){
  page <- read_html(url)
  
  web_headline <- page %>%
    html_node("meta[name='analyticsAttributes.contentTitle']") %>%
    html_attr("content")
  
  web_date <- page %>%
    html_node("meta[property='og:article:published_time']") %>%
    html_attr("content")
  
  web_text <- page %>%
    html_node("div[class='StandardArticleBody_body']") %>%
    html_text()
  
  tibble(web_headline,
         web_text,
         web_date,
         source = "reuters.com",
         url)
}
get_urls_reuters <- safely(get_urls_reuters)

reuters1 <- map(data.scrape_df$link[1:5000],get_urls_reuters) %>%
  map("result") %>%
  compact() %>%
  reduce(bind_rows)

reuters2 <- map(data.scrape_df$link[5001:10000],get_urls_reuters) %>%
  map("result") %>%
  compact() %>%
  reduce(bind_rows)

reuters3 <- map(data.scrape_df$link[10001:15000],get_urls_reuters) %>%
  map("result") %>%
  compact() %>%
  reduce(bind_rows)

reuters4 <- map(data.scrape_df$link[15000:18188],get_urls_reuters) %>%
  map("result") %>%
  compact() %>%
  reduce(bind_rows)

reuters <- rbind(reuters1,
                 reuters2,
                 reuters3,
                 reuters4)
fwrite(file = "data_files/Reuters_Scrape.csv", x = reuters)

temp <- fread("data_files/Reuters_Scrape.csv")