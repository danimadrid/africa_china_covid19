library(pacman)
p_load(tidyverse, rvest, httr, tictoc, furrr, magrittr, reticulate, data.table, dplyr, R.utils)

Sys.setenv(RETICULATE_PYTHON = "[...]") # To be set up by the user
reticulate::py_config()
plan(multisession(workers = 7))

###### Generate list of links from GDELT to scrape ######

# Get a manually-generated list of African sources
african_sources <- read.xlsx(xlsxFile = "source_files/AFR_media_sources.xlsx", sheet = "en") %>%
  mutate(country_iso = replace_na(country_iso, "NB")) %>% 
  mutate(medium_name = str_replace_all(medium_name, "http://www.|https://www.|http://|https://", "")) %>%
  distinct(medium_name,.keep_all = TRUE) %>%
  select(country_name,
         country_iso,
         medium_name)

african_sourcesFR <- read.xlsx(xlsxFile = "source_files/AFR_media_sources.xlsx", sheet = "fr") %>%
  mutate(medium_name = str_replace_all(medium_name, "http://www.|https://www.|http://|https://", "")) %>%
  distinct(medium_name,.keep_all = TRUE) %>%
  select(country_name,
         country_iso,
         medium_name)

# Read raw GDELT data
covid_gdelt_files <- list.files(pattern = "COVID", path = "sources_files/") %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble()
  }) %>%
  bind_rows()

# Save African links in GDELT EN
covid_gdelt_files_AFR <- covid_gdelt_files %>%
  select(web_url = DocumentIdentifier,
         date_sources = GKGRECORDID,
         medium_name = SourceCommonName) %>%
  right_join(african_sources)
fwrite(covid_gdelt_files_AFR, "data_files/COVID_GDELT_AFROnly.csv")

# Save African links in GDELT FR
covid_gdelt_files_AFR <- covid_gdelt_files %>%
  select(web_url = DocumentIdentifier,
         date_sources = GKGRECORDID,
         medium_name = SourceCommonName) %>%
  right_join(african_sourcesFR)
fwrite(covid_gdelt_files_AFR, "data_files/COVID_GDELT_AFROnly_FR.csv")

# Save Chinese links in GDELT
covid_gdelt_files_CN <- covid_gdelt_files %>%
  select(web_url = DocumentIdentifier,
         date_sources = GKGRECORDID,
         medium_name = SourceCommonName) %>%
  right_join(tibble(medium_name = c("chinadaily.com.cn", 
                                    "globaltimes.cn", 
                                    "xinhuanet.com", 
                                    "chinaview.cn", 
                                    "news.cn",
                                    "cgtn.com",
                                    "people.cn")))
fwrite(covid_gdelt_files_CN, "data_files/COVID_GDELT_CNOnly.csv")

# Save Reuters + AFP + BBC + CNN links in GDELT
covid_gdelt_files_West <- covid_gdelt_files %>%
  select(web_url = DocumentIdentifier,
         date_sources = GKGRECORDID,
         medium_name = SourceCommonName) %>%
  right_join(tibble(medium_name = c("reuters.com", 
                                    "bbc.com", 
                                    "bbc.co.uk", 
                                    "afp.com", 
                                    "cnn.com",
                                    "apnews.com")))
fwrite(covid_gdelt_files_West, "data_files/COVID_GDELT_WestOnly.csv")

covid_gdelt_files %>%
  group_by(country_iso) %>%
  count() %>%
  arrange(desc(n))


###### Step 1 CN - Get list of links from file to download ######
df <- fread("data_files/COVID_GDELT_CNOnly.csv")

df %<>%
  distinct(df$web_url, .keep_all = TRUE)

###### Step 2 CN - Split list of links into 10 parts ######
split_links <- sample(1:10, 
                      size = length(df$web_url), 
                      replace = TRUE, 
                      prob = c(0.1,0.1,0.1,0.1,0.1,
                               0.1,0.1,0.1,0.1,0.1))

list_links1 <- df$web_url[split_links == 1]
list_links2 <- df$web_url[split_links == 2]
list_links3 <- df$web_url[split_links == 3]
list_links4 <- df$web_url[split_links == 4]
list_links5 <- df$web_url[split_links == 5]
list_links6 <- df$web_url[split_links == 6]
list_links7 <- df$web_url[split_links == 7]
list_links8 <- df$web_url[split_links == 8]
list_links9 <- df$web_url[split_links == 9]
list_links0 <- df$web_url[split_links == 10]

###### Step 3 CN - Folds 1 & 2 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links1), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links1[splitting_urls == 1]
list_urls2 <- list_links1[splitting_urls == 2]
list_urls3 <- list_links1[splitting_urls == 3]
list_urls4 <- list_links1[splitting_urls == 4]
list_urls5 <- list_links1[splitting_urls == 5]

setwd(dir = "[...]") # To be set up by the user based on location of Python environment setup

get_text_from_py <- function(url_link){
  
  withTimeout({
    traf <- import("trafilatura")
    requests <- import("requests")
    rand <- import("requests_random_user_agent")
    goose <- import("goose3")
    
    r <- traf$fetch_url(url_link)
    web_text <- traf$extract(r)
    
    web_date <- traf$metadata$extract_metadata(r) %>%
      str_extract("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    
    g = goose$Goose({browser_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2)'})
    
    article <- g$extract(url = url_link)
    web_text2 <- article$cleaned_text
    web_date2 <- as.character(article$publish_datetime_utc)
    
    if (is.null(web_date2) == TRUE){
      web_date2 <- ""
    }
    
    if (is.null(web_text2) == TRUE){
      web_text2 <- ""
    }
    
    g$close()
    
    if (length(web_text) == 0 & length(web_text2) == 0) {
      
      source_python("source_files/python_justext.py")
      web_text <- (py_capture_output(get_web_texts(url_link)))
      
      web_date <- as.character(Sys.Date())
      web_date2 <- as.character(Sys.time())
    }
    
    if (is.null(web_text) == TRUE){
      web_text <- ""
    }
    
    if (length(web_date) == 0){
      web_date <- ""
    }
    
    if (length(web_date2) == 0){
      web_date2 <- ""
    }
    
    tibble(web_url = url_link,
           web_date = web_date,
           web_date2 = web_date2,
           web_sysdate = Sys.Date(),
           web_text = web_text,
           web_text2 = web_text2)
  }, timeout = 10, onTimeout = "silent")
  
}
get_text_from_py <- safely(get_text_from_py)

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links2), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links2[splitting_urls == 1]
list_urls7 <- list_links2[splitting_urls == 2]
list_urls8 <- list_links2[splitting_urls == 3]
list_urls9 <- list_links2[splitting_urls == 4]
list_urls0 <- list_links2[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links1) %>%
  rbind(tibble(web_url = list_links2)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextCN_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtext_",timestamp,".csv",sep = ""))

###### Step 4 CN - Folds 3 & 4 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links3), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links3[splitting_urls == 1]
list_urls2 <- list_links3[splitting_urls == 2]
list_urls3 <- list_links3[splitting_urls == 3]
list_urls4 <- list_links3[splitting_urls == 4]
list_urls5 <- list_links3[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links4), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links4[splitting_urls == 1]
list_urls7 <- list_links4[splitting_urls == 2]
list_urls8 <- list_links4[splitting_urls == 3]
list_urls9 <- list_links4[splitting_urls == 4]
list_urls0 <- list_links4[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links3) %>%
  rbind(tibble(web_url = list_links4)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextCN_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtext_",timestamp,".csv",sep = ""))

###### Step 5 CN - Folds 5 & 6 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links5), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links5[splitting_urls == 1]
list_urls2 <- list_links5[splitting_urls == 2]
list_urls3 <- list_links5[splitting_urls == 3]
list_urls4 <- list_links5[splitting_urls == 4]
list_urls5 <- list_links5[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links6), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links6[splitting_urls == 1]
list_urls7 <- list_links6[splitting_urls == 2]
list_urls8 <- list_links6[splitting_urls == 3]
list_urls9 <- list_links6[splitting_urls == 4]
list_urls0 <- list_links6[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links5) %>%
  rbind(tibble(web_url = list_links6)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextCN_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtext_",timestamp,".csv",sep = ""))

###### Step 6 CN - Folds 7 & 8 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links7), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links7[splitting_urls == 1]
list_urls2 <- list_links7[splitting_urls == 2]
list_urls3 <- list_links7[splitting_urls == 3]
list_urls4 <- list_links7[splitting_urls == 4]
list_urls5 <- list_links7[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links8), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links8[splitting_urls == 1]
list_urls7 <- list_links8[splitting_urls == 2]
list_urls8 <- list_links8[splitting_urls == 3]
list_urls9 <- list_links8[splitting_urls == 4]
list_urls0 <- list_links8[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links7) %>%
  rbind(tibble(web_url = list_links8)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextCN_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtext_",timestamp,".csv",sep = ""))

###### Step 7 CN - Folds 9 & 10 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links9), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links9[splitting_urls == 1]
list_urls2 <- list_links9[splitting_urls == 2]
list_urls3 <- list_links9[splitting_urls == 3]
list_urls4 <- list_links9[splitting_urls == 4]
list_urls5 <- list_links9[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links0), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links0[splitting_urls == 1]
list_urls7 <- list_links0[splitting_urls == 2]
list_urls8 <- list_links0[splitting_urls == 3]
list_urls9 <- list_links0[splitting_urls == 4]
list_urls0 <- list_links0[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links9) %>%
  rbind(tibble(web_url = list_links0)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextCN_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtext_",timestamp,".csv",sep = ""))




###### Step 8a CN - Retry failed links (round 1) ######

missing_df <- list.files(pattern = "missingtextCN_", path = "data_files/") %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble()
  }) %>%
  bind_rows() %>%
  filter(web_link != "web_url")

missing_df %<>%
  mutate(flag = str_detect(web_link, "spanish|french")) %>%
  filter(flag != TRUE) %>%
  select(-flag)

list_urls1 <- missing_df$web_link

get_text_from_py <- function(url_link){ # Extended timeout 
  
  withTimeout({
    traf <- import("trafilatura")
    requests <- import("requests")
    rand <- import("requests_random_user_agent")
    goose <- import("goose3")
    
    r <- traf$fetch_url(url_link)
    web_text <- traf$extract(r)
    
    web_date <- traf$metadata$extract_metadata(r) %>%
      str_extract("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    
    g = goose$Goose({browser_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2)'})
    
    article <- g$extract(url = url_link)
    web_text2 <- article$cleaned_text
    web_date2 <- as.character(article$publish_datetime_utc)
    
    if (is.null(web_date2) == TRUE){
      web_date2 <- ""
    }
    
    if (is.null(web_text2) == TRUE){
      web_text2 <- ""
    }
    
    g$close()
    
    if (length(web_text) == 0 & length(web_text2) == 0) {
      
      source_python("source_files/python_justext.py")
      web_text <- (py_capture_output(get_web_texts(url_link)))
      
      web_date <- as.character(Sys.Date())
      web_date2 <- as.character(Sys.time())
    }
    
    if (is.null(web_text) == TRUE){
      web_text <- ""
    }
    
    if (length(web_date) == 0){
      web_date <- ""
    }
    
    if (length(web_date2) == 0){
      web_date2 <- ""
    }
    
    tibble(web_url = url_link,
           web_date = web_date,
           web_date2 = web_date2,
           web_sysdate = Sys.Date(),
           web_text = web_text,
           web_text2 = web_text2)
  }, timeout = 30, onTimeout = "silent")
  
} 
get_text_from_py <- safely(get_text_from_py)

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_missing <- tibble(web_url = list_urls1) %>%
  anti_join(tibble(web_url = scraped_text_safe1$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe1, 
       paste0("data_files/fulltextCN_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextCN_",timestamp,".csv",sep = ""))


###### Step 8b CN - Retry failed links (round 2 - extra timeout) ######
get_text_from_py <- function(url_link){
  
  withTimeout({
    traf <- import("trafilatura")
    requests <- import("requests")
    rand <- import("requests_random_user_agent")
    goose <- import("goose3")
    
    r <- traf$fetch_url(url_link)
    web_text <- traf$extract(r)
    
    web_date <- traf$metadata$extract_metadata(r) %>%
      str_extract("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    
    g = goose$Goose({browser_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2)'})
    
    article <- g$extract(url = url_link)
    web_text2 <- article$cleaned_text
    web_date2 <- as.character(article$publish_datetime_utc)
    
    if (is.null(web_date2) == TRUE){
      web_date2 <- ""
    }
    
    if (is.null(web_text2) == TRUE){
      web_text2 <- ""
    }
    
    g$close()
    
    if (length(web_text) == 0 & length(web_text2) == 0) {
      
      source_python("source_files/python_justext.py")
      web_text <- (py_capture_output(get_web_texts(url_link)))
      
      web_date <- as.character(Sys.Date())
      web_date2 <- as.character(Sys.time())
    }
    
    if (is.null(web_text) == TRUE){
      web_text <- ""
    }
    
    if (length(web_date) == 0){
      web_date <- ""
    }
    
    if (length(web_date2) == 0){
      web_date2 <- ""
    }
    
    tibble(web_url = url_link,
           web_date = web_date,
           web_date2 = web_date2,
           web_sysdate = Sys.Date(),
           web_text = web_text,
           web_text2 = web_text2)
  }, timeout = 30, onTimeout = "silent")
  
}
get_text_from_py <- safely(get_text_from_py)

list_urls1 <- scraped_text_missing$web_url

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_missing <- tibble(web_url = list_urls1) %>%
  anti_join(tibble(web_url = scraped_text_safe1$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe1, 
       paste0("data_files/fulltextCN_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextCN_",timestamp,".csv",sep = ""))


###### Step 1 WE - Get list of links from file to download ######
df <- fread("data_files/COVID_GDELT_WestOnly.csv")

df %<>%
  distinct(df$web_url, .keep_all = TRUE)

###### Step 2 WE - Split list of links into 10 parts ######
split_links <- sample(1:10, 
                      size = length(df$web_url), 
                      replace = TRUE, 
                      prob = c(0.1,0.1,0.1,0.1,0.1,
                               0.1,0.1,0.1,0.1,0.1))

list_links1 <- df$web_url[split_links == 1]
list_links2 <- df$web_url[split_links == 2]
list_links3 <- df$web_url[split_links == 3]
list_links4 <- df$web_url[split_links == 4]
list_links5 <- df$web_url[split_links == 5]
list_links6 <- df$web_url[split_links == 6]
list_links7 <- df$web_url[split_links == 7]
list_links8 <- df$web_url[split_links == 8]
list_links9 <- df$web_url[split_links == 9]
list_links0 <- df$web_url[split_links == 10]

###### Step 3 WE - Folds 1 & 2 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links1), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links1[splitting_urls == 1]
list_urls2 <- list_links1[splitting_urls == 2]
list_urls3 <- list_links1[splitting_urls == 3]
list_urls4 <- list_links1[splitting_urls == 4]
list_urls5 <- list_links1[splitting_urls == 5]

setwd(dir = "[...]") # To be set up by the user based on location of Python environment setup

get_text_from_py <- function(url_link){
  
  withTimeout({
    traf <- import("trafilatura")
    requests <- import("requests")
    rand <- import("requests_random_user_agent")
    goose <- import("goose3")
    
    r <- traf$fetch_url(url_link)
    web_text <- traf$extract(r)
    
    web_date <- traf$metadata$extract_metadata(r) %>%
      str_extract("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    
    g = goose$Goose({browser_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2)'})
    
    article <- g$extract(url = url_link)
    web_text2 <- article$cleaned_text
    web_date2 <- as.character(article$publish_datetime_utc)
    
    if (is.null(web_date2) == TRUE){
      web_date2 <- ""
    }
    
    if (is.null(web_text2) == TRUE){
      web_text2 <- ""
    }
    
    g$close()
    
    if (length(web_text) == 0 & length(web_text2) == 0) {
      
      source_python("source_files/python_justext.py")
      web_text <- (py_capture_output(get_web_texts(url_link)))
      
      web_date <- as.character(Sys.Date())
      web_date2 <- as.character(Sys.time())
    }
    
    if (is.null(web_text) == TRUE){
      web_text <- ""
    }
    
    if (length(web_date) == 0){
      web_date <- ""
    }
    
    if (length(web_date2) == 0){
      web_date2 <- ""
    }
    
    tibble(web_url = url_link,
           web_date = web_date,
           web_date2 = web_date2,
           web_sysdate = Sys.Date(),
           web_text = web_text,
           web_text2 = web_text2)
  }, timeout = 10, onTimeout = "silent")
  
}
get_text_from_py <- safely(get_text_from_py)

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links2), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links2[splitting_urls == 1]
list_urls7 <- list_links2[splitting_urls == 2]
list_urls8 <- list_links2[splitting_urls == 3]
list_urls9 <- list_links2[splitting_urls == 4]
list_urls0 <- list_links2[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links1) %>%
  rbind(tibble(web_url = list_links2)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextWE_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextWE_",timestamp,".csv",sep = ""))

###### Step 4 WE - Folds 3 & 4 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links3), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links3[splitting_urls == 1]
list_urls2 <- list_links3[splitting_urls == 2]
list_urls3 <- list_links3[splitting_urls == 3]
list_urls4 <- list_links3[splitting_urls == 4]
list_urls5 <- list_links3[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links4), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links4[splitting_urls == 1]
list_urls7 <- list_links4[splitting_urls == 2]
list_urls8 <- list_links4[splitting_urls == 3]
list_urls9 <- list_links4[splitting_urls == 4]
list_urls0 <- list_links4[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links3) %>%
  rbind(tibble(web_url = list_links4)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextWE_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextWE_",timestamp,".csv",sep = ""))

###### Step 5 WE - Folds 5 & 6 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links5), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links5[splitting_urls == 1]
list_urls2 <- list_links5[splitting_urls == 2]
list_urls3 <- list_links5[splitting_urls == 3]
list_urls4 <- list_links5[splitting_urls == 4]
list_urls5 <- list_links5[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links6), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links6[splitting_urls == 1]
list_urls7 <- list_links6[splitting_urls == 2]
list_urls8 <- list_links6[splitting_urls == 3]
list_urls9 <- list_links6[splitting_urls == 4]
list_urls0 <- list_links6[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links5) %>%
  rbind(tibble(web_url = list_links6)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextWE_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextWE_",timestamp,".csv",sep = ""))

###### Step 6 WE - Folds 7 & 8 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links7), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links7[splitting_urls == 1]
list_urls2 <- list_links7[splitting_urls == 2]
list_urls3 <- list_links7[splitting_urls == 3]
list_urls4 <- list_links7[splitting_urls == 4]
list_urls5 <- list_links7[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links8), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links8[splitting_urls == 1]
list_urls7 <- list_links8[splitting_urls == 2]
list_urls8 <- list_links8[splitting_urls == 3]
list_urls9 <- list_links8[splitting_urls == 4]
list_urls0 <- list_links8[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links7) %>%
  rbind(tibble(web_url = list_links8)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextWE_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextWE_",timestamp,".csv",sep = ""))

###### Step 7 WE - Folds 9 & 10 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links9), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links9[splitting_urls == 1]
list_urls2 <- list_links9[splitting_urls == 2]
list_urls3 <- list_links9[splitting_urls == 3]
list_urls4 <- list_links9[splitting_urls == 4]
list_urls5 <- list_links9[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links0), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links0[splitting_urls == 1]
list_urls7 <- list_links0[splitting_urls == 2]
list_urls8 <- list_links0[splitting_urls == 3]
list_urls9 <- list_links0[splitting_urls == 4]
list_urls0 <- list_links0[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links9) %>%
  rbind(tibble(web_url = list_links0)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextWE_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextWE_",timestamp,".csv",sep = ""))




###### Step 8 WE - Retry failed links ######

missing_df <- list.files(pattern = "missingtextWE_", path = "data_files/") %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble()
  }) %>%
  bind_rows() %>%
  filter(web_link != "web_url")

list_urls1 <- missing_df$web_link

get_text_from_py <- function(url_link){ # Extended time out time
  
  withTimeout({
    traf <- import("trafilatura")
    requests <- import("requests")
    rand <- import("requests_random_user_agent")
    goose <- import("goose3")
    
    r <- traf$fetch_url(url_link)
    web_text <- traf$extract(r)
    
    web_date <- traf$metadata$extract_metadata(r) %>%
      str_extract("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    
    g = goose$Goose({browser_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2)'})
    
    article <- g$extract(url = url_link)
    web_text2 <- article$cleaned_text
    web_date2 <- as.character(article$publish_datetime_utc)
    
    if (is.null(web_date2) == TRUE){
      web_date2 <- ""
    }
    
    if (is.null(web_text2) == TRUE){
      web_text2 <- ""
    }
    
    g$close()
    
    if (length(web_text) == 0 & length(web_text2) == 0) {
      
      source_python("source_files/python_justext.py")
      web_text <- (py_capture_output(get_web_texts(url_link)))
      
      web_date <- as.character(Sys.Date())
      web_date2 <- as.character(Sys.time())
    }
    
    if (is.null(web_text) == TRUE){
      web_text <- ""
    }
    
    if (length(web_date) == 0){
      web_date <- ""
    }
    
    if (length(web_date2) == 0){
      web_date2 <- ""
    }
    
    tibble(web_url = url_link,
           web_date = web_date,
           web_date2 = web_date2,
           web_sysdate = Sys.Date(),
           web_text = web_text,
           web_text2 = web_text2)
  }, timeout = 30, onTimeout = "silent")
  
}
get_text_from_py <- safely(get_text_from_py)

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_missing <- tibble(web_url = list_urls1) %>%
  anti_join(tibble(web_url = scraped_text_safe1$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe1, 
       paste0("data_files/fulltextWE_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextWE_",timestamp,".csv",sep = ""))




###### Step 1 AFR - Get list of links from file to download ######
df <- fread("data_files/COVID_GDELT_AFROnly.csv")
df %<>%
  distinct(df$web_url, .keep_all = TRUE)

###### Step 2 AFR - Split list of links into 10 parts ######
split_links <- sample(1:10, 
                      size = length(df$web_url), 
                      replace = TRUE, 
                      prob = c(0.1,0.1,0.1,0.1,0.1,
                               0.1,0.1,0.1,0.1,0.1))

list_links1 <- df$web_url[split_links == 1]
list_links2 <- df$web_url[split_links == 2]
list_links3 <- df$web_url[split_links == 3]
list_links4 <- df$web_url[split_links == 4]
list_links5 <- df$web_url[split_links == 5]
list_links6 <- df$web_url[split_links == 6]
list_links7 <- df$web_url[split_links == 7]
list_links8 <- df$web_url[split_links == 8]
list_links9 <- df$web_url[split_links == 9]
list_links0 <- df$web_url[split_links == 10]

###### Step 3 AFR - Folds 1 & 2 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links1), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links1[splitting_urls == 1]
list_urls2 <- list_links1[splitting_urls == 2]
list_urls3 <- list_links1[splitting_urls == 3]
list_urls4 <- list_links1[splitting_urls == 4]
list_urls5 <- list_links1[splitting_urls == 5]

setwd(dir = "[...]") # To be set up by the user based on location of Python environment setup

get_text_from_py <- function(url_link){
  
  withTimeout({
    traf <- import("trafilatura")
    requests <- import("requests")
    rand <- import("requests_random_user_agent")
    goose <- import("goose3")
    
    r <- traf$fetch_url(url_link)
    web_text <- traf$extract(r)
    
    web_date <- traf$metadata$extract_metadata(r) %>%
      str_extract("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    
    g = goose$Goose({browser_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2)'})
    
    article <- g$extract(url = url_link)
    web_text2 <- article$cleaned_text
    web_date2 <- as.character(article$publish_datetime_utc)
    
    if (is.null(web_date2) == TRUE){
      web_date2 <- ""
    }
    
    if (is.null(web_text2) == TRUE){
      web_text2 <- ""
    }
    
    g$close()
    
    if (length(web_text) == 0 & length(web_text2) == 0) {
      
      source_python("source_files/python_justext.py")
      web_text <- (py_capture_output(get_web_texts(url_link)))
      
      web_date <- as.character(Sys.Date())
      web_date2 <- as.character(Sys.time())
    }
    
    if (is.null(web_text) == TRUE){
      web_text <- ""
    }
    
    if (length(web_date) == 0){
      web_date <- ""
    }
    
    if (length(web_date2) == 0){
      web_date2 <- ""
    }
    
    tibble(web_url = url_link,
           web_date = web_date,
           web_date2 = web_date2,
           web_sysdate = Sys.Date(),
           web_text = web_text,
           web_text2 = web_text2)
  }, timeout = 10, onTimeout = "silent")
  
}
get_text_from_py <- safely(get_text_from_py)

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links2), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links2[splitting_urls == 1]
list_urls7 <- list_links2[splitting_urls == 2]
list_urls8 <- list_links2[splitting_urls == 3]
list_urls9 <- list_links2[splitting_urls == 4]
list_urls0 <- list_links2[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links1) %>%
  rbind(tibble(web_url = list_links2)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAFR_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAFR_",timestamp,".csv",sep = ""))

###### Step 4 AFR - Folds 3 & 4 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links3), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links3[splitting_urls == 1]
list_urls2 <- list_links3[splitting_urls == 2]
list_urls3 <- list_links3[splitting_urls == 3]
list_urls4 <- list_links3[splitting_urls == 4]
list_urls5 <- list_links3[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links4), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links4[splitting_urls == 1]
list_urls7 <- list_links4[splitting_urls == 2]
list_urls8 <- list_links4[splitting_urls == 3]
list_urls9 <- list_links4[splitting_urls == 4]
list_urls0 <- list_links4[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links3) %>%
  rbind(tibble(web_url = list_links4)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAFR_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAFR_",timestamp,".csv",sep = ""))

###### Step 5 AFR - Folds 5 & 6 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links5), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links5[splitting_urls == 1]
list_urls2 <- list_links5[splitting_urls == 2]
list_urls3 <- list_links5[splitting_urls == 3]
list_urls4 <- list_links5[splitting_urls == 4]
list_urls5 <- list_links5[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links6), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links6[splitting_urls == 1]
list_urls7 <- list_links6[splitting_urls == 2]
list_urls8 <- list_links6[splitting_urls == 3]
list_urls9 <- list_links6[splitting_urls == 4]
list_urls0 <- list_links6[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links5) %>%
  rbind(tibble(web_url = list_links6)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAFR_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAFR_",timestamp,".csv",sep = ""))

###### Step 6 AFR - Folds 7 & 8 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links7), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links7[splitting_urls == 1]
list_urls2 <- list_links7[splitting_urls == 2]
list_urls3 <- list_links7[splitting_urls == 3]
list_urls4 <- list_links7[splitting_urls == 4]
list_urls5 <- list_links7[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links8), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links8[splitting_urls == 1]
list_urls7 <- list_links8[splitting_urls == 2]
list_urls8 <- list_links8[splitting_urls == 3]
list_urls9 <- list_links8[splitting_urls == 4]
list_urls0 <- list_links8[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links7) %>%
  rbind(tibble(web_url = list_links8)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAFR_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAFR_",timestamp,".csv",sep = ""))

###### Step 7 AFR - Folds 9 & 10 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links9), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links9[splitting_urls == 1]
list_urls2 <- list_links9[splitting_urls == 2]
list_urls3 <- list_links9[splitting_urls == 3]
list_urls4 <- list_links9[splitting_urls == 4]
list_urls5 <- list_links9[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links0), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links0[splitting_urls == 1]
list_urls7 <- list_links0[splitting_urls == 2]
list_urls8 <- list_links0[splitting_urls == 3]
list_urls9 <- list_links0[splitting_urls == 4]
list_urls0 <- list_links0[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links9) %>%
  rbind(tibble(web_url = list_links0)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAFR_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAFR_",timestamp,".csv",sep = ""))




###### Step 8a AFR - Retry failed links (round 1 - extra time out) ######

missing_df <- list.files(pattern = "missingtextAFR_", path = "data_files/") %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble()
  }) %>%
  bind_rows() %>%
  filter(web_link != "web_url")

missing_df %<>%
  mutate(flag = str_detect(web_link, "allafrica")) %>%
  filter(flag != TRUE) %>%
  select(-flag)

split_links <- sample(1:10, 
                      size = length(missing_df$web_link), 
                      replace = TRUE, 
                      prob = c(0.1,0.1,0.1,0.1,0.1,
                               0.1,0.1,0.1,0.1,0.1))

list_urls1 <- missing_df$web_link[split_links == 1]
list_urls2 <- missing_df$web_link[split_links == 2]
list_urls3 <- missing_df$web_link[split_links == 3]
list_urls4 <- missing_df$web_link[split_links == 4]
list_urls5 <- missing_df$web_link[split_links == 5]
list_urls6 <- missing_df$web_link[split_links == 6]
list_urls7 <- missing_df$web_link[split_links == 7]
list_urls8 <- missing_df$web_link[split_links == 8]
list_urls9 <- missing_df$web_link[split_links == 9]
list_urls0 <- missing_df$web_link[split_links == 10]

get_text_from_py <- function(url_link){
  
  withTimeout({
    traf <- import("trafilatura")
    requests <- import("requests")
    rand <- import("requests_random_user_agent")
    goose <- import("goose3")
    
    r <- traf$fetch_url(url_link)
    web_text <- traf$extract(r)
    
    web_date <- traf$metadata$extract_metadata(r) %>%
      str_extract("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    
    g = goose$Goose({browser_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2)'})
    
    article <- g$extract(url = url_link)
    web_text2 <- article$cleaned_text
    web_date2 <- as.character(article$publish_datetime_utc)
    
    if (is.null(web_date2) == TRUE){
      web_date2 <- ""
    }
    
    if (is.null(web_text2) == TRUE){
      web_text2 <- ""
    }
    
    g$close()
    
    if (length(web_text) == 0 & length(web_text2) == 0) {
      
      source_python("source_files/python_justext.py")
      web_text <- (py_capture_output(get_web_texts(url_link)))
      
      web_date <- as.character(Sys.Date())
      web_date2 <- as.character(Sys.time())
    }
    
    if (is.null(web_text) == TRUE){
      web_text <- ""
    }
    
    if (length(web_date) == 0){
      web_date <- ""
    }
    
    if (length(web_date2) == 0){
      web_date2 <- ""
    }
    
    tibble(web_url = url_link,
           web_date = web_date,
           web_date2 = web_date2,
           web_sysdate = Sys.Date(),
           web_text = web_text,
           web_text2 = web_text2)
  }, timeout = 30, onTimeout = "silent")
  
}
get_text_from_py <- safely(get_text_from_py)

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = missing_df$web_link) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAFR_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAFR_",timestamp,".csv",sep = ""))



###### Step 1 AFR FR - Get list of links from file to download ######
df <- fread("data_files/COVID_GDELT_AFROnly_FR.csv")
df %<>%
  distinct(df$web_url, .keep_all = TRUE)

###### Step 2 AFR FR - Split list of links into 10 parts ######
split_links <- sample(1:10, 
                      size = length(df$web_url), 
                      replace = TRUE, 
                      prob = c(0.1,0.1,0.1,0.1,0.1,
                               0.1,0.1,0.1,0.1,0.1))

list_links1 <- df$web_url[split_links == 1]
list_links2 <- df$web_url[split_links == 2]
list_links3 <- df$web_url[split_links == 3]
list_links4 <- df$web_url[split_links == 4]
list_links5 <- df$web_url[split_links == 5]
list_links6 <- df$web_url[split_links == 6]
list_links7 <- df$web_url[split_links == 7]
list_links8 <- df$web_url[split_links == 8]
list_links9 <- df$web_url[split_links == 9]
list_links0 <- df$web_url[split_links == 10]

###### Step 3 AFR FR - Folds 1 & 2 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links1), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links1[splitting_urls == 1]
list_urls2 <- list_links1[splitting_urls == 2]
list_urls3 <- list_links1[splitting_urls == 3]
list_urls4 <- list_links1[splitting_urls == 4]
list_urls5 <- list_links1[splitting_urls == 5]

setwd(dir = "[...]") # To be set up by the user based on location of Python environment setup

get_text_from_py <- function(url_link){
  
  withTimeout({
    traf <- import("trafilatura")
    requests <- import("requests")
    rand <- import("requests_random_user_agent")
    goose <- import("goose3")
    
    r <- traf$fetch_url(url_link)
    web_text <- traf$extract(r)
    
    web_date <- traf$metadata$extract_metadata(r) %>%
      str_extract("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    
    g = goose$Goose({browser_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2)'})
    
    article <- g$extract(url = url_link)
    web_text2 <- article$cleaned_text
    web_date2 <- as.character(article$publish_datetime_utc)
    
    if (is.null(web_date2) == TRUE){
      web_date2 <- ""
    }
    
    if (is.null(web_text2) == TRUE){
      web_text2 <- ""
    }
    
    g$close()
    
    if (length(web_text) == 0 & length(web_text2) == 0) {
      
      source_python("source_files/python_justext.py")
      web_text <- (py_capture_output(get_web_texts(url_link)))
      
      web_date <- as.character(Sys.Date())
      web_date2 <- as.character(Sys.time())
    }
    
    if (is.null(web_text) == TRUE){
      web_text <- ""
    }
    
    if (length(web_date) == 0){
      web_date <- ""
    }
    
    if (length(web_date2) == 0){
      web_date2 <- ""
    }
    
    tibble(web_url = url_link,
           web_date = web_date,
           web_date2 = web_date2,
           web_sysdate = Sys.Date(),
           web_text = web_text,
           web_text2 = web_text2)
  }, timeout = 10, onTimeout = "silent")
  
}
get_text_from_py <- safely(get_text_from_py)

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links2), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links2[splitting_urls == 1]
list_urls7 <- list_links2[splitting_urls == 2]
list_urls8 <- list_links2[splitting_urls == 3]
list_urls9 <- list_links2[splitting_urls == 4]
list_urls0 <- list_links2[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links1) %>%
  rbind(tibble(web_url = list_links2)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAFR_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAFR_",timestamp,".csv",sep = ""))

rm(list_links1,list_links2,splitting_urls,
   scraped_text_safe1,scraped_text_safe2, scraped_text_safe3, scraped_text_safe4, scraped_text_safe5,
   scraped_text_safe6, scraped_text_safe7,scraped_text_safe8,scraped_text_safe9,scraped_text_safe0,
   list_urls1,list_urls2,list_urls3,list_urls4,list_urls5,list_urls6,list_urls7,list_urls8,list_urls9,list_urls0,
   scraped_text_error1,scraped_text_error2,scraped_text_error3,scraped_text_error4,scraped_text_error5,
   scraped_text_error6,scraped_text_error7,scraped_text_error8,scraped_text_error9,scraped_text_error0,
   scraped_text_missing,scraped_text_safe)

###### Step 4 AFR FR - Folds 3 & 4 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links3), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links3[splitting_urls == 1]
list_urls2 <- list_links3[splitting_urls == 2]
list_urls3 <- list_links3[splitting_urls == 3]
list_urls4 <- list_links3[splitting_urls == 4]
list_urls5 <- list_links3[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links4), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links4[splitting_urls == 1]
list_urls7 <- list_links4[splitting_urls == 2]
list_urls8 <- list_links4[splitting_urls == 3]
list_urls9 <- list_links4[splitting_urls == 4]
list_urls0 <- list_links4[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links3) %>%
  rbind(tibble(web_url = list_links4)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAFR_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAFR_",timestamp,".csv",sep = ""))

rm(list_links3,list_links4,splitting_urls,
   scraped_text_safe1,scraped_text_safe2, scraped_text_safe3, scraped_text_safe4, scraped_text_safe5,
   scraped_text_safe6, scraped_text_safe7,scraped_text_safe8,scraped_text_safe9,scraped_text_safe0,
   list_urls1,list_urls2,list_urls3,list_urls4,list_urls5,list_urls6,list_urls7,list_urls8,list_urls9,list_urls0,
   scraped_text_error1,scraped_text_error2,scraped_text_error3,scraped_text_error4,scraped_text_error5,
   scraped_text_error6,scraped_text_error7,scraped_text_error8,scraped_text_error9,scraped_text_error0,
   scraped_text_missing,scraped_text_safe)
###### Step 5 AFR FR - Folds 5 & 6 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links5), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links5[splitting_urls == 1]
list_urls2 <- list_links5[splitting_urls == 2]
list_urls3 <- list_links5[splitting_urls == 3]
list_urls4 <- list_links5[splitting_urls == 4]
list_urls5 <- list_links5[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links6), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links6[splitting_urls == 1]
list_urls7 <- list_links6[splitting_urls == 2]
list_urls8 <- list_links6[splitting_urls == 3]
list_urls9 <- list_links6[splitting_urls == 4]
list_urls0 <- list_links6[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links5) %>%
  rbind(tibble(web_url = list_links6)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAFR_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAFR_",timestamp,".csv",sep = ""))

rm(list_links5,list_links6,splitting_urls,
   scraped_text_safe1,scraped_text_safe2, scraped_text_safe3, scraped_text_safe4, scraped_text_safe5,
   scraped_text_safe6, scraped_text_safe7,scraped_text_safe8,scraped_text_safe9,scraped_text_safe0,
   list_urls1,list_urls2,list_urls3,list_urls4,list_urls5,list_urls6,list_urls7,list_urls8,list_urls9,list_urls0,
   scraped_text_error1,scraped_text_error2,scraped_text_error3,scraped_text_error4,scraped_text_error5,
   scraped_text_error6,scraped_text_error7,scraped_text_error8,scraped_text_error9,scraped_text_error0,
   scraped_text_missing,scraped_text_safe)
###### Step 6 AFR FR - Folds 7 & 8 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links7), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links7[splitting_urls == 1]
list_urls2 <- list_links7[splitting_urls == 2]
list_urls3 <- list_links7[splitting_urls == 3]
list_urls4 <- list_links7[splitting_urls == 4]
list_urls5 <- list_links7[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links8), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links8[splitting_urls == 1]
list_urls7 <- list_links8[splitting_urls == 2]
list_urls8 <- list_links8[splitting_urls == 3]
list_urls9 <- list_links8[splitting_urls == 4]
list_urls0 <- list_links8[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links7) %>%
  rbind(tibble(web_url = list_links8)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAFR_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAFR_",timestamp,".csv",sep = ""))

rm(list_links7,list_links8,splitting_urls,
   scraped_text_safe1,scraped_text_safe2, scraped_text_safe3, scraped_text_safe4, scraped_text_safe5,
   scraped_text_safe6, scraped_text_safe7,scraped_text_safe8,scraped_text_safe9,scraped_text_safe0,
   list_urls1,list_urls2,list_urls3,list_urls4,list_urls5,list_urls6,list_urls7,list_urls8,list_urls9,list_urls0,
   scraped_text_error1,scraped_text_error2,scraped_text_error3,scraped_text_error4,scraped_text_error5,
   scraped_text_error6,scraped_text_error7,scraped_text_error8,scraped_text_error9,scraped_text_error0,
   scraped_text_missing,scraped_text_safe)

###### Step 7 AFR FR - Folds 9 & 10 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links9), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links9[splitting_urls == 1]
list_urls2 <- list_links9[splitting_urls == 2]
list_urls3 <- list_links9[splitting_urls == 3]
list_urls4 <- list_links9[splitting_urls == 4]
list_urls5 <- list_links9[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links0), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links0[splitting_urls == 1]
list_urls7 <- list_links0[splitting_urls == 2]
list_urls8 <- list_links0[splitting_urls == 3]
list_urls9 <- list_links0[splitting_urls == 4]
list_urls0 <- list_links0[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links9) %>%
  rbind(tibble(web_url = list_links0)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAFR_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAFR_",timestamp,".csv",sep = ""))

rm(list_links9,list_links0,splitting_urls,
   scraped_text_safe1,scraped_text_safe2, scraped_text_safe3, scraped_text_safe4, scraped_text_safe5,
   scraped_text_safe6, scraped_text_safe7,scraped_text_safe8,scraped_text_safe9,scraped_text_safe0,
   list_urls1,list_urls2,list_urls3,list_urls4,list_urls5,list_urls6,list_urls7,list_urls8,list_urls9,list_urls0,
   scraped_text_error1,scraped_text_error2,scraped_text_error3,scraped_text_error4,scraped_text_error5,
   scraped_text_error6,scraped_text_error7,scraped_text_error8,scraped_text_error9,scraped_text_error0,
   scraped_text_missing,scraped_text_safe)


###### Final Step - Merge datasets ######
setwd("data_files/")

scraped_gdelt_data <- list.files("data_files/", pattern = "fulltext") %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble() %>%
      mutate(source = x,
             source = str_replace(source, "fulltext", ""),
             source = str_replace(source, "_[0-9]{13,15}(.csv)", ""))
  }) %>%
  bind_rows()

AFR_gdelt <- fread("COVID_GDELT_AFROnly.csv") # N = 233529
CN_gdelt <- fread("COVID_GDELT_CNOnly.csv") # N = 64754
WE_gdelt <- fread("COVID_GDELT_WestOnly.csv") # N = 204820

scraped_gdelt_data %<>%
  filter(source == "AFR")

###### Adding data labeled as "China" in V2Locations for paper revision based on reviewers' suggestions ######

# Generate list of existing URLs in the DB
setwd("[...]") #To be defined by user  
covid_gdelt_files_existing <- list.files(pattern = "COVID_GDELT_", 
                                         path = "data_files/") %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble()
  }) %>%
  bind_rows()

covid_gdelt_files_existing %<>%
  select(web_url)

# Get a manually-generated list of African sources
african_sources <- read.xlsx(xlsxFile = "source_files/AFR_media_sources.xlsx", sheet = "en") %>%
  mutate(country_iso = replace_na(country_iso, "NB")) %>% 
  mutate(medium_name = str_replace_all(medium_name, "http://www.|https://www.|http://|https://", "")) %>%
  distinct(medium_name,.keep_all = TRUE) %>%
  select(country_name,
         country_iso,
         medium_name)

african_sourcesFR <- read.xlsx(xlsxFile = "source_files/AFR_media_sources.xlsx", sheet = "fr") %>%
  mutate(medium_name = str_replace_all(medium_name, "http://www.|https://www.|http://|https://", "")) %>%
  distinct(medium_name,.keep_all = TRUE) %>%
  select(country_name,
         country_iso,
         medium_name)

# Read raw GDELT data & remove existing URLs
covid_gdelt_files_china <- fread("source_files/GDELT_Additional_China.csv")
covid_gdelt_files_china %<>%
  select(web_url = DocumentIdentifier,
         date_sources = GKGRECORDID,
         medium_name = SourceCommonName) %>%
  anti_join(covid_gdelt_files_existing)
rm(covid_gdelt_files_existing)

# Save African links in GDELT EN
covid_gdelt_files_AFR <- covid_gdelt_files_china %>%
  right_join(african_sources)
fwrite(covid_gdelt_files_AFR, "data_files/COVID_GDELT_AboutChina_AFR_EN.csv")

# Save African links in GDELT FR
covid_gdelt_files_AFR <- covid_gdelt_files_china %>%
  right_join(african_sourcesFR)
fwrite(covid_gdelt_files_AFR, "data_files/COVID_GDELT_AboutChina_AFR_FR.csv")

rm(covid_gdelt_files_AFR,
   african_sourcesFR,
   african_sources)

# Save Chinese links in GDELT
covid_gdelt_files_CN <- covid_gdelt_files_china %>%
  right_join(tibble(medium_name = c("chinadaily.com.cn", 
                                    "globaltimes.cn", 
                                    "xinhuanet.com", 
                                    "chinaview.cn", 
                                    "news.cn",
                                    "cgtn.com",
                                    "people.cn")))
fwrite(covid_gdelt_files_CN, "data_files/COVID_GDELT_AboutChina_CN.csv")

# Save Reuters + AFP + BBC + CNN links in GDELT
covid_gdelt_files_West <- covid_gdelt_files_china %>%
  right_join(tibble(medium_name = c("reuters.com", 
                                    "bbc.com", 
                                    "bbc.co.uk", 
                                    "afp.com", 
                                    "cnn.com",
                                    "apnews.com")))
fwrite(covid_gdelt_files_West, "data_files/COVID_GDELT_AboutChina_West.csv")

rm(covid_gdelt_files_West,
   covid_gdelt_files_CN,
   covid_gdelt_files_china)

###### Step 1 Extra - Get list of links from file to download ######
df <- list.files(pattern = "data_files/COVID_GDELT_AboutChina_", path = getwd()) %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble()
  }) %>%
  bind_rows()

df %<>%
  distinct(df$web_url, .keep_all = TRUE)

###### Step 2 Extra - Split list of links into 10 parts ######
split_links <- sample(1:10, 
                      size = length(df$web_url), 
                      replace = TRUE, 
                      prob = c(0.1,0.1,0.1,0.1,0.1,
                               0.1,0.1,0.1,0.1,0.1))

list_links1 <- df$web_url[split_links == 1]
list_links2 <- df$web_url[split_links == 2]
list_links3 <- df$web_url[split_links == 3]
list_links4 <- df$web_url[split_links == 4]
list_links5 <- df$web_url[split_links == 5]
list_links6 <- df$web_url[split_links == 6]
list_links7 <- df$web_url[split_links == 7]
list_links8 <- df$web_url[split_links == 8]
list_links9 <- df$web_url[split_links == 9]
list_links0 <- df$web_url[split_links == 10]

###### Step 3 Extra - Folds 1 & 2 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links1), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links1[splitting_urls == 1]
list_urls2 <- list_links1[splitting_urls == 2]
list_urls3 <- list_links1[splitting_urls == 3]
list_urls4 <- list_links1[splitting_urls == 4]
list_urls5 <- list_links1[splitting_urls == 5]

setwd(dir = "[...]") # To be set up by the user based on location of Python environment setup

get_text_from_py <- function(url_link){
  
  withTimeout({
    traf <- import("trafilatura")
    requests <- import("requests")
    rand <- import("requests_random_user_agent")
    goose <- import("goose3")
    
    r <- traf$fetch_url(url_link)
    web_text <- traf$extract(r)
    
    web_date <- traf$metadata$extract_metadata(r) %>%
      str_extract("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    
    g = goose$Goose({browser_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2)'})
    
    article <- g$extract(url = url_link)
    web_text2 <- article$cleaned_text
    web_date2 <- as.character(article$publish_datetime_utc)
    
    if (is.null(web_date2) == TRUE){
      web_date2 <- ""
    }
    
    if (is.null(web_text2) == TRUE){
      web_text2 <- ""
    }
    
    g$close()
    
    if (length(web_text) == 0 & length(web_text2) == 0) {
      
      source_python("source_files/python_justext.py")
      web_text <- (py_capture_output(get_web_texts(url_link)))
      
      web_date <- as.character(Sys.Date())
      web_date2 <- as.character(Sys.time())
    }
    
    if (is.null(web_text) == TRUE){
      web_text <- ""
    }
    
    if (length(web_date) == 0){
      web_date <- ""
    }
    
    if (length(web_date2) == 0){
      web_date2 <- ""
    }
    
    tibble(web_url = url_link,
           web_date = web_date,
           web_date2 = web_date2,
           web_sysdate = Sys.Date(),
           web_text = web_text,
           web_text2 = web_text2)
  }, timeout = 10, onTimeout = "silent")
  
}
get_text_from_py <- safely(get_text_from_py)

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links2), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links2[splitting_urls == 1]
list_urls7 <- list_links2[splitting_urls == 2]
list_urls8 <- list_links2[splitting_urls == 3]
list_urls9 <- list_links2[splitting_urls == 4]
list_urls0 <- list_links2[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links1) %>%
  rbind(tibble(web_url = list_links2)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAdditional_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAdditional_",timestamp,".csv",sep = ""))

rm(scraped_text_safe, scraped_text_safe1,scraped_text_safe2,scraped_text_safe3,
   scraped_text_safe4, scraped_text_safe5, scraped_text_safe6, scraped_text_safe7,
   scraped_text_safe8, scraped_text_safe9, scraped_text_safe0, scraped_text_missing,
   scraped_text_error0, scraped_text_error1, scraped_text_error2, scraped_text_error3,
   scraped_text_error4, scraped_text_error5, scraped_text_error6, scraped_text_error7,
   scraped_text_error8, scraped_text_error9, timestamp,
   list_urls0, list_urls1, list_urls2, list_urls3, list_urls4, list_urls5,
   list_urls6, list_urls7, list_urls8, list_urls9, splitting_urls,
   list_links1, list_links2)

###### Step 4 Extra - Folds 3 & 4 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links3), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links3[splitting_urls == 1]
list_urls2 <- list_links3[splitting_urls == 2]
list_urls3 <- list_links3[splitting_urls == 3]
list_urls4 <- list_links3[splitting_urls == 4]
list_urls5 <- list_links3[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links4), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links4[splitting_urls == 1]
list_urls7 <- list_links4[splitting_urls == 2]
list_urls8 <- list_links4[splitting_urls == 3]
list_urls9 <- list_links4[splitting_urls == 4]
list_urls0 <- list_links4[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links3) %>%
  rbind(tibble(web_url = list_links4)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAdditional_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAdditional_",timestamp,".csv",sep = ""))

rm(scraped_text_safe, scraped_text_safe1,scraped_text_safe2,scraped_text_safe3,
   scraped_text_safe4, scraped_text_safe5, scraped_text_safe6, scraped_text_safe7,
   scraped_text_safe8, scraped_text_safe9, scraped_text_safe0, scraped_text_missing,
   scraped_text_error0, scraped_text_error1, scraped_text_error2, scraped_text_error3,
   scraped_text_error4, scraped_text_error5, scraped_text_error6, scraped_text_error7,
   scraped_text_error8, scraped_text_error9, timestamp,
   list_urls0, list_urls1, list_urls2, list_urls3, list_urls4, list_urls5,
   list_urls6, list_urls7, list_urls8, list_urls9, splitting_urls,
   list_links3, list_links4)

###### Step 5 Extra - Folds 5 & 6 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links5), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links5[splitting_urls == 1]
list_urls2 <- list_links5[splitting_urls == 2]
list_urls3 <- list_links5[splitting_urls == 3]
list_urls4 <- list_links5[splitting_urls == 4]
list_urls5 <- list_links5[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links6), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links6[splitting_urls == 1]
list_urls7 <- list_links6[splitting_urls == 2]
list_urls8 <- list_links6[splitting_urls == 3]
list_urls9 <- list_links6[splitting_urls == 4]
list_urls0 <- list_links6[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links5) %>%
  rbind(tibble(web_url = list_links6)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAdditional_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAdditional_",timestamp,".csv",sep = ""))

rm(scraped_text_safe, scraped_text_safe1,scraped_text_safe2,scraped_text_safe3,
   scraped_text_safe4, scraped_text_safe5, scraped_text_safe6, scraped_text_safe7,
   scraped_text_safe8, scraped_text_safe9, scraped_text_safe0, scraped_text_missing,
   scraped_text_error0, scraped_text_error1, scraped_text_error2, scraped_text_error3,
   scraped_text_error4, scraped_text_error5, scraped_text_error6, scraped_text_error7,
   scraped_text_error8, scraped_text_error9, timestamp,
   list_urls0, list_urls1, list_urls2, list_urls3, list_urls4, list_urls5,
   list_urls6, list_urls7, list_urls8, list_urls9, splitting_urls)

setwd(dir = "[...]")
Sys.setenv(RETICULATE_PYTHON = "[...]python/bin/python")
reticulate::py_config()
plan(multisession(workers = 7))

###### Step 6 Extra - Folds 7 & 8 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links7), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links7[splitting_urls == 1]
list_urls2 <- list_links7[splitting_urls == 2]
list_urls3 <- list_links7[splitting_urls == 3]
list_urls4 <- list_links7[splitting_urls == 4]
list_urls5 <- list_links7[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links8), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links8[splitting_urls == 1]
list_urls7 <- list_links8[splitting_urls == 2]
list_urls8 <- list_links8[splitting_urls == 3]
list_urls9 <- list_links8[splitting_urls == 4]
list_urls0 <- list_links8[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links7) %>%
  rbind(tibble(web_url = list_links8)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAdditional_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAdditional_",timestamp,".csv",sep = ""))

rm(scraped_text_safe, scraped_text_safe1,scraped_text_safe2,scraped_text_safe3,
   scraped_text_safe4, scraped_text_safe5, scraped_text_safe6, scraped_text_safe7,
   scraped_text_safe8, scraped_text_safe9, scraped_text_safe0, scraped_text_missing,
   scraped_text_error0, scraped_text_error1, scraped_text_error2, scraped_text_error3,
   scraped_text_error4, scraped_text_error5, scraped_text_error6, scraped_text_error7,
   scraped_text_error8, scraped_text_error9, timestamp,
   list_urls0, list_urls1, list_urls2, list_urls3, list_urls4, list_urls5,
   list_urls6, list_urls7, list_urls8, list_urls9, splitting_urls)

setwd(dir = "[...]")
Sys.setenv(RETICULATE_PYTHON = "[...]python/bin/python")
reticulate::py_config()
plan(multisession(workers = 7))

###### Step 7 Extra - Folds 9 & 10 of 10 of links to retrieve  ######

splitting_urls <- sample(1:5, 
                         size = length(list_links9), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls1 <- list_links9[splitting_urls == 1]
list_urls2 <- list_links9[splitting_urls == 2]
list_urls3 <- list_links9[splitting_urls == 3]
list_urls4 <- list_links9[splitting_urls == 4]
list_urls5 <- list_links9[splitting_urls == 5]

tic()
scraped_text_safe1 <- future_map(list_urls1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_urls2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_urls3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_urls4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_urls5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

splitting_urls <- sample(1:5, 
                         size = length(list_links0), 
                         replace = TRUE, 
                         prob = c(0.2,0.2,0.2,0.2,0.2))

list_urls6 <- list_links0[splitting_urls == 1]
list_urls7 <- list_links0[splitting_urls == 2]
list_urls8 <- list_links0[splitting_urls == 3]
list_urls9 <- list_links0[splitting_urls == 4]
list_urls0 <- list_links0[splitting_urls == 5]

tic()
scraped_text_safe6 <- future_map(list_urls6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_urls7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_urls8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_urls9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_urls0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = list_links9) %>%
  rbind(tibble(web_url = list_links0)) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAdditional_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAdditional_",timestamp,".csv",sep = ""))

rm(scraped_text_safe, scraped_text_safe1,scraped_text_safe2,scraped_text_safe3,
   scraped_text_safe4, scraped_text_safe5, scraped_text_safe6, scraped_text_safe7,
   scraped_text_safe8, scraped_text_safe9, scraped_text_safe0, scraped_text_missing,
   scraped_text_error0, scraped_text_error1, scraped_text_error2, scraped_text_error3,
   scraped_text_error4, scraped_text_error5, scraped_text_error6, scraped_text_error7,
   scraped_text_error8, scraped_text_error9, timestamp,
   list_urls0, list_urls1, list_urls2, list_urls3, list_urls4, list_urls5,
   list_urls6, list_urls7, list_urls8, list_urls9, splitting_urls)

Sys.setenv(RETICULATE_PYTHON = "[...]python/bin/python")
reticulate::py_config()
plan(multisession(workers = 7))

rm(list_links0, list_links5, list_links6, list_links7, list_links8, list_links9)

###### Step 8a Extra - Retry failed links (round 1) ######

missing_df <- list.files(pattern = "missingtextAFR_", path = "data_files/") %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble()
  }) %>%
  bind_rows() %>%
  filter(web_link != "web_url")

missing_df %<>%
  mutate(web_link = str_replace(web_link, ".cn/www.chinadaily.com.", ""))

missing_df %<>%
  mutate(web_link = str_replace(web_link, "www.chinadaily.com.cn/global.chinadaily.com.cn", "global.chinadaily.com.cn"))

get_text_from_py <- function(url_link){ # Extra timeout time
  
  withTimeout({
    traf <- import("trafilatura")
    requests <- import("requests")
    rand <- import("requests_random_user_agent")
    goose <- import("goose3")
    
    r <- traf$fetch_url(url_link)
    web_text <- traf$extract(r)
    
    web_date <- traf$metadata$extract_metadata(r) %>%
      str_extract("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    
    g = goose$Goose({browser_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2)'})
    
    article <- g$extract(url = url_link)
    web_text2 <- article$cleaned_text
    web_date2 <- as.character(article$publish_datetime_utc)
    
    if (is.null(web_date2) == TRUE){
      web_date2 <- ""
    }
    
    if (is.null(web_text2) == TRUE){
      web_text2 <- ""
    }
    
    g$close()
    
    if (length(web_text) == 0 & length(web_text2) == 0) {
      
      source_python("source_files/python_justext.py")
      web_text <- (py_capture_output(get_web_texts(url_link)))
      
      web_date <- as.character(Sys.Date())
      web_date2 <- as.character(Sys.time())
    }
    
    if (is.null(web_text) == TRUE){
      web_text <- ""
    }
    
    if (length(web_date) == 0){
      web_date <- ""
    }
    
    if (length(web_date2) == 0){
      web_date2 <- ""
    }
    
    tibble(web_url = url_link,
           web_date = web_date,
           web_date2 = web_date2,
           web_sysdate = Sys.Date(),
           web_text = web_text,
           web_text2 = web_text2)
  }, timeout = 30, onTimeout = "silent")
  
}
get_text_from_py <- safely(get_text_from_py)

split_links <- sample(1:10, 
                      size = length(missing_df$web_link), 
                      replace = TRUE, 
                      prob = c(0.1,0.1,0.1,0.1,0.1,
                               0.1,0.1,0.1,0.1,0.1))

list_links1 <- missing_df$web_link[split_links == 1]
list_links2 <- missing_df$web_link[split_links == 2]
list_links3 <- missing_df$web_link[split_links == 3]
list_links4 <- missing_df$web_link[split_links == 4]
list_links5 <- missing_df$web_link[split_links == 5]
list_links6 <- missing_df$web_link[split_links == 6]
list_links7 <- missing_df$web_link[split_links == 7]
list_links8 <- missing_df$web_link[split_links == 8]
list_links9 <- missing_df$web_link[split_links == 9]
list_links0 <- missing_df$web_link[split_links == 10]

tic()
scraped_text_safe1 <- future_map(list_links1, get_text_from_py, .progress = TRUE)
scraped_text_error1 <- map(scraped_text_safe1, "error")
scraped_text_safe1 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe2 <- future_map(list_links2, get_text_from_py, .progress = TRUE)
scraped_text_error2 <- map(scraped_text_safe2, "error")
scraped_text_safe2 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe3 <- future_map(list_links3, get_text_from_py, .progress = TRUE)
scraped_text_error3 <- map(scraped_text_safe3, "error")
scraped_text_safe3 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe4 <- future_map(list_links4, get_text_from_py, .progress = TRUE)
scraped_text_error4 <- map(scraped_text_safe4, "error")
scraped_text_safe4 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe5 <- future_map(list_links5, get_text_from_py, .progress = TRUE)
scraped_text_error5 <- map(scraped_text_safe5, "error")
scraped_text_safe5 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe6 <- future_map(list_links6, get_text_from_py, .progress = TRUE)
scraped_text_error6 <- map(scraped_text_safe6, "error")
scraped_text_safe6 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe7 <- future_map(list_links7, get_text_from_py, .progress = TRUE)
scraped_text_error7 <- map(scraped_text_safe7, "error")
scraped_text_safe7 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe8 <- future_map(list_links8, get_text_from_py, .progress = TRUE)
scraped_text_error8 <- map(scraped_text_safe8, "error")
scraped_text_safe8 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe9 <- future_map(list_links9, get_text_from_py, .progress = TRUE)
scraped_text_error9 <- map(scraped_text_safe9, "error")
scraped_text_safe9 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

tic()
scraped_text_safe0 <- future_map(list_links0, get_text_from_py, .progress = TRUE)
scraped_text_error0 <- map(scraped_text_safe0, "error")
scraped_text_safe0 %<>% 
  map("result") %>% 
  compact() %>% 
  discard(~nrow(.) == 0) %>%
  reduce(bind_rows)
toc()

scraped_text_safe <- rbind(scraped_text_safe1,
                           scraped_text_safe2,
                           scraped_text_safe3,
                           scraped_text_safe4,
                           scraped_text_safe5,
                           scraped_text_safe6,
                           scraped_text_safe7,
                           scraped_text_safe8,
                           scraped_text_safe9,
                           scraped_text_safe0)

scraped_text_missing <- tibble(web_url = missing_df$web_link) %>%
  anti_join(tibble(web_url = scraped_text_safe$web_url))

timestamp <- str_replace_all(Sys.time(), ":|-| ", "")
fwrite(scraped_text_safe, 
       paste0("data_files/fulltextAdditional_",timestamp,".csv",sep = ""))

fwrite(scraped_text_missing, 
       paste0("data_files/missingtextAdditional_",timestamp,".csv",sep = ""))

rm(scraped_text_safe, scraped_text_safe1,scraped_text_safe2,scraped_text_safe3,
   scraped_text_safe4, scraped_text_safe5, scraped_text_safe6, scraped_text_safe7,
   scraped_text_safe8, scraped_text_safe9, scraped_text_safe0, scraped_text_missing,
   scraped_text_error0, scraped_text_error1, scraped_text_error2, scraped_text_error3,
   scraped_text_error4, scraped_text_error5, scraped_text_error6, scraped_text_error7,
   scraped_text_error8, scraped_text_error9, timestamp,
   list_urls1, list_urls2, list_urls3, list_urls4, list_urls5,
   list_links0, list_links1, list_links2, list_links3, list_links4,
   list_links5, list_links6, list_links7, list_links8, list_links9, split_links, splitting_urls)


###### Export Additional Links ######
scraped_gdelt_data_add <- list.files("data_files/", pattern = "fulltextAdditional") %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble() %>%
      mutate(web_date = as.character(web_date),
             source = x,
             source = str_replace(source, "fulltext", ""),
             source = str_replace(source, "_[0-9]{13,15}(.csv)", ""))
  }) %>%
  bind_rows()

fwrite(scraped_gdelt_data_add, "COVID_GDELT_AboutChina.csv")

rm(get_text_from_py,
   list_proxies,
   list_urls1,
   missing_df,
   scraped_gdelt_data_add,
   scraped_text_error1, 
   scraped_text_safe1)
