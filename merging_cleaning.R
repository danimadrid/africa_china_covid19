library(pacman)
p_load(data.table, reader, tidyverse, magrittr, furrr, future, ggplot2, hrbrthemes,quanteda,openxlsx,textreuse, tm, NLP,readxl,zip,
       RNewsflow, lubridate)

###### Load Reuters Scraped Data ######
setwd("/Volumes/LaCieOrange/COVID19/GMAC/")

df_r <- fread("Reuters_Scrape.csv") %>%
  tibble()

df_r %<>%
  mutate(country_name = "United Kingdom",
         country_iso = "GB",
         language_gdelt = "EN",
         data_source = "SERPAPI",
         id = paste0("SERPAPI",seq(1,length(df_r$url)))) %>%
  select(title = web_headline,
         publish_date = web_date,
         source,
         country_name,
         country_iso,
         full_text = web_text,
         web_url = url,
         data_source,
         id,
         language_gdelt)

###### Merge all GDELT data EN ######

# Load CSV files manually scraped
scraped_gdelt_data <- list.files("/Volumes/LaCieOrange/COVID19/GMAC/", pattern = "fulltext") %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble() %>%
      mutate(web_date = as.Date(web_date),
             web_sysdate = as.Date(web_sysdate))}) %>%
  bind_rows()

# Load metadata for URLs from GDELT for screening and filtering
af_gdelt <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_AFROnly.csv")
cn_gdelt <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_CNOnly.csv") %>%
  tibble() %>%
  mutate(country_name = "China",
         country_iso = "CN")
we_gdelt <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_WestOnly.csv") %>%
  tibble %>%
  mutate(country_name = case_when(medium_name == "reuters.com" ~ "United Kingdom",
                                  medium_name == "apnews.com" ~ "United States",
                                  medium_name == "cnn.com" ~ "United States",
                                  medium_name == "bbc.com" ~ "United Kingdom",
                                  medium_name == "bbc.co.uk" ~ "United Kingdom",
                                  medium_name == "bbc.co.uk" ~ "France"),
         country_iso = case_when(medium_name == "reuters.com" ~ "GB",
                                 medium_name == "apnews.com" ~ "US",
                                 medium_name == "cnn.com" ~ "US",
                                 medium_name == "bbc.com" ~ "GB",
                                 medium_name == "bbc.co.uk" ~ "GB",
                                 medium_name == "bbc.co.uk" ~ "FR"))
full_gdelt <- rbind(af_gdelt,
                    cn_gdelt,
                    we_gdelt)

# Select the best fit for text from 2 scraped options
scraped_gdelt_data %<>%
  mutate(chars_web_text = nchar(web_text),
         chars_web_text2 = nchar(web_text2),
         chars_web_text = replace_na(chars_web_text, 0),
         chars_web_text2 = replace_na(chars_web_text2, 0)) %>%
  left_join(full_gdelt)
rm(full_gdelt,
   af_gdelt,
   cn_gdelt,
   we_gdelt)

# Load ALL GDELT links to extract date, and language
gdelt1 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_Apr1.csv")
gdelt2 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_Apr2.csv")
gdelt3 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_Mar1.csv")
gdelt4 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_Mar2.csv")
gdelt5 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_May1.csv")
gdelt6 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_May2.csv")
gdelt7 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_May3.csv")
gdelt8 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_DecJanFeb.csv")

gdelt <- rbind(gdelt1,gdelt2,gdelt3,gdelt4,
               gdelt5,gdelt6,gdelt7,gdelt8)
rm(gdelt1,gdelt2,gdelt3,gdelt4,
   gdelt5,gdelt6,gdelt7,gdelt8)
gdelt %<>% 
  select(web_url = DocumentIdentifier,
         web_gdeltdate = GKGRECORDID,
         web_language = TranslationInfo) %>%
  mutate(web_gdeltdate = str_extract(web_gdeltdate, "[0-9]{8}"),
         web_gdeltdate = ymd(web_gdeltdate))
gdelt %<>% # Remove GDELT duplicates
  distinct(web_url, .keep_all = TRUE)
scraped_gdelt_data %<>%
  left_join(gdelt)

# Split data into two, one df for each text column
df1 <- scraped_gdelt_data %>%
  filter(chars_web_text >= chars_web_text2) %>% # Keep only first text if longer
  select(web_url,
         web_text,
         web_date,
         web_date2,
         web_gdeltdate,
         country_name,
         country_iso,
         medium_name,
         web_language)
df2 <- scraped_gdelt_data %>%
  filter(chars_web_text < chars_web_text2) %>% # Keep only second text if longer
  select(web_url,
         web_text = web_text2,
         web_date,
         web_date2,
         web_gdeltdate,
         country_name,
         country_iso,
         medium_name,
         web_language)
df_g <- rbind(df1,df2)
rm(df1,df2,scraped_gdelt_data)

# Coalesce dates, keeping the best fit
df_g %<>%
  mutate(web_date = as.Date(web_date),
         web_date2 = as.Date(web_date2),
         web_gdeltdate = as.Date(web_gdeltdate),
         publish_date = coalesce(web_gdeltdate,web_date,web_date2))

# Rename, select variables, and format final version of corpus
df_g %<>%
  mutate(title= " ",
         data_source = "GDELT",
         id = paste0("GDELT",seq(1,length(df_g$web_url)))) %>%
  select(title,
         publish_date,
         source = medium_name,
         country_name,
         country_iso,
         full_text = web_text,
         web_url,
         data_source,
         id,
         language_gdelt = web_language) %>%
  filter(is.na(publish_date) != TRUE) %>%
  filter(full_text != "")

# Identify language of news items
list_languages <- df_g %>%
  group_by(language_gdelt) %>%
  count(language_gdelt)
# Keeps French and English only
df_g %<>%
  filter(language_gdelt == "" | language_gdelt == "srclc:fra;eng:Moses 2.1.1 / MosesCore Europarl fr-en / GT-FRA 1.0" | language_gdelt == "srclc:fra;eng:GT-FRA 1.0") %>%
  mutate(language_gdelt = case_when(language_gdelt == "" ~ "EN",
                                    TRUE ~ "FR"))

# Merge Reuters scraped data with GDELT
df_g <- rbind(df_g,df_r)
rm(df_r,gdelt,list_languages)

df_g %>%
  group_by(country_iso,source) %>%
  count(source) %>%
  write.xlsx("list_all_sources_gdelt_EN.xlsx")

###### Merge all Nexis ######
n1 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_AFP_EN.csv") %>% mutate(pub.date = as.Date(pub.date))
n2 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_AFR_EN1.csv") %>% mutate(pub.date = as.Date(pub.date))
n3 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_AFR_EN2.csv") %>% mutate(pub.date = as.Date(pub.date))
n4 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_CD_EN.csv") %>% mutate(pub.date = as.Date(pub.date))
n5 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_GT_EN.csv") %>% mutate(pub.date = as.Date(pub.date))
n6 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_PD_EN.csv") %>% mutate(pub.date = as.Date(pub.date))
n7 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_XH_EN.csv") %>% mutate(pub.date = as.Date(pub.date))
n8 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_AP_EN.csv") %>% mutate(pub.date = as.Date(pub.date))

n9 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_AFR_FR.csv") %>% mutate(pub.date = as.Date(pub.date))
n10 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_AFP_FR.csv") %>% mutate(pub.date = as.Date(pub.date))
n11 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_PD_FR.csv") %>% mutate(pub.date = as.Date(pub.date))
n12 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/Nexis/FullData_XH_FR.csv") %>% mutate(pub.date = as.Date(pub.date))

df_en <- rbind(n1,n2,n3,n4,n5,n6,n7,n8) %>%
  mutate(language_gdelt = "EN")
df_fr <- rbind(n9,n10,n11,n12) %>%
  mutate(language_gdelt = "FR")
df_n <- rbind(df_en,df_fr)
rm(n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12)

df_n %<>%
  mutate(country_name = " ",
         country_iso = " ",
         web_url = " ",
         data_source = "NEXIS",
         id = paste0("NEXIS",seq(1,length(df_n$geo)))) %>%
  select(title,
         publish_date = pub.date,
         source = medium,
         country_name,
         country_iso,
         full_text = text,
         web_url,
         data_source,
         id,
         language_gdelt)

rm(df_en,df_fr)
df_n %>%
  group_by(country_iso,source) %>%
  count(source) %>%
  write.xlsx("list_all_sources_nexis.xlsx")

###### Homogenize names of sources and countries ######
# Load list of processed sources and merge
df_sources_nexis <- read_excel("list_all_sources_nexis_processed.xlsx")
df_sources_gdelt <- read_excel("list_all_sources_gdelt_EN_processed.xlsx")

df_nexis <- df_n %>%
  full_join(df_sources_nexis, by = c("source")) %>%
  filter(is.keep == "T") %>%
  filter(new.source != "NA") %>%
  mutate(source = new.source,
         country_iso = new.country) %>%
  select(-new.source,
         -new.country,
         -is.keep,
         -country_name) %>%
  tibble()
rm(df_sources_nexis, df_n)

df_gdelt <- df_g %>%
  full_join(df_sources_gdelt, by = c("source", "country_iso")) %>%
  filter(new.source != "NA") %>%
  mutate(source = new.source,
         country_iso = new.country_iso) %>%
  select(-new.source,
         -new.country_iso,
         -country_name) %>%
  tibble()
rm(df_sources_gdelt, df_g)

df_en <- rbind(df_gdelt,df_nexis) %>%
  filter(language_gdelt == "EN")
df_fr <- rbind(df_gdelt,df_nexis) %>%
  filter(language_gdelt == "FR")
rm(df_gdelt, df_nexis)

###### Add GDELT FR ######

df_f1 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/fulltextAFR_20200829195038.csv") %>% 
  mutate(web_date = as.Date(web_date),
         web_date2 = str_squish(str_replace(web_date2, "[0-9]{2}:[0-9]{2}:[0-9]{2}","")),
         web_date2 = parse_date_time(web_date2, "%Y-%m-%d"))
df_f2 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/fulltextAFR_20200829214510.csv") %>% 
  mutate(web_date = as.Date(web_date),
         web_date2 = str_squish(str_replace(web_date2, "[0-9]{2}:[0-9]{2}:[0-9]{2}","")),
         web_date2 = parse_date_time(web_date2, "%Y-%m-%d"))
df_f3 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/fulltextAFR_20200829234306.csv") %>% 
  mutate(web_date = as.Date(web_date),
         web_date2 = str_squish(str_replace(web_date2, "[0-9]{2}:[0-9]{2}:[0-9]{2}","")),
         web_date2 = parse_date_time(web_date2, "%Y-%m-%d"))
df_f4 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/fulltextAFR_20200830013041.csv") %>% 
  mutate(web_date = as.Date(web_date),
         web_date2 = str_squish(str_replace(web_date2, "[0-9]{2}:[0-9]{2}:[0-9]{2}","")),
         web_date2 = parse_date_time(web_date2, "%Y-%m-%d"))
df_f5 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/fulltextAFR_20200830031615.csv") %>% 
  mutate(web_date = as.Date(web_date),
         web_date2 = str_squish(str_replace(web_date2, "[0-9]{2}:[0-9]{2}:[0-9]{2}","")),
         web_date2 = parse_date_time(web_date2, "%Y-%m-%d"))
df_f6 <- fread("/Volumes/LaCieOrange/COVID19/GMAC/fulltextAFR_20200830122356.csv") %>% 
  mutate(web_date = as.Date(web_date),
         web_date2 = str_squish(str_replace(web_date2, "[0-9]{2}:[0-9]{2}:[0-9]{2}","")),
         web_date2 = parse_date_time(web_date2, "%Y-%m-%d"))

scraped_gdelt_data <- rbind(df_f1,df_f2,df_f3,df_f4,df_f5,df_f6)
rm(df_f1,df_f2,df_f3,df_f4,df_f5,df_f6)

# Load metadata for URLs from GDELT for screening and filtering
fr_gdelt <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_AFROnly_FR.csv")

# Select the best fit for text from 2 scraped options
scraped_gdelt_data %<>%
  mutate(chars_web_text = nchar(web_text),
         chars_web_text2 = nchar(web_text2),
         chars_web_text = replace_na(chars_web_text, 0),
         chars_web_text2 = replace_na(chars_web_text2, 0)) %>%
  left_join(fr_gdelt, by =  "web_url") %>%
  mutate(web_gdeltdate = str_extract(date_sources, "[0-9]{8}"),
         web_gdeltdate = ymd(web_gdeltdate),
         web_language = "FR")
rm(fr_gdelt)

# Split data into two, one df for each text column
df1 <- scraped_gdelt_data %>%
  filter(chars_web_text >= chars_web_text2) %>% # Keep only first text if longer
  select(web_url,
         web_text,
         web_date,
         web_date2,
         web_gdeltdate,
         country_name,
         country_iso,
         medium_name,
         web_language)
df2 <- scraped_gdelt_data %>%
  filter(chars_web_text < chars_web_text2) %>% # Keep only second text if longer
  select(web_url,
         web_text = web_text2,
         web_date,
         web_date2,
         web_gdeltdate,
         country_name,
         country_iso,
         medium_name,
         web_language)
df_g <- rbind(df1,df2)
rm(df1,df2,scraped_gdelt_data)

# Coalesce dates, keeping the best fit
df_g %<>%
  mutate(web_date = as.Date(web_date),
         web_date2 = as.Date(web_date2),
         web_gdeltdate = as.Date(web_gdeltdate),
         publish_date = coalesce(web_gdeltdate,web_date,web_date2))

# Rename, select variables, and format final version of corpus
df_g %<>%
  mutate(title= " ",
         data_source = "GDELT",
         id = paste0("GDELT",seq(102025,(102024+length(df_g$web_text))))) %>%
  select(title,
         publish_date,
         source = medium_name,
         country_iso,
         full_text = web_text,
         web_url,
         data_source,
         id,
         language_gdelt = web_language) %>%
  filter(is.na(publish_date) != TRUE) %>%
  filter(full_text != "")

df_fr <- rbind(df_fr, df_g)
rm(df_g)

# Export list of sources FR for homogenizing names
df_fr %>%
  group_by(country_iso,source) %>%
  count(source) %>%
  write.xlsx("list_all_sources_FR.xlsx")

df_sources_fr <- read_excel("list_all_sources_FR_processed.xlsx")

df_fr %<>%
  full_join(df_sources_fr, by = c("source", "country_iso")) %>%
  filter(new_source != "NA") %>%
  mutate(source = new_source,
         country_iso = new.country_iso) %>%
  select(-new_source,
         -new.country_iso,
         -n) %>%
  tibble()
rm(df_sources_fr)


###### Remove Duplicates FR ######
# Trim dataset by date
df_fr %<>%
  filter(publish_date > as.Date("2019-12-31")) %>%
  filter(publish_date < as.Date("2020-06-01"))

# Trim dataset by country
countries <- c("SN", "CM", "ML", "MA", "TN", "CI", "CG", "DZ", "MG", "BF", "CD", "GN", "BJ", "GA", "TG", "FR", "CN", "US", "GB")
df_fr %<>%
  filter(country_iso %in% countries)
rm(countries)

remove_save_delete <- function(name.source){
  df_id <- df_fr %>%
    filter(source == eval(name.source)) %>% 
    distinct(id, .keep_all = TRUE) %>%
    corpus(text_field = "full_text", # create corpus
           docid_field = "id") %>% # assign unique ID as doc name
    dfm() %>%
    textstat_simil(method = "jaccard") %>% # run textstat
    as.data.frame() %>%
    filter(jaccard > 0.8) %>% # discard duplicates with jaccard higher than 0.8
    mutate(document1 = as.character(document1),
           document2 = as.character(document2)) %>%
    group_by(grp = paste(pmax(document1, document2), pmin(document1, document2), sep = "_")) %>%
    slice(1) %>%
    ungroup() %>%
    select(-grp) %>%
    select(id = document2) %>%
    unique()
  
  if(nrow(df_id) == 0){
    df_sample <- df_fr %>%
      filter(source == eval(name.source)) %>%
      distinct(id, .keep_all = TRUE)
  }
  
  if(nrow(df_id) != 0){
    df_sample <- df_fr %>%
      filter(source == eval(name.source)) %>%
      distinct(id, .keep_all = TRUE) %>%
      anti_join(df_id)
  }
  
  df_sample
}

sources <- df_fr %>% group_by(source) %>% count(source)
sources1 <- sources %>%
  ungroup %>%
  filter(n < 2000) %>%
  select(-n)

final_df1 <- map(sources1$source[1:20],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_FR01.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[21:50],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_FR02.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[51:100],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_FR03.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[101:150],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_FR04.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[151:172],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_FR05.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1,
   sources1,
   remove_save_delete)

remove_save_delete_big <- function(name.source){
  df_id <- df_fr %>%
    filter(source == eval(name.source)) %>% 
    distinct(id, .keep_all = TRUE)
  
  df_id$publish_date <- as.Date(df_id$publish_date)
  df_id$ym <- floor_date(df_id$publish_date, "month")
  available_dates <- unique(df_id$ym)
  
  extract_by_date <- function(availabledates)
  {
    df_id %<>%
      filter(ym == availabledates) %>%  
      corpus(text_field = "full_text", # create corpus
             docid_field = "id") %>% # assign unique ID as doc name
      dfm() %>%
      textstat_simil(method = "jaccard") %>% # run textstat
      as.data.frame() %>%
      filter(jaccard > 0.8) %>% # discard duplicates with jaccard higher than 0.8
      mutate(document1 = as.character(document1),
             document2 = as.character(document2)) %>%
      group_by(grp = paste(pmax(document1, document2), pmin(document1, document2), sep = "_")) %>%
      slice(1) %>%
      ungroup() %>%
      select(-grp) %>%
      select(id = document2) %>%
      unique()
  }
  
  df_id <- map_df(available_dates, extract_by_date) %>%
    compact() %>%
    bind_rows()
  
  if(nrow(df_id) == 0){
    df_sample <- df_fr %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE)
  }
  
  if(nrow(df_id) != 0){
    df_sample <- df_fr %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE) %>%
      anti_join(df_id)
  }
  
  df_sample
}

sources2 <- sources %>%
  ungroup %>%
  filter(n > 2000 & n < 10000) %>%
  select(-n)

final_df2 <- map(sources2$source[1:6],remove_save_delete_big)
df_final2 <- final_df2 %>%
  compact()%>%
  bind_rows()
fwrite(df_final2, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_FR07.csv", quote = TRUE, sep = "|")
rm(df_final2,
   final_df2)

final_df2 <- map(sources2$source[7:12],remove_save_delete_big)
df_final2 <- final_df2 %>%
  compact()%>%
  bind_rows()
fwrite(df_final2, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_FR08.csv", quote = TRUE, sep = "|")
rm(df_final2,
   final_df2)

final_df2 <- map(sources2$source[13:length(sources2$source)],remove_save_delete_big)
df_final2 <- final_df2 %>%
  compact()%>%
  bind_rows()
fwrite(df_final2, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_FR09.csv", quote = TRUE, sep = "|")
rm(df_final2,
   final_df2,
   remove_save_delete_big,
   sources2)

remove_save_delete_very_big <- function(name.source){
  df_id <- df_fr %>%
    filter(source == eval(name.source)) %>% 
    distinct(id, .keep_all = TRUE)
  
  df_id$publish_date <- as.Date(df_id$publish_date)
  df_id$ym <- floor_date(df_id$publish_date, "day")
  available_dates <- unique(df_id$ym)
  
  extract_by_date <- function(availabledates)
  {
    df_id %<>%
      filter(ym == availabledates) %>%  
      corpus(text_field = "full_text", # create corpus
             docid_field = "id") %>% # assign unique ID as doc name
      dfm() %>%
      textstat_simil(method = "jaccard") %>% # run textstat
      as.data.frame() %>%
      filter(jaccard > 0.8) %>% # discard duplicates with jaccard higher than 0.8
      mutate(document1 = as.character(document1),
             document2 = as.character(document2)) %>%
      group_by(grp = paste(pmax(document1, document2), pmin(document1, document2), sep = "_")) %>%
      slice(1) %>%
      ungroup() %>%
      select(-grp) %>%
      select(id = document2) %>%
      unique()
  }
  
  df_id <- map_df(available_dates, extract_by_date) %>%
    compact() %>%
    bind_rows()
  
  if(nrow(df_id) == 0){
    df_sample <- df_fr %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE)
  }
  
  if(nrow(df_id) != 0){
    df_sample <- df_fr %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE) %>%
      anti_join(df_id)
  }
  
  df_sample
}

sources3 <- sources %>%
  ungroup %>%
  filter(n > 10000) %>%
  select(-n)

final_df3 <- map(sources3$source,remove_save_delete_very_big)
df_final3 <- final_df3 %>%
  compact()%>%
  bind_rows()
fwrite(df_final3, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_FR10.csv", quote = TRUE, sep = "|")

###### Remove Duplicates EN ######

# Trim dataset by date
df_en %<>%
  filter(publish_date > as.Date("2019-12-31")) %>%
  filter(publish_date < as.Date("2020-06-01"))

# Trim dataset by country
countries <- c("NG","ZA","GH","KE","ZW","UG","LR","NB","MW","BW","ZM","SL","GM","CM","TZ","EG","US","FR","GB","CN")
df_en %<>%
  filter(country_iso %in% countries)
rm(countries)

remove_save_delete <- function(name.source){
  df_id <- df_en %>%
    filter(source == eval(name.source)) %>% 
    distinct(id, .keep_all = TRUE) %>%
    corpus(text_field = "full_text", # create corpus
           docid_field = "id") %>% # assign unique ID as doc name
    dfm() %>%
    textstat_simil(method = "jaccard") %>% # run textstat
    as.data.frame() %>%
    filter(jaccard > 0.8) %>% # discard duplicates with jaccard higher than 0.8
    mutate(document1 = as.character(document1),
           document2 = as.character(document2)) %>%
    group_by(grp = paste(pmax(document1, document2), pmin(document1, document2), sep = "_")) %>%
    slice(1) %>%
    ungroup() %>%
    select(-grp) %>%
    select(id = document2) %>%
    unique()
  
  if(nrow(df_id) == 0){
    df_sample <- df_en %>%
      filter(source == eval(name.source)) %>%
      distinct(id, .keep_all = TRUE)
  }
  
  if(nrow(df_id) != 0){
    df_sample <- df_en %>%
      filter(source == eval(name.source)) %>%
      distinct(id, .keep_all = TRUE) %>%
      anti_join(df_id)
  }
  
  df_sample
}

sources1 <- sources %>%
  ungroup %>%
  filter(n < 2000) %>%
  select(-n)

final_df1 <- map(sources1$source[1:20],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN01.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[21:50],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN02.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[51:100],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN03.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[101:150],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN04.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[151:200],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN05.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[201:289],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN06.csv", quote = TRUE, sep = "|")
rm(sources1,
   final_df1,
   remove_save_delete, 
   df_final1)

remove_save_delete_big <- function(name.source){
  df_id <- df_en %>%
    filter(source == eval(name.source)) %>% 
    distinct(id, .keep_all = TRUE)
  
  df_id$publish_date <- as.Date(df_id$publish_date)
  df_id$ym <- floor_date(df_id$publish_date, "month")
  available_dates <- unique(df_id$ym)
  
  extract_by_date <- function(availabledates)
  {
    df_id %<>%
      filter(ym == availabledates) %>%  
      corpus(text_field = "full_text", # create corpus
             docid_field = "id") %>% # assign unique ID as doc name
      dfm() %>%
      textstat_simil(method = "jaccard") %>% # run textstat
      as.data.frame() %>%
      filter(jaccard > 0.8) %>% # discard duplicates with jaccard higher than 0.8
      mutate(document1 = as.character(document1),
             document2 = as.character(document2)) %>%
      group_by(grp = paste(pmax(document1, document2), pmin(document1, document2), sep = "_")) %>%
      slice(1) %>%
      ungroup() %>%
      select(-grp) %>%
      select(id = document2) %>%
      unique()
  }
  
  df_id <- map_df(available_dates, extract_by_date) %>%
    compact() %>%
    bind_rows()
  
  if(nrow(df_id) == 0){
    df_sample <- df_en %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE)
  }
  
  if(nrow(df_id) != 0){
    df_sample <- df_en %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE) %>%
      anti_join(df_id)
  }
  
  df_sample
}

df_en %<>%
  mutate(source = str_replace(source, "dailytrust.com.ng", "dailytrust.com"))

sources <- df_en %>% group_by(source) %>% count(source)
sources2 <- sources %>%
  ungroup %>%
  filter(n > 2000 & n < 10000) %>%
  select(-n)

final_df2 <- map(sources2$source[1:2],remove_save_delete_big)
df_final2 <- final_df2 %>%
  compact()%>%
  bind_rows()
fwrite(df_final2, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN07.csv", quote = TRUE, sep = "|")
rm(df_final2,
   final_df2)

final_df2 <- map(sources2$source[3:10],remove_save_delete_big)
df_final2 <- final_df2 %>%
  compact()%>%
  bind_rows()
fwrite(df_final2, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN08.csv", quote = TRUE, sep = "|")
rm(df_final2,
   final_df2)

final_df2 <- map(sources2$source[11:20],remove_save_delete_big)
df_final2 <- final_df2 %>%
  compact()%>%
  bind_rows()
fwrite(df_final2, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN10.csv", quote = TRUE, sep = "|")
rm(df_final2,
   final_df2)

final_df2 <- map(sources2$source[21:30],remove_save_delete_big)
df_final2 <- final_df2 %>%
  compact()%>%
  bind_rows()
fwrite(df_final2, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN11.csv", quote = TRUE, sep = "|")
rm(df_final2,
   final_df2)

final_df2 <- map(sources2$source[31:nrow(sources2)],remove_save_delete_big)
df_final2 <- final_df2 %>%
  compact()%>%
  bind_rows()
fwrite(df_final2, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN12.csv", quote = TRUE, sep = "|")
rm(df_final2,
   final_df2,
   remove_save_delete_big,
   sources,
   sources2)

remove_save_delete_very_big <- function(name.source){
  df_id <- df_en %>%
    filter(source == eval(name.source)) %>% 
    distinct(id, .keep_all = TRUE)
  
  df_id$publish_date <- as.Date(df_id$publish_date)
  df_id$ym <- floor_date(df_id$publish_date, "day")
  available_dates <- unique(df_id$ym)
  
  extract_by_date <- function(availabledates)
  {
    df_id %<>%
      filter(ym == availabledates) %>%  
      corpus(text_field = "full_text", # create corpus
             docid_field = "id") %>% # assign unique ID as doc name
      dfm() %>%
      textstat_simil(method = "jaccard") %>% # run textstat
      as.data.frame() %>%
      filter(jaccard > 0.8) %>% # discard duplicates with jaccard higher than 0.8
      mutate(document1 = as.character(document1),
             document2 = as.character(document2)) %>%
      group_by(grp = paste(pmax(document1, document2), pmin(document1, document2), sep = "_")) %>%
      slice(1) %>%
      ungroup() %>%
      select(-grp) %>%
      select(id = document2) %>%
      unique()
  }
  
  df_id <- map_df(available_dates, extract_by_date) %>%
    compact() %>%
    bind_rows()
  
  if(nrow(df_id) == 0){
    df_sample <- df_en %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE)
  }
  
  if(nrow(df_id) != 0){
    df_sample <- df_en %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE) %>%
      anti_join(df_id)
  }
  
  df_sample
}

sources <- df_en %>% group_by(source) %>% count(source)
sources3 <- sources %>%
  ungroup %>%
  filter(n > 10000) %>%
  select(-n)

final_df3 <- map(sources3$source,remove_save_delete_very_big)
df_final3 <- final_df3 %>%
  compact()%>%
  bind_rows()
fwrite(df_final3, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_EN09.csv", quote = TRUE, sep = "|")

###### Add GDELT Additional ######

df_gdelt_additional <- fread("/Volumes/LaCieOrange/COVID19/GMAC/GDELT_Additional_China.csv") %>%
  select(web_url = DocumentIdentifier,
         web_gdeltdate = GKGRECORDID,
         web_language = TranslationInfo,
         medium_name = SourceCommonName) %>%
  mutate(web_gdeltdate = str_extract(web_gdeltdate, "[0-9]{8}"),
         web_gdeltdate = ymd(web_gdeltdate)) 
df_gdelt_additional %<>% # Remove GDELT duplicates
  distinct(web_url, .keep_all = TRUE)

df_add <- fread("/Volumes/LaCieOrange/COVID19/GMAC/COVID_GDELT_AboutChina.csv") %>% 
  mutate(web_date = as.Date(web_date),
         web_date2 = str_squish(str_replace(web_date2, "[0-9]{2}:[0-9]{2}:[0-9]{2}","")),
         web_date2 = parse_date_time(web_date2, "%Y-%m-%d"))

# Select the best fit for text from 2 scraped options
df_add %<>%
  mutate(chars_web_text = nchar(web_text),
         chars_web_text2 = nchar(web_text2),
         chars_web_text = replace_na(chars_web_text, 0),
         chars_web_text2 = replace_na(chars_web_text2, 0)) %>%
  left_join(df_gdelt_additional, by =  "web_url")
rm(df_gdelt_additional)

df_add %<>%
  mutate(flag_EN = nchar(web_language) < 1,
         flag_FR = str_detect(web_language, "-FRA")) %>%
  filter(flag_EN == TRUE | flag_FR == TRUE)

df_add %<>%
  mutate(web_language = case_when(flag_EN == TRUE ~ "EN",
                                  flag_FR == TRUE ~ "FR",)) %>%
  select(-flag_EN,
         -flag_FR)

# Split data into two, one df for each text column
df_add1 <- df_add %>%
  filter(chars_web_text >= chars_web_text2) %>% # Keep only first text if longer
  mutate(country_name = "",
         country_iso = "") %>%
  select(web_url,
         web_text,
         web_date,
         web_date2,
         web_gdeltdate,
         country_name,
         country_iso ,
         medium_name,
         web_language)
df_add2 <- df_add %>%
  filter(chars_web_text < chars_web_text2) %>% # Keep only second text if longer
  mutate(country_name = "",
         country_iso = "") %>%  
  select(web_url,
         web_text = web_text2,
         web_date,
         web_date2,
         web_gdeltdate,
         country_name,
         country_iso,
         medium_name,
         web_language)
df_add <- rbind(df_add1, df_add2)
rm(df_add1, df_add2)

# Coalesce dates, keeping the best fit
df_add %<>%
  mutate(web_date = as.Date(web_date),
         web_date2 = as.Date(web_date2),
         web_gdeltdate = as.Date(web_gdeltdate),
         publish_date = coalesce(web_gdeltdate,web_date,web_date2))

# Rename, select variables, and format final version of corpus
df_add %<>%
  mutate(title= " ",
         data_source = "GDELT",
         id = paste0("GDELT",seq(800000,(799999+length(df_add$web_text))))) %>%
  select(title,
         publish_date,
         source = medium_name,
         country_iso,
         full_text = web_text,
         web_url,
         data_source,
         id,
         language_gdelt = web_language) %>%
  filter(is.na(publish_date) != TRUE) %>%
  filter(full_text != "")

# Split dataset by language and add metadata (country and source)
df_add_EN <- df_add %>%
  filter(language_gdelt == "EN")
df_add_FR <- df_add %>%
  filter(language_gdelt == "FR")
rm(df_add)

list_FR_sources <- read_excel("list_all_sources_FR_processed.xlsx") %>%
  select(country_iso = new.country_iso,
         source = new_source)
df_add_FR <- df_add_FR %>%
  left_join(list_FR_sources, by = "source") %>%
  select(-country_iso.x) %>%
  rename(country_iso = country_iso.y) %>%
  filter(source != "allafrica.com",
         source != "rtbf.be",
         source != "presidence.ci",
         source != "gouv.bj",
         source != "magazine24.news",
         source != "africadaily.news") %>%
  mutate(country_iso = case_when(source == "telegramme228.com" ~ "TG",
                                 source == "camer.be" ~ "CM",
                                 source == "matinlibre.com" ~ "BJ",
                                 source == "makaila.fr" ~ "TD",
                                 source == "journaldutchad.com" ~ "TD",
                                 source == "lasemaineafricaine.net" ~ "CG",
                                 TRUE ~ as.character(country_iso)))
rm(list_FR_sources)

list_EN_sources <- read_excel("list_all_sources_gdelt_EN_processed.xlsx") %>%
  select(country_iso = new.country_iso,
         source = new.source)

source_remove_EN <- c("beijingnews.net", "allafrica.com", "macaubusiness.com", "devdiscourse.com", 
                      "pctechmag.com", "africatimes.com", "panapress.com", "la-croix.com", "rtbf.be", 
                      "camer.be", "bruneinews.net", "malaysiasun.com", "shanghainews.net", "britainnews.net", 
                      "myanmarnews.net", "africa-newsroom.com", "justiceinfo.net", "ntv.ca", "thecambodianews.net", 
                      "africanbusinessmagazine.com", "africa-me.com", "voazimbabwe.com", "togofirst.com")

df_add_EN <- df_add_EN %>%
  left_join(list_EN_sources, by = "source") %>%
  select(-country_iso.x) %>%
  rename(country_iso = country_iso.y) %>%
  filter(!grepl(paste(source_remove_EN, collapse="|"), source)) %>%
  mutate(country_iso = case_when(source == "bbc.co.uk" ~ "UK",
                                 source == "infosplusgabon.com" ~ "GA",
                                 source == "globaltimes-sl.com" ~ "SL",
                                 source == "allghananews.com" ~ "GH",
                                 source == "theparadigmng.com" ~ "NG",
                                 source == "businessday.ng" ~ "NG",
                                 source == "foroyaa.net" ~ "GM",
                                 source == "zambiadailynation.com" ~ "ZM",
                                 TRUE ~ as.character(country_iso)))
rm(list_EN_sources)

###### Remove Duplicates Additional FR ######

# Trim dataset by date
df_add_FR %<>%
  filter(publish_date > as.Date("2019-12-31")) %>%
  filter(publish_date < as.Date("2020-06-01"))

# Trim dataset by country
countries <- c("SN", "CM", "ML", "MA", "TN", "CI", "CG", "DZ", "MG", "BF", "CD", "GN", "BJ", "GA", "TG", "FR", "CN", "US", "GB")
df_add_FR %<>%
  filter(country_iso %in% countries)
rm(countries)

remove_save_delete <- function(name.source){
  df_id <- df_add_FR %>%
    filter(source == eval(name.source)) %>% 
    distinct(id, .keep_all = TRUE) %>%
    corpus(text_field = "full_text", # create corpus
           docid_field = "id") %>% # assign unique ID as doc name
    dfm() %>%
    textstat_simil(method = "jaccard") %>% # run textstat
    as.data.frame() %>%
    filter(jaccard > 0.8) %>% # discard duplicates with jaccard higher than 0.8
    mutate(document1 = as.character(document1),
           document2 = as.character(document2)) %>%
    group_by(grp = paste(pmax(document1, document2), pmin(document1, document2), sep = "_")) %>%
    slice(1) %>%
    ungroup() %>%
    select(-grp) %>%
    select(id = document2) %>%
    unique()
  
  if(nrow(df_id) == 0){
    df_sample <- df_add_FR %>%
      filter(source == eval(name.source)) %>%
      distinct(id, .keep_all = TRUE)
  }
  
  if(nrow(df_id) != 0){
    df_sample <- df_add_FR %>%
      filter(source == eval(name.source)) %>%
      distinct(id, .keep_all = TRUE) %>%
      anti_join(df_id)
  }
  
  df_sample
}

sources <- df_add_FR %>% group_by(source) %>% count(source)
sources1 <- sources %>%
  ungroup %>%
  filter(n < 2000) %>%
  select(-n)

final_df1 <- map(sources1$source[1:20],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddFR01.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[21:50],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddFR02.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[51:100],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddFR03.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[101:111],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddFR04.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)


###### Remove Duplicates Additional EN ######

# Trim dataset by date
df_add_EN %<>%
  filter(publish_date > as.Date("2019-12-31")) %>%
  filter(publish_date < as.Date("2020-06-01"))

# Trim dataset by country
countries <- c("NG","ZA","GH","KE","ZW","UG","LR","NB","MW","BW","ZM","SL","GM","CM","TZ","EG","US","FR","GB","CN")
df_add_EN %<>%
  filter(country_iso %in% countries)
rm(countries)

remove_save_delete <- function(name.source){
  df_id <- df_add_EN %>%
    filter(source == eval(name.source)) %>% 
    distinct(id, .keep_all = TRUE) %>%
    corpus(text_field = "full_text", # create corpus
           docid_field = "id") %>% # assign unique ID as doc name
    dfm() %>%
    textstat_simil(method = "jaccard") %>% # run textstat
    as.data.frame() %>%
    filter(jaccard > 0.8) %>% # discard duplicates with jaccard higher than 0.8
    mutate(document1 = as.character(document1),
           document2 = as.character(document2)) %>%
    group_by(grp = paste(pmax(document1, document2), pmin(document1, document2), sep = "_")) %>%
    slice(1) %>%
    ungroup() %>%
    select(-grp) %>%
    select(id = document2) %>%
    unique()
  
  if(nrow(df_id) == 0){
    df_sample <- df_add_EN %>%
      filter(source == eval(name.source)) %>%
      distinct(id, .keep_all = TRUE)
  }
  
  if(nrow(df_id) != 0){
    df_sample <- df_add_EN %>%
      filter(source == eval(name.source)) %>%
      distinct(id, .keep_all = TRUE) %>%
      anti_join(df_id)
  }
  
  df_sample
}

sources <- df_add_EN %>% group_by(source) %>% count(source)
sources1 <- sources %>%
  ungroup %>%
  filter(n < 2000) %>%
  select(-n)

final_df1 <- map(sources1$source[1:20],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddEN01.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[21:50],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddEN02.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[51:100],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddEN03.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[101:150],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddEN04.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[151:200],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddEN05.csv", quote = TRUE, sep = "|")
rm(final_df1,
   df_final1)

final_df1 <- map(sources1$source[201:236],remove_save_delete)
df_final1 <- final_df1 %>%
  compact()%>%
  bind_rows()
fwrite(df_final1, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddEN06.csv", quote = TRUE, sep = "|")
rm(sources1,
   final_df1,
   remove_save_delete, 
   df_final1)

remove_save_delete_big <- function(name.source){
  df_id <- df_add_EN %>%
    filter(source == eval(name.source)) %>% 
    distinct(id, .keep_all = TRUE)
  
  df_id$publish_date <- as.Date(df_id$publish_date)
  df_id$ym <- floor_date(df_id$publish_date, "month")
  available_dates <- unique(df_id$ym)
  
  extract_by_date <- function(availabledates)
  {
    df_id %<>%
      filter(ym == availabledates) %>%  
      corpus(text_field = "full_text", # create corpus
             docid_field = "id") %>% # assign unique ID as doc name
      dfm() %>%
      textstat_simil(method = "jaccard") %>% # run textstat
      as.data.frame() %>%
      filter(jaccard > 0.8) %>% # discard duplicates with jaccard higher than 0.8
      mutate(document1 = as.character(document1),
             document2 = as.character(document2)) %>%
      group_by(grp = paste(pmax(document1, document2), pmin(document1, document2), sep = "_")) %>%
      slice(1) %>%
      ungroup() %>%
      select(-grp) %>%
      select(id = document2) %>%
      unique()
  }
  
  df_id <- map_df(available_dates, extract_by_date) %>%
    compact() %>%
    bind_rows()
  
  if(nrow(df_id) == 0){
    df_sample <- df_add_EN %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE)
  }
  
  if(nrow(df_id) != 0){
    df_sample <- df_add_EN %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE) %>%
      anti_join(df_id)
  }
  
  df_sample
}

sources2 <- sources %>%
  ungroup %>%
  filter(n > 2000 & n < 10000) %>%
  select(-n)

final_df2 <- map(sources2$source[1],remove_save_delete_big)
df_final2 <- final_df2 %>%
  compact()%>%
  bind_rows()
fwrite(df_final2, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddEN07.csv", quote = TRUE, sep = "|")
rm(df_final2,
   final_df2)

remove_save_delete_very_big <- function(name.source){
  df_id <- df_add_EN %>%
    filter(source == eval(name.source)) %>% 
    distinct(id, .keep_all = TRUE)
  
  df_id$publish_date <- as.Date(df_id$publish_date)
  df_id$ym <- floor_date(df_id$publish_date, "day")
  available_dates <- unique(df_id$ym)
  
  extract_by_date <- function(availabledates)
  {
    df_id %<>%
      filter(ym == availabledates) %>%  
      corpus(text_field = "full_text", # create corpus
             docid_field = "id") %>% # assign unique ID as doc name
      dfm() %>%
      textstat_simil(method = "jaccard") %>% # run textstat
      as.data.frame() %>%
      filter(jaccard > 0.8) %>% # discard duplicates with jaccard higher than 0.8
      mutate(document1 = as.character(document1),
             document2 = as.character(document2)) %>%
      group_by(grp = paste(pmax(document1, document2), pmin(document1, document2), sep = "_")) %>%
      slice(1) %>%
      ungroup() %>%
      select(-grp) %>%
      select(id = document2) %>%
      unique()
  }
  
  df_id <- map_df(available_dates, extract_by_date) %>%
    compact() %>%
    bind_rows()
  
  if(nrow(df_id) == 0){
    df_sample <- df_add_EN %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE)
  }
  
  if(nrow(df_id) != 0){
    df_sample <- df_add_EN %>%
      filter(source == eval(name.source)) %>% 
      distinct(id, .keep_all = TRUE) %>%
      anti_join(df_id)
  }
  
  df_sample
}

sources <- df_add_EN %>% group_by(source) %>% count(source)
sources3 <- sources %>%
  ungroup %>%
  filter(n > 10000) %>%
  select(-n)

final_df3 <- map(sources3$source[1],remove_save_delete_very_big)
df_final3 <- final_df3 %>%
  compact()%>%
  bind_rows()
fwrite(df_final3, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddEN08.csv", quote = TRUE, sep = "|")

final_df3 <- map(sources3$source[2],remove_save_delete_very_big)
df_final3 <- final_df3 %>%
  compact()%>%
  bind_rows()
fwrite(df_final3, "/Volumes/LaCieOrange/COVID19/GMAC/Analysis/Full_AddEN09.csv", quote = TRUE, sep = "|")

rm(df_add_EN,
   df_add_FR,
   df_final3,
   final_df3,
   remove_save_delete_big,
   remove_save_delete_very_big,
   source_remove_EN,
   sources,
   sources2,
   sources3)

###### Basic Plots ######
df %>%
  mutate(publish_date = ymd(df$publish_date)) %>%
  group_by(month = floor_date(publish_date, unit = "quarter")) %>%
  count(month) %>%
  filter(month >=as.Date("2010-01-01") & month <= as.Date("2020-08-15")) %>%
  ggplot(aes(x=month, y=n)) + 
  geom_bar(stat = "identity")

df %>%
  mutate(publish_date = ymd(df$publish_date)) %>%
  group_by(month = floor_date(publish_date, unit = "quarter"),source.country) %>%
  count(month) %>%
  filter(month >=as.Date("2010-01-01") & month <= as.Date("2020-08-15")) %>%
  ggplot(aes(x=month, y=n)) + 
  geom_bar(stat = "identity") +
  facet_wrap(~ source.country, scales = "free")

df$covid <- str_count(df$full_text, "COVID|Covid|covid|coronavirus")

df %<>%
  mutate(publish_date = ymd(df$publish_date)) %>%
  filter(publish_date >=as.Date("2010-01-01") & publish_date <= as.Date("2020-08-15"))

df %>%
  filter(covid > 0) %>%
  group_by(source.country) %>%
  count(source.country) %>%
  ggplot(aes(x=source.country, y=n)) + 
  geom_bar(stat = "identity")

df %>%
  filter(covid > 0 & source != "Reuters") %>%
  group_by(source.country) %>%
  count(source.country) %>%
  ggplot(aes(x=source.country, y=n)) + 
  geom_bar(stat = "identity")

df %>%
  filter(covid > 0) %>%
  group_by(source.country) %>%
  count(source.country) %>%
  write.xlsx("Counts_mention_COVID_by_country.xlsx")

df %>%
  filter(covid > 0) %>%
  group_by(source.country) %>%
  count(source.country) %>%
  write.xlsx("Counts_total_by_country.xlsx")

df %>%
  select(-title,
         -full_text,
         -web_url) %>%
  fwrite("Metadata.csv")
