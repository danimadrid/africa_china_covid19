library(pacman)
p_load(stringr,textreadr,dplyr,magrittr,tidyverse,lubridate,furrr,data.table)
plan(multisession(workers = 10))

###### Parse English Language Data ######
get_lexis_articles <- function(filename){
  full_text <- read_rtf(filename)
  file.docx <- str_extract(filename, "(?<=Nexis/)(.*?)(?=/)")
  title.docx <- as.character(full_text[1])
  source.docx <- as.character(full_text[2])
  date.docx <- full_text[3]
  pos.length <- grep('Length:', full_text)
  length.docx <- full_text[pos.length]
  pos2 = grep('Classification', full_text)
  pos1 = grep('Body', full_text)
  if(length(pos1) < 1){
    pos1 <- pos2-1
  }
  text.docx <- full_text[(pos1+1):(pos2-1)]
  text.docx <- paste(gsub("[\r\n]", "", text.docx),sep = " ", collapse = " ")
  pos.language <- grep('Language:', full_text)
  language.docx <- full_text[pos.language]
  if(length(pos.language) < 1){
    language.docx <- " "
  }
  pos.pubtype <- grep('Publication-Type:', full_text)
  pubtype.docx <- full_text[pos.pubtype]
  if(length(pos.pubtype) < 1){
    pubtype.docx <- " "
  }
  pos.subject <- grep('Subject:', full_text)
  subject.docx <- full_text[pos.subject]
  if(length(pos.subject) < 1){
    subject.docx <- " "
  }
  pos.geo <- grep('Geographic:', full_text)
  geo.docx <- full_text[pos.geo]
  if(length(pos.geo) < 1){
    geo.docx <- " "
  }
  docx <- data.frame(title.docx, source.docx, pubtype.docx, date.docx, length.docx,
                     text.docx,language.docx,subject.docx,geo.docx, file.docx)
  
  docx %<>% 
    mutate(length.docx = gsub("([0-9]+).*$", "\\1", length.docx),
           length = as.numeric(str_trim(gsub("Length:", "", length.docx))),
           subject = str_trim(gsub("Subject:", "", subject.docx)),
           geo = str_trim(gsub("Geographic:", "", geo.docx)),
           text = str_squish(as.character(text.docx)),
           pub.type = as.character(str_trim(gsub("Publication-Type:", "", pubtype.docx))),
           title = as.character(title.docx),
           medium = as.character(source.docx),
           date.month = str_trim(str_extract(date.docx, "([a-zA-Z]*\\s)")),
           date.day = str_extract(date.docx, "(\\s[0-9]*),"),
           date.day = str_trim(str_replace(date.day, ",", "")),
           date.day = replace_na(date.day, replace = 1),
           date.year = str_trim(str_extract(date.docx, "(\\s[0-9]{4})")), 
           date.docx = paste(date.day, date.month, date.year, sep = "-"),
           pub.date = as.Date(parse_date_time(date.docx, "%e-%B-%Y", exact = TRUE))) %>%
    select(title,
           medium,
           pub.type,
           source.country = file.docx,
           date.month,
           date.day,
           date.year,
           pub.date,
           wordcount = length,
           text = text,
           topic = subject,
           geo = geo)
}

get_lexis_articles_txt <- function(filename){
  full_text <- read_lines(filename, skip_empty_rows = TRUE)
  file.docx <- str_extract(filename, "(?<=Nexis/)(.*?)(?=/)")
  title.docx <- as.character(full_text[1])
  source.docx <- as.character(full_text[2])
  date.docx <- full_text[3]
  pos.length <- grep('Length:', full_text)
  length.docx <- full_text[pos.length]
  pos2 = grep('Classification', full_text)
  pos1 = grep('Body', full_text)
  if(length(pos1) < 1){
    pos1 <- pos2-1
  }
  text.docx <- full_text[(pos1+1):(pos2-1)]
  text.docx <- paste(gsub("[\r\n]", "", text.docx),sep = " ", collapse = " ")
  pos.language <- grep('Language:', full_text)
  language.docx <- full_text[pos.language]
  if(length(pos.language) < 1){
    language.docx <- " "
  }
  pos.pubtype <- grep('Publication-Type:', full_text)
  pubtype.docx <- full_text[pos.pubtype]
  if(length(pos.pubtype) < 1){
    pubtype.docx <- " "
  }
  pos.subject <- grep('Subject:', full_text)
  subject.docx <- full_text[pos.subject]
  if(length(pos.subject) < 1){
    subject.docx <- " "
  }
  pos.geo <- grep('Geographic:', full_text)
  geo.docx <- full_text[pos.geo]
  if(length(pos.geo) < 1){
    geo.docx <- " "
  }
  docx <- data.frame(title.docx, source.docx, pubtype.docx, date.docx, length.docx,
                     text.docx,language.docx,subject.docx,geo.docx, file.docx)
  
  docx %<>% 
    mutate(length.docx = gsub("([0-9]+).*$", "\\1", length.docx),
           length = as.numeric(str_trim(gsub("Length:", "", length.docx))),
           subject = str_trim(gsub("Subject:", "", subject.docx)),
           geo = str_trim(gsub("Geographic:", "", geo.docx)),
           text = str_squish(as.character(text.docx)),
           pub.type = as.character(str_trim(gsub("Publication-Type:", "", pubtype.docx))),
           title = as.character(title.docx),
           medium = as.character(source.docx),
           date.month = str_trim(str_extract(date.docx, "([a-zA-Z]*\\s)")),
           date.day = str_extract(date.docx, "(\\s[0-9]*),"),
           date.day = str_trim(str_replace(date.day, ",", "")),
           date.day = replace_na(date.day, replace = 1),
           date.year = str_trim(str_extract(date.docx, "(\\s[0-9]{4})")), 
           date.docx = paste(date.day, date.month, date.year, sep = "-"),
           pub.date = as.Date(parse_date_time(date.docx, "%d-%B-%Y", exact = TRUE))) %>%
    select(title,
           medium,
           pub.type,
           source.country = file.docx,
           date.month,
           date.day,
           date.year,
           pub.date,
           wordcount = length,
           text = text,
           topic = subject,
           geo = geo)
}

get_lexis_articles_docx <- function(filename){
  full_text <- read_docx(filename)
  file.docx <- str_extract(filename, "(?<=Nexis/)(.*?)(?=/)")
  title.docx <- as.character(full_text[1])
  source.docx <- as.character(full_text[2])
  date.docx <- full_text[3]
  pos.length <- grep('Length:', full_text)
  length.docx <- full_text[pos.length]
  pos2 = grep('Classification', full_text)
  pos1 = grep('Body', full_text)
  if(length(pos1) < 1){
    pos1 <- pos2-1
  }
  text.docx <- full_text[(pos1+1):(pos2-1)]
  text.docx <- paste(gsub("[\r\n]", "", text.docx),sep = " ", collapse = " ")
  pos.language <- grep('Language:', full_text)
  language.docx <- full_text[pos.language]
  if(length(pos.language) < 1){
    language.docx <- " "
  }
  pos.pubtype <- grep('Publication-Type:', full_text)
  pubtype.docx <- full_text[pos.pubtype]
  if(length(pos.pubtype) < 1){
    pubtype.docx <- " "
  }
  pos.subject <- grep('Subject:', full_text)
  subject.docx <- full_text[pos.subject]
  if(length(pos.subject) < 1){
    subject.docx <- " "
  }
  pos.geo <- grep('Geographic:', full_text)
  geo.docx <- full_text[pos.geo]
  if(length(pos.geo) < 1){
    geo.docx <- " "
  }
  docx <- data.frame(title.docx, source.docx, pubtype.docx, date.docx, length.docx,
                     text.docx,language.docx,subject.docx,geo.docx, file.docx)
  
  docx %<>% 
    mutate(length.docx = gsub("([0-9]+).*$", "\\1", length.docx),
           length = as.numeric(str_trim(gsub("Length:", "", length.docx))),
           subject = str_trim(gsub("Subject:", "", subject.docx)),
           geo = str_trim(gsub("Geographic:", "", geo.docx)),
           text = str_squish(as.character(text.docx)),
           pub.type = as.character(str_trim(gsub("Publication-Type:", "", pubtype.docx))),
           title = as.character(title.docx),
           medium = as.character(source.docx),
           date.month = str_trim(str_extract(date.docx, "([a-zA-Z]*\\s)")),
           date.day = str_extract(date.docx, "(\\s[0-9]*),"),
           date.day = str_trim(str_replace(date.day, ",", "")),
           date.day = replace_na(date.day, replace = 1),
           date.year = str_trim(str_extract(date.docx, "(\\s[0-9]{4})")), 
           date.docx = paste(date.day, date.month, date.year, sep = "-"),
           pub.date = as.Date(parse_date_time(date.docx, "%d-%B-%Y", exact = TRUE))) %>%
    select(title,
           medium,
           pub.type,
           source.country = file.docx,
           date.month,
           date.day,
           date.year,
           pub.date,
           wordcount = length,
           text = text,
           topic = subject,
           geo = geo)
}

list_of_files <- list.files("Nexis/FR_AFP_EN/", recursive = TRUE, full.names = TRUE)
nexis.df <- future_map(list_of_files, safely(get_lexis_articles)) %>%
  map("result") %>%
  bind_rows()
fwrite(nexis.df, "Nexis/FullData_AFP_EN.csv",quote = TRUE, sep = "|")

list_of_files <- list.files("Nexis/CN_GlobalTimes/", recursive = TRUE, full.names = TRUE)
nexis.df <- future_map(list_of_files, safely(get_lexis_articles)) %>%
  map("result") %>%
  bind_rows()
fwrite(nexis.df, "Nexis/FullData_GT_EN.csv",quote = TRUE, sep = "|")

list_of_files <- list.files("Nexis/CN_PeoplesDailyEN/", recursive = TRUE, full.names = TRUE)
nexis.df <- future_map(list_of_files, safely(get_lexis_articles)) %>%
  map("result") %>%
  bind_rows()
fwrite(nexis.df, "Nexis/FullData_PD_EN.csv",quote = TRUE, sep = "|")

list_of_files <- list.files("Nexis/CN_ChinaDaily/", recursive = TRUE, full.names = TRUE)
nexis.df <- future_map(list_of_files, safely(get_lexis_articles)) %>%
  map("result") %>%
  bind_rows()
fwrite(nexis.df, "Nexis/FullData_CD_EN.csv",quote = TRUE, sep = "|")

list_of_files <- list.files("Nexis/CN_XinhuaEN/", recursive = TRUE, full.names = TRUE)
nexis.df <- future_map(list_of_files, safely(get_lexis_articles)) %>%
  map("result") %>%
  bind_rows()
fwrite(nexis.df, "Nexis/FullData_XH_EN.csv",quote = TRUE, sep = "|")

list_of_files <- list.files("Nexis/AFR_MultipleEN/", recursive = TRUE, full.names = TRUE, pattern = "*.rtf")
nexis.df <- future_map(list_of_files, safely(get_lexis_articles)) %>%
  map("result") %>%
  bind_rows()
fwrite(nexis.df, "Nexis/FullData_AFR_EN1.csv",quote = TRUE, sep = "|")

list_of_files <- list.files("Nexis/AFR_MultipleEN/", recursive = TRUE, full.names = TRUE, pattern = "*.docx")
nexis.df <- future_map(list_of_files, safely(get_lexis_articles_docx)) %>%
  map("result") %>%
  bind_rows()
fwrite(nexis.df, "Nexis/FullData_AFR_EN2.csv",quote = TRUE, sep = "|")

list_of_files <- list.files("Nexis/US_AP_EN/", recursive = TRUE, full.names = TRUE, pattern = "*.txt")
nexis.df <- future_map(list_of_files, safely(get_lexis_articles_txt), .progress = TRUE) %>%
  map("result") %>%
  bind_rows()
fwrite(nexis.df, "Nexis/FullData_AP_EN.csv",quote = TRUE, sep = "|")



###### Parse French Language Data  ######
get_lexis_articles_fr <- function(filename){
  Sys.setlocale('LC_TIME', "fr_FR.UTF-8")
  full_text <- read_lines(filename, skip_empty_rows = TRUE)
  file.docx <- str_extract(filename, "(?<=Nexis/)(.*?)(?=/)")
  title.docx <- as.character(full_text[1])
  source.docx <- as.character(full_text[2])
  date.docx <- full_text[3]
  date.docx2 <- parse_date_time(date.docx, "%A %d %b %Y")
  if(is.na(date.docx2 == TRUE)){
    date.day2 <- str_squish(str_extract(date.docx, "([0-9]{1,2})"))
    date.month2 <- str_trim(str_extract(date.docx, 
                                        "January|February|March|April|May|June|July|August|September|October|November|December"))
    date.year2 <- str_trim(str_extract(date.docx, "(\\s[0-9]{4})")) 
    date.docx2 <- paste(date.day2, date.month2, date.year2, sep = " ")
    date.docx2 = parse_date_time(date.docx2, "%d %B %Y", exact = TRUE, locale = "en_US.UTF-8")
  }
  pos.length <- grep('Length:', full_text)
  length.docx <- full_text[pos.length]
  pos2 = grep('Classification', full_text)
  pos1 = grep('Body', full_text)
  if(length(pos1) < 1){
    pos1 <- pos2-1
  }
  text.docx <- full_text[(pos1+1):(pos2-1)]
  text.docx <- paste(gsub("[\r\n]", "", text.docx),sep = " ", collapse = " ")
  pos.language <- grep('Language:', full_text)
  language.docx <- full_text[pos.language]
  if(length(pos.language) < 1){
    language.docx <- " "
  }
  pos.pubtype <- grep('Publication-Type:', full_text)
  pubtype.docx <- full_text[pos.pubtype]
  if(length(pos.pubtype) < 1){
    pubtype.docx <- " "
  }
  pos.subject <- grep('Subject:', full_text)
  subject.docx <- full_text[pos.subject]
  if(length(pos.subject) < 1){
    subject.docx <- " "
  }
  pos.geo <- grep('Geographic:', full_text)
  geo.docx <- full_text[pos.geo]
  if(length(pos.geo) < 1){
    geo.docx <- " "
  }
  docx <- data.frame(title.docx, source.docx, pubtype.docx, date.docx, date.docx2, length.docx,
                     text.docx,language.docx,subject.docx,geo.docx, file.docx)
  
  docx %<>% 
    mutate(length.docx = gsub("([0-9]+).*$", "\\1", length.docx),
           length = as.numeric(str_trim(gsub("Length:", "", length.docx))),
           subject = str_trim(gsub("Subject:", "", subject.docx)),
           geo = str_trim(gsub("Geographic:", "", geo.docx)),
           text = str_squish(as.character(text.docx)),
           pub.type = as.character(str_trim(gsub("Publication-Type:", "", pubtype.docx))),
           title = as.character(title.docx),
           medium = as.character(source.docx),
           date.month = str_trim(str_extract(tolower(date.docx), 
                                             "janvier|février|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|décembre")),
           date.day = str_squish(str_extract(date.docx, "(\\s[0-9]{1,2}\\s)")),
           date.day = replace_na(date.day, replace = 1),
           date.year = str_trim(str_extract(date.docx, "(\\s[0-9]{4})")), 
           date.docx = paste(date.day, date.month, date.year, sep = " "),
           pub.date = parse_date_time(date.docx, "%d %B %Y", exact = TRUE),
           pub.date = coalesce(date.docx2,pub.date)) %>%
    select(title,
           medium,
           pub.type,
           source.country = file.docx,
           date.month,
           date.day,
           date.year,
           pub.date,
           wordcount = length,
           text = text,
           topic = subject,
           geo = geo)
}

list_of_files <- list.files("Nexis/FR_AFP_FR", recursive = TRUE, full.names = TRUE, pattern = ".txt")
nexis.df <- future_map(list_of_files[1:12000], safely(get_lexis_articles_fr), .progress = TRUE) %>%
  map("result") %>%
  bind_rows()
nexis.df2 <- future_map(list_of_files[12001:length(list_of_files)], safely(get_lexis_articles_fr), .progress = TRUE) %>%
  map("result") %>%
  bind_rows()
nexis.df <- rbind(nexis.df,nexis.df2)
fwrite(nexis.df, "Nexis/FullData_AFP_FR.csv",quote = TRUE, sep = "|")

list_of_files <- list.files("Nexis/CN_XinhuaFR", recursive = TRUE, full.names = TRUE, pattern = ".txt")
nexis.df <- future_map(list_of_files, safely(get_lexis_articles_fr), .progress = TRUE) %>%
  map("result") %>%
  bind_rows()
fwrite(nexis.df, "Nexis/FullData_XH_FR.csv",quote = TRUE, sep = "|")

list_of_files <- list.files("Nexis/CN_PeoplesDailyFR", recursive = TRUE, full.names = TRUE, pattern = ".txt")
nexis.df <- future_map(list_of_files, safely(get_lexis_articles_fr), .progress = TRUE) %>%
  map("result") %>%
  bind_rows()
fwrite(nexis.df, "Nexis/FullData_PD_FR.csv",quote = TRUE, sep = "|")

list_of_files <- list.files("Nexis/AFR_MultipleFR", recursive = TRUE, full.names = TRUE, pattern = ".txt")
nexis.df <- future_map(list_of_files[1:20000], safely(get_lexis_articles_fr), .progress = TRUE) %>%
  map("result") %>%
  bind_rows()
nexis.df2 <- future_map(list_of_files[20001:length(list_of_files)], safely(get_lexis_articles_fr), .progress = TRUE) %>%
  map("result") %>%
  bind_rows()
nexis.df <- rbind(nexis.df,nexis.df2)
fwrite(nexis.df, "Nexis/FullData_AFR_FR.csv",quote = TRUE, sep = "|")

rm(list_of_files,
   nexis.df,
   nexis.df2,
   get_lexis_articles_fr)
