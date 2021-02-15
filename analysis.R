library(pacman)
#devtools::install_github("chainsawriot/oolong")
p_load(data.table, reader, tidyverse, magrittr, furrr, future, ggplot2, hrbrthemes,quanteda,openxlsx,textreuse, 
       tm, NLP,readxl,zip,RNewsflow, lubridate, quanteda, ISOcodes, GGally, network,sna,gridExtra, ggnetwork,caret,
       e1071,spacyr, reticulate, udpipe,stm, geometry, Rtsne, rsvd, cld2, cld3,devtools,oolong)

####### RQ1: Descriptive Statistics #######

# Load EN df, and compute descriptive
df_en <- list.files("/Volumes/LaCieOrange/COVID19/GMAC/Analysis/", 
                    pattern = "Full_EN", full.names = TRUE) %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble() %>%
      mutate(publish_date = as.Date(publish_date))}) %>%
  bind_rows()
df_en %<>%
  mutate(count_china = str_count(full_text, "China|Chinese|Beijing"),
         count_covid = str_count(full_text, "COVID|covid|coronavirus|influenza|COVID-19|Covid-19|Corona|Coronavirus|pneumonia"),
         about_china = count_china > 0,
         about_covid = count_covid > 0,
         about_china_covid = (about_china == TRUE & about_covid == TRUE),
         about_china_not_covid = (about_china == TRUE & about_covid == FALSE),
         about_covid_not_china = (about_china == FALSE & about_covid == TRUE))
df_en %<>%
  filter(about_china == TRUE | about_covid == TRUE) # Exclude non-China or non-COVID

# Load EN df, and compute descriptive
df_fr <- list.files("/Volumes/LaCieOrange/COVID19/GMAC/Analysis/", 
                    pattern = "Full_FR", full.names = TRUE) %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble() %>%
      mutate(publish_date = as.Date(publish_date))}) %>%
  bind_rows()
df_fr %<>%
  mutate(count_china = str_count(full_text, "Chine|Chinois|Chinoise|Chinoises|Beijing|Pékin"),
         count_covid = str_count(full_text, "COVID|covid|coronavirus|grippe|COVID-19|Covid-19|Corona|Coronavirus|pneumonie"),
         about_china = count_china > 0,
         about_covid = count_covid > 0,
         about_china_covid = (about_china == TRUE & about_covid == TRUE),
         about_china_not_covid = (about_china == TRUE & about_covid == FALSE),
         about_covid_not_china = (about_china == FALSE & about_covid == TRUE))
df_fr %<>%
  filter(about_china == TRUE | about_covid == TRUE) # Exclude non-China or non-COVID

# Retrieve list of sources from Nexis and GDELT
df <- rbind(df_fr, df_en)
df %>%
  filter(data_source == "NEXIS") %>%
  group_by(source) %>%
  count(source)

tmp <- read.xlsx("ListAfricanSources/Scraper_Sources_mod_FR.xlsx", sheet = 2) %>%
  filter(news_site == TRUE) %>%
  distinct(url)
tmp2 <- read.xlsx("ListAfricanSources/Scraper_Sources_mod_FR.xlsx", sheet = 1) %>%
  filter(news_site == TRUE) %>%
  distinct(url)
df %>%
  filter(data_source == "GDELT") %>%
  group_by(source) %>%
  count(source)

# Compute general descriptive stats of corpus
table(df$about_china_not_covid)
table(df$about_china_covid)
table(df$about_covid_not_china)

round(prop.table(table(df$about_china_covid)),2)*100
round(prop.table(table(df$about_china_not_covid)),2)*100
round(prop.table(table(df$about_covid_not_china)),2)*100

descriptive_about_china <- df %>%
  group_by(country_iso) %>%
  summarise(totals = n()) %>%
  ungroup() %>%
  full_join(df) %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  group_by(country_iso,month_pub,about_china, totals) %>%
  count(about_china) %>%
  filter(about_china == TRUE) %>%
  ungroup() %>%
  select(-about_china) %>%
  rename(about_china = n) %>%
  mutate(prop_about_china = round(about_china/totals*100,2))

descriptive_covid_not_china <- df %>%
  group_by(country_iso) %>%
  summarise(totals = n()) %>%
  ungroup() %>%
  full_join(df) %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  group_by(country_iso,month_pub,about_covid_not_china, totals) %>%
  count(about_covid_not_china) %>%
  filter(about_covid_not_china == TRUE) %>%
  ungroup() %>%
  select(-about_covid_not_china) %>%
  rename(about_covid_not_china = n) %>%
  mutate(prop_about_covid_not_china = round(about_covid_not_china/totals*100,2))

descriptive_china_covid <- df %>%
  group_by(country_iso) %>%
  summarise(totals = n()) %>%
  ungroup() %>%
  full_join(df) %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  group_by(country_iso,month_pub,about_china_covid, totals) %>%
  count(about_china_covid) %>%
  filter(about_china_covid == TRUE) %>%
  ungroup() %>%
  select(-about_china_covid) %>%
  rename(about_china_covid = n) %>%
  mutate(prop_about_china_covid = round(about_china_covid/totals*100,2))

descriptive_china_not_covid <- df %>%
  group_by(country_iso) %>%
  summarise(totals = n()) %>%
  ungroup() %>%
  full_join(df) %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  group_by(country_iso,month_pub,about_china_not_covid, totals) %>%
  count(about_china_not_covid) %>%
  filter(about_china_not_covid == TRUE) %>%
  ungroup() %>%
  select(-about_china_not_covid) %>%
  rename(about_china_not_covid = n) %>%
  mutate(prop_about_china_not_covid = round(about_china_not_covid/totals*100,2))

descriptive_summary <- descriptive_china_covid %>%
  left_join(descriptive_covid_not_china, by = c("country_iso", "month_pub")) %>%
  left_join(descriptive_china_not_covid, by = c("country_iso", "month_pub")) %>%
  select(-about_covid_not_china,
         -about_china_covid,
         -about_china_not_covid) %>%
  gather(key = "content_type", value = "n", 
         prop_about_covid_not_china, 
         prop_about_china_covid, 
         prop_about_china_not_covid) %>%
  mutate(country_iso = str_replace(country_iso, "NB", "NA"))

descriptive_summary <- ISO_3166_1 %>%
  select(country_iso = Alpha_2,
         country_name = Name) %>%
  right_join(descriptive_summary) %>%
  mutate(country_name = str_replace(country_name, "Congo, The Democratic Republic of the", "DRC"),
         country_name = str_replace(country_name, "Tanzania, United Republic of", "Tanzania"))

png("Analysis/CountryTotalsUpdated.png", width = 1920, height = 1260)
ggplot(descriptive_summary, aes(x = month_pub, y = n)) + 
  geom_bar(stat = "identity",aes(fill = content_type)) + 
  facet_wrap(~country_name) +
  theme_minimal() +
  theme(strip.text.x = element_text(size = 16), legend.text=element_text(size=14)) +
  xlab("") + ylab("") + scale_fill_manual(values=c("gray70", "black", "gray50"),
                                          name = "",
                                          labels=c("Stories about\nChina and COVID",
                                                   "Stories about\nChina but not COVID",
                                                   "Stories about\nCOVID but not China")) + 
  theme(legend.position="bottom")
dev.off()

df %>%  
  filter(country_iso == "CM") %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(about_china_covid == TRUE) %>%
  group_by(month_pub) %>%
  count(month_pub)

df %>%  
  filter(country_iso == "CM") %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(about_china == TRUE) %>%
  group_by(month_pub) %>%
  count(month_pub)

# Compare French and English descriptive
df_fr %>% 
  filter(about_china == TRUE) %>% 
  group_by(source) %>% 
  count(source) %>% 
  arrange(-n) %>% 
  ungroup()
df_fr %>% 
  filter(about_china == TRUE)

df_fr %>% 
  filter(about_china == TRUE) %>% 
  group_by(source) %>% 
  count(source) %>% 
  arrange(-n) %>% 
  ungroup() %>%
  filter(n < 482) %>%
  summarise(n_sum = sum(n))

df_en %>% 
  filter(about_china == TRUE) %>% 
  group_by(source) %>% 
  count(source) %>% 
  arrange(-n) %>% 
  ungroup()

df_en %>% 
  filter(about_china == TRUE)
  
df_en %>% 
  filter(about_china == TRUE) %>% 
  group_by(source) %>% 
  count(source) %>% 
  arrange(-n) %>% 
  ungroup() %>%
  filter(n < 2307) %>%
  summarise(n_sum = sum(n))

rm(descriptive_about_china,
   descriptive_china_covid,
   descriptive_china_not_covid,
   descriptive_covid_not_china,
   descriptive_summary, 
   df)

####### RQ2: Creating a Gold Standard #######
df %<>%
  mutate(publish_date = paste0(publish_date, " 11:00:00"),
         publish_date = as.character(publish_date),
         id = sprintf("ID_%06d", seq(1,nrow(df)))) %>%
  rename(date = publish_date,
         sourcetype = country_iso)
corp <- corpus(df, docid_field = 'id', text_field='full_text')
corp$date <- ymd_hms(corp$date)
corp$date <- as.POSIXct(corp$date)
dtm <- dfm(corp)

golden_standard_EN <- corp %>%
  corpus_subset(date > as.Date("2020-01-18", "%Y-%m-%d") & date < as.Date("2020-01-21", "%Y-%m-%d")) %>%
  corpus_subset(language_gdelt == "EN") %>%
  quanteda::tokens(remove_punct = TRUE,remove_symbols = TRUE) %>%
  dfm() %>%
  textstat_simil(method = "cosine") 

set.seed(15565)
sample_gs1_EN <- as.data.frame(golden_standard_EN) %>%
  filter(cosine >= 0.97) %>%
  sample_n(15)
sample_gs2_EN <- as.data.frame(golden_standard_EN) %>%
  filter(cosine < 0.97) %>%
  sample_n(35)
sample_gs_EN <- rbind(sample_gs1_EN, sample_gs2_EN)
sample_gs_text_EN <- map2_df(sample_gs_EN$document1, 
                             sample_gs_EN$document2, 
                             function(x,y){
                               tibble(doc1_text = corp[[as.character(x)]],
                                      doc2_text = corp[[as.character(y)]])
                             })
write.xlsx(sample_gs_text_EN, "Analysis/document_gold_standard_EN.xlsx")
gs_results <- read.xlsx("Analysis/document_gold_standard_result_EN.xlsx")


golden_standard_FR <- corp %>%
  corpus_subset(date > as.Date("2020-01-18", "%Y-%m-%d") & date < as.Date("2020-01-25", "%Y-%m-%d")) %>%
  corpus_subset(language_gdelt == "FR") %>%
  quanteda::tokens(remove_punct = TRUE,remove_symbols = TRUE) %>%
  dfm() %>%
  textstat_simil(method = "cosine") 

set.seed(15565)
sample_gs1_FR <- as.data.frame(golden_standard_FR) %>%
  filter(cosine >= 0.97) %>%
  sample_n(15)
sample_gs2_FR <- as.data.frame(golden_standard_FR) %>%
  filter(cosine < 0.97) %>%
  sample_n(35)
sample_gs_FR <- rbind(sample_gs1_FR, sample_gs2_FR)
sample_gs_text_FR <- map2_df(sample_gs_FR$document1, 
                             sample_gs_FR$document2, 
                             function(x,y){
                               tibble(doc1_text = corp[[as.character(x)]],
                                      doc2_text = corp[[as.character(y)]])
                             })
write.xlsx(sample_gs_text_FR, "Analysis/document_gold_standard_FR.xlsx")

gs_en <- read_xlsx("Analysis/document_gold_standard_EN_processed.xlsx") %>%
  bind_cols(sample_gs_EN) %>%
  mutate(same.cosine = case_when(cosine >= 0.97 ~ "identical",
                                 TRUE ~ "diff"))
gs_fr <- read_xlsx("Analysis/document_gold_standard_FR_processed.xlsx") %>%
  bind_cols(sample_gs_FR)  %>%
  mutate(same.cosine = case_when(cosine >= 0.97 ~ "identical",
                                 TRUE ~ "diff"),
         same = str_replace(same, "similar", "identical"))
gs <- rbind(gs_en,gs_fr)
# write.xlsx(gs, "Analysis/GoldenStandard.xlsx")
confusionMatrix(as.factor(gs$same.cosine), as.factor(gs$same), positive = "identical", mode = "prec_recall")

####### RQ3: Computing FR similarities #######
df_sample_fr <- df_fr %>%
  mutate(publish_date = paste0(publish_date, " 11:00:00"),
         publish_date = as.character(publish_date),
         id = sprintf("ID_%06d", seq(1,nrow(df_fr)))) %>%
  rename(date = publish_date,
         sourcetype = country_iso)
corp_fr = corpus(df_sample_fr, docid_field = 'id', text_field='full_text')
corp_fr$date <- ymd_hms(corp_fr$date)
corp_fr$date <- as.POSIXct(corp_fr$date)
dtm_fr <- quanteda::tokens(corp_fr,remove_punct = TRUE,remove_symbols = TRUE) %>%
  dfm()

g_fr <- newsflow_compare(dtm_fr, date_var = "date",
                         hour_window = c(0,48), 
                         min_similarity = 0.97)
network_fr <- as_data_frame(g_fr, 'edges')

edge_processed_fr <- network_fr %>%
  group_by(grp = paste(pmax(from, to), pmin(from, to), sep = "_")) %>%
  slice(1) %>%
  ungroup() %>%
  select(-grp) %>%
  unique()
edge_processed_fr2 <- edge_processed_fr %>%
  filter(weight >= 0.97)

e_from_FR <- edge_processed_fr2 %>%
  filter(weight >= 0.97) %>%
  select(id = from,
         to,
         weight) %>%
  left_join(df_sample_fr, "id")

e_to_FR <- edge_processed_fr2 %>%
  filter(weight >= 0.97) %>%
  select(id = to,
         from,
         weight) %>%
  left_join(df_sample_fr, "id")

edge_processed_fr2 %<>%
  mutate(from_country = e_from_FR$sourcetype,
         to_country = e_to_FR$sourcetype) %>%
  filter(from_country != to_country)

fr_reuse_FR <- edge_processed_fr2 %>%
  filter(from_country == "FR" | to_country == "FR" ) %>%
  mutate(from = case_when(to_country == "FR" ~ as.character(to),
                          to_country != "FR" ~ as.character(from))) %>%
  mutate(to_country = case_when(to_country == "FR" ~ as.character(from_country),
                                to_country != "FR" ~ as.character(to_country)),
         from_country = "FR")

cn_reuse_FR <- edge_processed_fr2 %>%
  filter(from_country == "CN" | to_country == "CN" ) %>%
  mutate(from = case_when(to_country == "CN" ~ as.character(to),
                          to_country != "CN" ~ as.character(from))) %>%
  mutate(to_country = case_when(to_country == "CN" ~ as.character(from_country),
                                to_country != "CN" ~ as.character(to_country)),
         from_country = "CN")

us_reuse_FR <- edge_processed_fr2 %>%
  filter(from_country == "US" | to_country == "US" ) %>%
  mutate(from = case_when(to_country == "US" ~ as.character(to),
                          to_country != "US" ~ as.character(from))) %>%
  mutate(to_country = case_when(to_country == "US" ~ as.character(from_country),
                                to_country != "US" ~ as.character(to_country)),
         from_country = "US")

gb_reuse_FR <- edge_processed_fr2 %>%
  filter(from_country == "GB" | to_country == "GB" ) %>%
  mutate(from = case_when(to_country == "GB" ~ as.character(to),
                          to_country != "GB" ~ as.character(from))) %>%
  mutate(to_country = case_when(to_country == "GB" ~ as.character(from_country),
                                to_country != "GB" ~ as.character(to_country)),
         from_country = "GB")

french_reuse <- rbind(fr_reuse_FR,
                      cn_reuse_FR,
                      us_reuse_FR,
                      gb_reuse_FR) %>%
  mutate(net_edge = paste0(from_country,to_country)) %>%
  group_by(net_edge) %>%
  count(net_edge) %>%
  ungroup() %>%
  mutate(from = str_sub(net_edge, start = 1, end=2),
         to =  str_sub(net_edge, start = 3, end=4),
         to = str_replace(to, "NB", "NA")) %>%
  select(-net_edge) %>%
  relocate(from,
           to,
           n)

french_reuse <- df_sample_fr %>%
  group_by(sourcetype) %>%
  count(sourcetype) %>%
  rename(to = sourcetype,
         totals = n) %>%
  right_join(french_reuse, by = "to") %>%
  filter(to != "CN") %>%
  filter(to != "FR") %>%
  filter(to != "US") %>%
  filter(to != "GB") %>%
  mutate(n = (n/totals)*100) %>%
  relocate(from,
           to,
           n)

cn_net_FR <- french_reuse %>% filter(from == "CN")
nw_cn_FR <- network(cn_net_FR, directed = TRUE, matrix.type = "edgelist")
nw_cn_FR %v% "type" = ifelse(network.vertex.names(nw_cn_FR) %in% c("CN"), "Global", "African")
set.edge.attribute(x = nw_cn_FR, attrname = "weights", round(cn_net_FR$n,2))
plot_net_cn_FR <- ggplot(ggnetwork(nw_cn_FR, arrow.gap = 0.05, layout = "kamadakawai"), 
                         aes(x = x, y = y, xend = xend, yend = yend)) +
  geom_edges(na.rm = TRUE, color = "gray40", arrow = arrow(length = unit(7, "pt"), type = "closed")) +
  geom_nodes(aes(color = type), size = 23, na.rm = TRUE) + 
  theme_blank() + theme(legend.position = "none") + 
  scale_color_manual(values = c("gray60", "black")) +
  geom_edgetext(aes(label = weights), color = "gray30", size = 9) +
  geom_nodetext(aes(label = vertex.names),
                fontface = "bold", color = "white", size = 9)

fr_net_FR <- french_reuse %>% filter(from == "FR")
nw_fr_FR <- network(fr_net_FR, directed = TRUE, matrix.type = "edgelist")
nw_fr_FR %v% "type" = ifelse(network.vertex.names(nw_fr_FR) %in% c("FR"), "Global", "African")
set.edge.attribute(x = nw_fr_FR, attrname = "weights", round(fr_net_FR$n,2))
plot_net_fr_FR <- ggplot(ggnetwork(nw_fr_FR, arrow.gap = 0.05, layout = "kamadakawai"), 
                         aes(x = x, y = y, xend = xend, yend = yend)) +
  geom_edges(na.rm = TRUE, color = "gray40", arrow = arrow(length = unit(7, "pt"), type = "closed")) +
  geom_nodes(aes(color = type), size = 23, na.rm = TRUE) + 
  theme_blank() + theme(legend.position = "none") + 
  scale_color_manual(values = c("gray60", "black")) +
  geom_edgetext(aes(label = weights), color = "gray30", size = 9) +
  geom_nodetext(aes(label = vertex.names),
                fontface = "bold", color = "white", size = 9)

us_net_FR <- french_reuse %>% filter(from == "US")
nw_us_FR <- network(us_net_FR, directed = TRUE, matrix.type = "edgelist")
nw_us_FR %v% "type" = ifelse(network.vertex.names(nw_us_FR) %in% c("US"), "Global", "African")
set.edge.attribute(x = nw_us_FR, attrname = "weights", round(us_net_FR$n,2))
plot_net_us_FR <- ggplot(ggnetwork(nw_us_FR, arrow.gap = 0.05, layout = "kamadakawai"), 
                         aes(x = x, y = y, xend = xend, yend = yend)) +
  geom_edges(na.rm = TRUE, color = "gray40", arrow = arrow(length = unit(7, "pt"), type = "closed")) +
  geom_nodes(aes(color = type), size = 23, na.rm = TRUE) + 
  theme_blank() + theme(legend.position = "none") + 
  scale_color_manual(values = c("gray60", "black")) +
  geom_edgetext(aes(label = weights), color = "gray30", size = 9) +
  geom_nodetext(aes(label = vertex.names),
                fontface = "bold", color = "white", size = 9)

gb_net_FR <- french_reuse %>% filter(from == "GB")
nw_gb_FR <- network(gb_net_FR, directed = TRUE, matrix.type = "edgelist")
nw_gb_FR %v% "type" = ifelse(network.vertex.names(nw_gb_FR) %in% c("GB"), "Global", "African")
set.edge.attribute(x = nw_gb_FR, attrname = "weights", round(gb_net_FR$n,2))
plot_net_gb_FR <- ggplot(ggnetwork(nw_gb_FR, arrow.gap = 0.05, layout = "kamadakawai"), 
                         aes(x = x, y = y, xend = xend, yend = yend)) +
  geom_edges(na.rm = TRUE, color = "gray40", arrow = arrow(length = unit(7, "pt"), type = "closed")) +
  geom_nodes(aes(color = type), size = 23, na.rm = TRUE) + 
  theme_blank() + theme(legend.position = "none") + 
  scale_color_manual(values = c("gray60", "black")) +
  geom_edgetext(aes(label = weights), color = "gray30", size = 9) +
  geom_nodetext(aes(label = vertex.names),
                fontface = "bold", color = "white", size = 9)

png("Analysis/TextReuse_FR097.png", height = 1600, width = 1300)
grid.arrange(plot_net_cn_FR + ggtitle("a. China") + theme(plot.title = element_text(size = 40, face = "bold")),
             plot_net_fr_FR + ggtitle("b. France") + theme(plot.title = element_text(size = 40, face = "bold")),
             plot_net_gb_FR + ggtitle("c. United Kingdom") + theme(plot.title = element_text(size = 40, face = "bold")),
             plot_net_us_FR + ggtitle("d. United States") + theme(plot.title = element_text(size = 40, face = "bold")), 
             nrow = 2)
dev.off()

rm(gb_net_FR, nw_gb_FR, us_net_FR, nw_us_FR, cn_net_FR, nw_fr_FR, fr_net_FR, nw_cn_FR,
   plot_net_cn_FR, plot_net_gb_FR, plot_net_us_FR, plot_net_fr_FR,
   e_to_FR, e_from_FR, edge_processed_fr2, edge_processed_fr,
   corp_fr, dtm_fr, df_fr)

####### RQ3: Computing EN similarities #######
df_sample_en <- df_en %>%
  mutate(publish_date = paste0(publish_date, " 11:00:00"),
         publish_date = as.character(publish_date),
         id = sprintf("ID_%06d", seq(1,nrow(df_en)))) %>%
  rename(date = publish_date,
         sourcetype = country_iso)

corp_en = corpus(df_sample_en, docid_field = 'id', text_field='full_text')
corp_en$date <- ymd_hms(corp_en$date)
corp_en$date <- as.POSIXct(corp_en$date)
dtm_en <- quanteda::tokens(corp_en,remove_punct = TRUE,remove_symbols = TRUE) %>%
  dfm()

g_en <- newsflow_compare(dtm_en, date_var = "date",
                         hour_window = c(0,48), 
                         min_similarity = 0.97)
network_en <- as_data_frame(g_en, 'edges')

edge_processed_en <- network_en %>%
  group_by(grp = paste(pmax(from, to), pmin(from, to), sep = "_")) %>%
  slice(1) %>%
  ungroup() %>%
  select(-grp) %>%
  unique()
edge_processed_en2 <- edge_processed_en %>%
  filter(weight >= 0.97)

e_from_EN <- edge_processed_en2 %>%
  filter(weight >= 0.97) %>%
  select(id = from,
         to,
         weight) %>%
  left_join(df_sample_en, "id")

e_to_EN <- edge_processed_en2 %>%
  filter(weight >= 0.97) %>%
  select(id = to,
         from,
         weight) %>%
  left_join(df_sample_en, "id")

edge_processed_en2 %<>%
  mutate(from_country = e_from_EN$sourcetype,
         to_country = e_to_EN$sourcetype) %>%
  filter(from_country != to_country)

fr_reuse_EN <- edge_processed_en2 %>%
  filter(from_country == "FR" | to_country == "FR" ) %>%
  mutate(from = case_when(to_country == "FR" ~ as.character(to),
                          to_country != "FR" ~ as.character(from))) %>%
  mutate(to_country = case_when(to_country == "FR" ~ as.character(from_country),
                                to_country != "FR" ~ as.character(to_country)),
         from_country = "FR")

cn_reuse_EN <- edge_processed_en2 %>%
  filter(from_country == "CN" | to_country == "CN" ) %>%
  mutate(from = case_when(to_country == "CN" ~ as.character(to),
                          to_country != "CN" ~ as.character(from))) %>%
  mutate(to_country = case_when(to_country == "CN" ~ as.character(from_country),
                                to_country != "CN" ~ as.character(to_country)),
         from_country = "CN")

us_reuse_EN <- edge_processed_en2 %>%
  filter(from_country == "US" | to_country == "US" ) %>%
  mutate(from = case_when(to_country == "US" ~ as.character(to),
                          to_country != "US" ~ as.character(from))) %>%
  mutate(to_country = case_when(to_country == "US" ~ as.character(from_country),
                                to_country != "US" ~ as.character(to_country)),
         from_country = "US")

gb_reuse_EN <- edge_processed_en2 %>%
  filter(from_country == "GB" | to_country == "GB" ) %>%
  mutate(from = case_when(to_country == "GB" ~ as.character(to),
                          to_country != "GB" ~ as.character(from))) %>%
  mutate(to_country = case_when(to_country == "GB" ~ as.character(from_country),
                                to_country != "GB" ~ as.character(to_country)),
         from_country = "GB")

english_reuse <- rbind(fr_reuse_EN,
                       cn_reuse_EN,
                       us_reuse_EN,
                       gb_reuse_EN) %>%
  mutate(net_edge = paste0(from_country,to_country)) %>%
  group_by(net_edge) %>%
  count(net_edge) %>%
  ungroup() %>%
  mutate(from = str_sub(net_edge, start = 1, end=2),
         to =  str_sub(net_edge, start = 3, end=4)) %>%
  select(-net_edge) %>%
  relocate(from,
           to,
           n)

english_reuse <- df_sample_en %>%
  group_by(sourcetype) %>%
  count(sourcetype) %>%
  rename(to = sourcetype,
         totals = n) %>%
  right_join(english_reuse, by = "to") %>%
  filter(to != "CN") %>%
  filter(to != "FR") %>%
  filter(to != "US") %>%
  filter(to != "GB") %>%
  mutate(n = (n/totals)*100) %>%
  relocate(from,
           to,
           n)

cn_net_en <- english_reuse %>% filter(from == "CN") %>% mutate(to = str_replace(to, "NB", "NA"))
nw_cn_EN <- network(cn_net_en, directed = TRUE, matrix.type = "edgelist")
nw_cn_EN %v% "type" = ifelse(network.vertex.names(nw_cn_EN) %in% c("CN"), "Global", "African")
set.edge.attribute(x = nw_cn_EN, attrname = "weights", round(cn_net_en$n,1))
plot_net_cn_EN <- ggplot(ggnetwork(nw_cn_EN, arrow.gap = 0.05, layout = "kamadakawai"), 
                         aes(x = x, y = y, xend = xend, yend = yend)) +
  geom_edges(na.rm = TRUE, color = "gray40", arrow = arrow(length = unit(7, "pt"), type = "closed")) +
  geom_nodes(aes(color = type), size = 23, na.rm = TRUE) + 
  theme_blank() + theme(legend.position = "none") + 
  scale_color_manual(values = c("gray60", "black")) +
  geom_edgetext(aes(label = weights), color = "gray30", size = 9) +
  geom_nodetext(aes(label = vertex.names),
                fontface = "bold", color = "white", size = 9)

fr_net_en <- english_reuse %>% filter(from == "FR") %>% mutate(to = str_replace(to, "NB", "NA"))
nw_fr_EN <- network(fr_net_en, directed = TRUE, matrix.type = "edgelist")
nw_fr_EN %v% "type" = ifelse(network.vertex.names(nw_fr_EN) %in% c("FR"), "Global", "African")
set.edge.attribute(x = nw_fr_EN, attrname = "weights", round(fr_net_en$n,1))
plot_net_fr_EN <- ggplot(ggnetwork(nw_fr_EN, arrow.gap = 0.05, layout = "kamadakawai"), 
                         aes(x = x, y = y, xend = xend, yend = yend)) +
  geom_edges(na.rm = TRUE, color = "gray40", arrow = arrow(length = unit(7, "pt"), type = "closed")) +
  geom_nodes(aes(color = type), size = 23, na.rm = TRUE) + 
  theme_blank() + theme(legend.position = "none") + 
  scale_color_manual(values = c("gray60", "black")) +
  geom_edgetext(aes(label = weights), color = "gray30", size = 9) +
  geom_nodetext(aes(label = vertex.names),
                fontface = "bold", color = "white", size = 9)

us_net_en <- english_reuse %>% filter(from == "US") %>% mutate(to = str_replace(to, "NB", "NA"))
nw_us_EN <- network(us_net_en, directed = TRUE, matrix.type = "edgelist")
nw_us_EN %v% "type" = ifelse(network.vertex.names(nw_us_EN) %in% c("US"), "Global", "African")
set.edge.attribute(x = nw_us_EN, attrname = "weights", round(us_net_en$n,1))
plot_net_us_EN <- ggplot(ggnetwork(nw_us_EN, arrow.gap = 0.05, layout = "kamadakawai"), 
                         aes(x = x, y = y, xend = xend, yend = yend)) +
  geom_edges(na.rm = TRUE, color = "gray40", arrow = arrow(length = unit(7, "pt"), type = "closed")) +
  geom_nodes(aes(color = type), size = 23, na.rm = TRUE) + 
  theme_blank() + theme(legend.position = "none") + 
  scale_color_manual(values = c("gray60", "black")) +
  geom_edgetext(aes(label = weights), color = "gray30", size = 9) +
  geom_nodetext(aes(label = vertex.names),
                fontface = "bold", color = "white", size = 9)

gb_net_en <- english_reuse %>% filter(from == "GB") %>% mutate(to = str_replace(to, "NB", "NA"))
nw_gb_EN <- network(gb_net_en, directed = TRUE, matrix.type = "edgelist")
nw_gb_EN %v% "type" = ifelse(network.vertex.names(nw_gb_EN) %in% c("GB"), "Global", "African")
set.edge.attribute(x = nw_gb_EN, attrname = "weights", round(gb_net_en$n,1))
plot_net_gb_EN <- ggplot(ggnetwork(nw_gb_EN, arrow.gap = 0.05, layout = "kamadakawai"), 
                         aes(x = x, y = y, xend = xend, yend = yend)) +
  geom_edges(na.rm = TRUE, color = "gray40", arrow = arrow(length = unit(7, "pt"), type = "closed")) +
  geom_nodes(aes(color = type), size = 23, na.rm = TRUE) + 
  theme_blank() + theme(legend.position = "none") + 
  scale_color_manual(values = c("gray60", "black")) +
  geom_edgetext(aes(label = weights), color = "gray30", size = 9) +
  geom_nodetext(aes(label = vertex.names),
                fontface = "bold", color = "white", size = 9)

png("Analysis/TextReuse_EN097.png", height = 1600, width = 1300)
grid.arrange(plot_net_cn_EN + ggtitle("a. China") + theme(plot.title = element_text(size = 40, face = "bold")),
             plot_net_fr_EN + ggtitle("b. France") + theme(plot.title = element_text(size = 40, face = "bold")),
             plot_net_gb_EN + ggtitle("c. United Kingdom") + theme(plot.title = element_text(size = 40, face = "bold")),
             plot_net_us_EN + ggtitle("d. United States") + theme(plot.title = element_text(size = 40, face = "bold")), 
             nrow = 2)
dev.off()

rm(gb_net_en, nw_gb_EN, us_net_en, nw_us_EN, cn_net_en, nw_cn_EN, nw_fr_EN,fr_net_en,
   plot_net_cn_EN, plot_net_gb_EN, plot_net_us_EN, plot_net_fr_EN,
   e_to_EN, e_from_EN, edge_processed_en2, edge_processed_en,
   corp_en, dtm_en, df_en, g_en, g_fr)

####### RQ2: Text Reuse Overall #######

# Grouping by producing news source
english_reuse_totals <- rbind(fr_reuse_EN,cn_reuse_EN,gb_reuse_EN,us_reuse_EN) %>%
  filter(to_country != "CN") %>%
  filter(to_country != "FR") %>%
  filter(to_country != "US") %>%
  filter(to_country != "GB") %>%
  select(-to) %>%
  rename(id = from) %>%
  left_join(df_sample_en)

english_reuse_totals %>% group_by(from_country) %>% count(from_country) %>% mutate(prop = n/nrow(df_sample_en)*100)

french_reuse_totals <- rbind(fr_reuse_FR,cn_reuse_FR,gb_reuse_FR,us_reuse_FR)%>%
  filter(to_country != "CN") %>%
  filter(to_country != "FR") %>%
  filter(to_country != "US") %>%
  filter(to_country != "GB")  %>%
  select(-to) %>%
  rename(id = from) %>%
  left_join(df_sample_fr)

tmp <- nrow(df_sample_fr %>%
  filter(sourcetype != "CN") %>%
  filter(sourcetype != "FR") %>%
  filter(sourcetype != "US") %>%
  filter(sourcetype != "GB"))
french_reuse_totals %>% group_by(from_country) %>% count(from_country) %>% mutate(prop = (n/tmp)*100)

tmp <- nrow(df_sample_en %>%
       filter(sourcetype != "CN") %>%
       filter(sourcetype != "FR") %>%
       filter(sourcetype != "US") %>%
       filter(sourcetype != "GB"))
english_reuse_totals %>% group_by(from_country) %>% count(from_country) %>% mutate(prop = (n/tmp)*100)

totals_reuse <- rbind(english_reuse_totals,french_reuse_totals)

# Summary reuse by from country
totals_reuse %>% group_by(from_country) %>% count(from_country) %>% mutate(prop = n/nrow(totals_reuse)*100)

tmp <- totals_reuse %>% 
  group_by(source) %>% 
  count(source) %>% 
  mutate(prop = n/nrow(totals_reuse)*100,
         prop_total = n/(272050)*100)

# Summary reuse by type of content
totals_reuse %>% 
  filter(about_china_covid == TRUE) %>%
  mutate(tots = nrow(.)) %>%
  group_by(from_country, about_china_covid,tots) %>%
  summarise(n = n()) %>%
  mutate(prop = n/tots*100)

totals_reuse %>% 
  filter(about_china_not_covid == TRUE) %>%
  mutate(tots = nrow(.)) %>%
  group_by(from_country, about_china_not_covid,tots) %>%
  summarise(n = n()) %>%
  mutate(prop = n/tots*100)

totals_reuse %>% 
  filter(about_covid_not_china == TRUE) %>%
  mutate(tots = nrow(.)) %>%
  group_by(from_country, about_covid_not_china,tots) %>%
  summarise(n = n()) %>%
  mutate(prop = n/tots*100)

chi.square.reuse <- data.frame(about_china_covid = c(559,5436,2110,320),
                               about_china_not_covid = c(131,766,346,152),
                               about_covid_not_china = c(735,7542,2617,467))
rownames(chi.square.reuse) <- c("CN", "FR", "GB", "US")

chisq.test(chi.square.reuse$about_covid_not_china)

# Grouping by reusing news source
english_reuse_totals <- rbind(fr_reuse_EN,cn_reuse_EN,gb_reuse_EN,us_reuse_EN) %>%
  filter(to_country != "CN") %>%
  filter(to_country != "FR") %>%
  filter(to_country != "US") %>%
  filter(to_country != "GB") %>%
  select(-from) %>%
  rename(id = to) %>%
  left_join(df_sample_en)
french_reuse_totals <- rbind(fr_reuse_FR,cn_reuse_FR,gb_reuse_FR,us_reuse_FR)%>%
  filter(to_country != "CN") %>%
  filter(to_country != "FR") %>%
  filter(to_country != "US") %>%
  filter(to_country != "GB")  %>%
  select(-from) %>%
  rename(id = to) %>%
  left_join(df_sample_fr)
totals_reuse <- rbind(english_reuse_totals,french_reuse_totals)

# Summary reuse by from source
tmp <- totals_reuse %>% 
  filter(from_country == "CN") %>%
  group_by(source) %>% 
  count(source)
tmp2 <- df_sample_en %>%
  rbind(df_sample_fr) %>%
  select(date,
         source,
         sourcetype, 
         language_gdelt,
         id) %>%
  group_by(source) %>%
  count(source) %>%
  rename(totals = n) %>%
  left_join(tmp) %>%
  drop_na() %>%
  mutate(prop = (n/totals)*100)


tmp2 <- english_reuse_totals %>% 
  filter(sourcetype != "CN") %>%
  filter(sourcetype != "FR") %>%
  filter(sourcetype != "US") %>%
  filter(sourcetype != "GB") %>%
  group_by(source) %>% 
  count(source)

# Summary reuse by from country
tmp <- totals_reuse %>% group_by(to_country) %>% count(to_country)
tmp2 <- df_sample_en %>%
  rbind(df_sample_fr) %>%
  select(date,
         source,
         sourcetype, 
         language_gdelt,
         id) %>%
  group_by(sourcetype) %>%
  count(sourcetype) %>%
  rename(totals = n,
         to_country = sourcetype) %>%
  left_join(tmp) %>%
  drop_na() %>%
  mutate(prop = (n/totals)*100)


####### RQ3: Compare FR and EN  #######
# 
nrow(english_reuse_totals)/nrow(df_en)*100
nrow(french_reuse_totals)/nrow(df_fr)*100

# Average text reuse by country and language
french_reuse %>%
  filter(from == "CN") %>%
  group_by(from) %>%
  summarize(average = mean(n))
english_reuse %>%
  filter(from == "CN") %>%
  group_by(from) %>%
  summarize(average = mean(n, na.rm = TRUE))

french_reuse %>%
  filter(from == "FR") %>%
  group_by(from) %>%
  summarize(average = mean(n))
english_reuse %>%
  filter(from == "FR") %>%
  group_by(from) %>%
  summarize(average = mean(n, na.rm = TRUE))

french_reuse %>%
  filter(from == "GB") %>%
  group_by(from) %>%
  summarize(average = mean(n))
english_reuse %>%
  filter(from == "GB") %>%
  group_by(from) %>%
  summarize(average = mean(n, na.rm = TRUE))

french_reuse %>%
  filter(from == "US") %>%
  group_by(from) %>%
  summarize(average = mean(n))
english_reuse %>%
  filter(from == "US") %>%
  group_by(from) %>%
  summarize(average = mean(n, na.rm = TRUE))

french_reuse_totals %>%
  filter(from_country == "CN") %>%
  group_by(source) %>%
  count(source)
english_reuse_totals %>%
  filter(from_country == "CN") %>%
  group_by(source) %>%
  count(source)

rm(chi.square.reuse,
   cn_reuse_EN, cn_reuse_FR, df_reuse, df_sample_en, df_sample_fr,
   english_reuse, english_reuse_totals, fr_reuse_EN, fr_reuse_FR,
   french_reuse, french_reuse_totals, gb_reuse_EN, gb_reuse_FR, 
   network_en, network_fr, tmp, tmp2, totals_reuse, us_reuse_EN, us_reuse_FR)

####### RQ4: STM Preparation #######
# Add metadata about text reuse FR corpus
fr_reuse_stm_FR <- edge_processed_fr2 %>%
  filter(from_country == "FR" | to_country == "FR" ) %>%
  mutate(to = case_when(to_country == "FR" ~ as.character(from),
                        to_country != "FR" ~ as.character(to))) %>%
  mutate(to_country = case_when(to_country == "FR" ~ as.character(from_country),
                                to_country != "FR" ~ as.character(to_country)),
         from_country = "FR")
cn_reuse_stm_FR <- edge_processed_fr2 %>%
  filter(from_country == "CN" | to_country == "CN" ) %>%
  mutate(to = case_when(to_country == "CN" ~ as.character(from),
                        to_country != "CN" ~ as.character(to))) %>%
  mutate(to_country = case_when(to_country == "CN" ~ as.character(from_country),
                                to_country != "CN" ~ as.character(to_country)),
         from_country = "CN")
us_reuse_stm_FR <- edge_processed_fr2 %>%
  filter(from_country == "US" | to_country == "US" ) %>%
  mutate(to = case_when(to_country == "US" ~ as.character(from),
                        to_country != "US" ~ as.character(to))) %>%
  mutate(to_country = case_when(to_country == "US" ~ as.character(from_country),
                                to_country != "US" ~ as.character(to_country)),
         from_country = "US")
gb_reuse_stm_FR <- edge_processed_fr2 %>%
  filter(from_country == "GB" | to_country == "GB" ) %>%
  mutate(to = case_when(to_country == "GB" ~ as.character(from),
                        to_country != "GB" ~ as.character(to))) %>%
  mutate(to_country = case_when(to_country == "GB" ~ as.character(from_country),
                                to_country != "GB" ~ as.character(to_country)),
         from_country = "GB")
FR_reuse_stm <- rbind(fr_reuse_stm_FR,cn_reuse_stm_FR,gb_reuse_stm_FR,us_reuse_stm_FR) %>%
  select(id = to,
         source_from = from_country,
         sourcetype = to_country)

# Add metadata about text reuse EN corpus
fr_reuse_stm_EN <- edge_processed_en2 %>%
  filter(from_country == "FR" | to_country == "FR" ) %>%
  mutate(to = case_when(to_country == "FR" ~ as.character(from),
                        to_country != "FR" ~ as.character(to))) %>%
  mutate(to_country = case_when(to_country == "FR" ~ as.character(from_country),
                                to_country != "FR" ~ as.character(to_country)),
         from_country = "FR")
cn_reuse_stm_EN <- edge_processed_en2 %>%
  filter(from_country == "CN" | to_country == "CN" ) %>%
  mutate(to = case_when(to_country == "CN" ~ as.character(from),
                        to_country != "CN" ~ as.character(to))) %>%
  mutate(to_country = case_when(to_country == "CN" ~ as.character(from_country),
                                to_country != "CN" ~ as.character(to_country)),
         from_country = "CN")
us_reuse_stm_EN <- edge_processed_en2 %>%
  filter(from_country == "US" | to_country == "US" ) %>%
  mutate(to = case_when(to_country == "US" ~ as.character(from),
                        to_country != "US" ~ as.character(to))) %>%
  mutate(to_country = case_when(to_country == "US" ~ as.character(from_country),
                                to_country != "US" ~ as.character(to_country)),
         from_country = "US")
gb_reuse_stm_EN <- edge_processed_en2 %>%
  filter(from_country == "GB" | to_country == "GB" ) %>%
  mutate(to = case_when(to_country == "GB" ~ as.character(from),
                        to_country != "GB" ~ as.character(to))) %>%
  mutate(to_country = case_when(to_country == "GB" ~ as.character(from_country),
                                to_country != "GB" ~ as.character(to_country)),
         from_country = "GB")
EN_reuse_stm <- rbind(fr_reuse_stm_EN,cn_reuse_stm_EN,gb_reuse_stm_EN,us_reuse_stm_EN) %>%
  select(id = to,
         source_from = from_country,
         sourcetype = to_country)
reuse_stm <- rbind(FR_reuse_stm, EN_reuse_stm)  

# Create smaller df, corpus for STM
df_stm <- df %>%
  filter(count_china > 2 & count_covid > 2) %>%
  left_join(reuse_stm, by = c("id", "sourcetype")) %>%
  mutate(stm_id = sprintf("STMID_%06d", seq(1,nrow(.))))
corpus_stm <- corpus(df_stm, docid_field = "stm_id", text_field = "full_text")

df_stm$cld3 <- cld3::detect_language(df_stm$full_text)
df_stm$cld2 <- cld2::detect_language(df_stm$full_text)

####### RQ4: STM FR #######

# Prepare FR DFM for STM & POS tagging with UDPIPE
corpus_stm_fr <- corpus_subset(corpus_stm, language_gdelt == "FR")
tokens_stm_fr <- udpipe(texts(corpus_stm_fr), "french")
list_upos <- c("ADJ", "ADV", "NOUN", "PROPN", "VERB")
dtm_fr <- tokens_stm_fr %>%
  filter(upos %in% list_upos) 
'%ni%' <- Negate('%in%')
list_docid_fr <- df_stm %>%
  filter(language_gdelt == "FR") %>%
  filter(cld3 %in% c("sw","de","en","es")) %>%
  select(stm_id)
list_docid_fr <- list_docid_fr$stm_id
dtm_fr <- dtm_fr %>%
  filter(doc_id %ni% list_docid_fr) 
dtm_fr <- as.tokens(split(dtm_fr$lemma, dtm_fr$doc_id)) %>%
  tokens_remove(c("l'", "pas", "d'", "ne", "s'", "qu'", "n'", "plus", "avoir", " l’", "d’", "faire", "chine",
                  "l’", "n’", "qu’", "c’", "s’", "avril", "mars", "janvier", "février", "mai", "de", "m", "le",
                  "pouvoir", "de", "la", "et", "n", "aller", "lundi", "mardi", "mercredi", "jeudi", "vendredi",
                  "samedi", "dimanche", "repy_author", "am", "en", "xinhua", "afp", "reuters", "chine", 
                  "abonnés", "checknews", "récap", "libé", "actu", "libération.fr", "french.xinhuanet.com", "apa", "we", 
                  "government", "i", "fashion", "voir+", "reply_author", "par", "qui", "les", "nous", "pour", "seneweb", 
                  "nos", "cette", "tout", "même", "j’", "m’", "l&rsquo;épidémie", "croix", "@lacroix", "ratp", "RFI"))
docvars_fr <- docvars(corpus_stm_fr) %>%
  mutate(doc_id = docid(corpus_stm_fr)) %>%
  select(doc_id,
         date,
         sourcetype) %>%
  mutate(day_of_year = yday(date)) %>%
  filter(doc_id %ni% list_docid_fr) 
docvars(dtm_fr) <- docvars_fr
sources_fr <- unique(as.character(corpus_stm_fr$source))
dtm_fr <- tokens_remove(dtm_fr, sources_fr)
dtm_fr <- dfm(dtm_fr)
#topfeatures(dtm_fr, 100)
nfeat(dtm_fr) # 69318

# Get a first sense of the size of k 
my_lda_fit20 <- stm(dtm_fr, 
                    K = 0, # results suggest 70
                    verbose = TRUE, 
                    prevalence = ~ sourcetype + s(day_of_year), 
                    init.type = "Spectral")
# Identify best k
lda_60 <- stm(dtm_fr, 
              K = 60, 
              verbose = TRUE, 
              prevalence = ~ sourcetype + s(day_of_year), 
              init.type = "Spectral")
lda_65 <- stm(dtm_fr, 
              K = 60, 
              verbose = TRUE, 
              prevalence = ~ sourcetype + s(day_of_year), 
              init.type = "Spectral")
lda_70 <- stm(dtm_fr, 
              K = 70, 
              verbose = TRUE, 
              prevalence = ~ sourcetype + s(day_of_year), 
              init.type = "Spectral")

# Reduce dimensionality and fit different models
dtm_fr_trimmed <- dfm_trim(dtm_fr, 
                           min_termfreq = 5, termfreq_type = "count",
                           min_docfreq = 10, docfreq_type = "count")
nfeat(dtm_fr_trimmed) # 11072
lda_fr_35 <- stm(dtm_fr_trimmed, 
                 K = 35, 
                 verbose = TRUE, 
                 prevalence = ~ sourcetype + s(day_of_year), 
                 init.type = "Spectral")
lda_fr_30 <- stm(dtm_fr_trimmed, 
                 K = 30, 
                 verbose = TRUE, 
                 prevalence = ~ sourcetype + s(day_of_year), 
                 init.type = "Spectral")
lda_fr_25 <- stm(dtm_fr_trimmed, 
                 K = 25, 
                 verbose = TRUE, 
                 prevalence = ~ sourcetype + s(day_of_year), 
                 init.type = "Spectral")

#plot.STM(lda_fr_30,type = "labels")
lda_labels_fr_25 <- labelTopics(lda_fr_25)
lda_labels_fr_30 <- labelTopics(lda_fr_30)
lda_labels_fr_35 <- labelTopics(lda_fr_35)

plot(lda_fr_35, type = "summary", xlim = c(0, .3))

shortdocs_fr <- convert(corpus_stm_fr, "data.frame") %>%
  select(text,
         doc_id) %>%
  filter(doc_id %ni% list_docid_fr) %>%
  mutate(text = substr(text, 1,400))

findThoughts(lda_fr_30, texts = shortdocs_fr$text, n = 10, topics = c(17))
#plotQuote(thoughts1, width = 30, main = "Topic 3")

lda_fr_32 <- stm(dtm_fr_trimmed, 
                 K = 32, 
                 verbose = TRUE, 
                 prevalence = ~ sourcetype + s(day_of_year), 
                 init.type = "Spectral")
lda_fr_34 <- stm(dtm_fr_trimmed, 
                 K = 34, 
                 verbose = TRUE, 
                 prevalence = ~ sourcetype + s(day_of_year), 
                 init.type = "Spectral")

labelTopics(lda_fr_32)
labelTopics(lda_fr_34)

oolong_test_rater1 <- create_oolong(lda_fr_35, shortdocs_fr$text)
oolong_test_rater2 <- clone_oolong(oolong_test_rater1)
oolong_test_rater1$do_word_intrusion_test()
oolong_test_rater1$do_topic_intrusion_test()
oolong_test_rater1$lock(force = TRUE)

oolong_test_rater2$do_word_intrusion_test()
oolong_test_rater2$do_topic_intrusion_test()
oolong_test_rater2$lock(force = TRUE)

summarize_oolong(oolong_test_rater1,oolong_test_rater2)

plot.STM(lda_fr_35,type = "labels",topics = c(3,5,9), 
         topic.names = c("Topic X: Ths", "Topic Y: Thas","Topic Z: Thes"))

findThoughts(lda_fr_35, texts = shortdocs_fr$text, n = 10, topics = c(17))

labels_facet <- c("Overview of Cases", "Treatments and Vaccines", "Masks", "Xi Jinping Messages", 
                  "US attacks WHO", "Discrimination against Africans", "China's Global Responses", 
                  "Situation in North/West Africa", "Stock Exchange", "International Community Response", 
                  "Airline Industry", "Pyschological Impact", "Situation in West Africa", 
                  "China's Multilateral Cooperation", "COVID as Zoovirus", 
                  "Situation in Early Affected Areas", "G20 and COVID", "Situation in Frech Hospitals", 
                  "Origins of COVID-19", "Hong Kong and Taiwan politics", 
                  "Cruiseships in Japan and Hong Kng", "France's Containment", 
                  "Testing and Detection in Africa", "African Debt Relief", "Situation in Italy", 
                  "China's Global Cooperation", "Explaining COVID", "China's Support for WHO", 
                  "China's Donations to Africa", "China-US Tensions", "Situation in Madagascar", 
                  "Situation in Europe/Middle East", "Sports and COVID-9", "Daily Life in Wuhan", 
                  "China, Russia, ASEAN")
names(labels_facet) <- colnames(as.data.frame(lda_fr_35$theta))

topic_fr_proportions <- as.data.frame(lda_fr_35$theta) %>%
  cbind(docvars(dtm_fr_trimmed)) %>%
  left_join(df_fr) %>%
  select(V1:V35,
         sourcetype) %>%
  group_by(sourcetype) %>%
  summarise_all(list(mean)) %>%
  melt() %>%
  ggplot(aes(x = sourcetype, y = value, fill = sourcetype)) +
  geom_bar(stat="identity") +
  coord_flip() + facet_wrap(~variable, labeller = labeller(variable = labels_facet))
  
find_top_topics <- function(country_iso_name){
  as.data.frame(lda_fr_35$theta) %>%
    cbind(docvars(dtm_fr_trimmed)) %>%
    left_join(df_fr) %>%
    select(V1:V35,
           sourcetype) %>%
    group_by(sourcetype) %>%
    summarise_all(list(mean)) %>%
    filter(sourcetype == country_iso_name) %>%
    gather(key = topic, value = n, V1:V35) %>%
    top_n(7) %>%
    arrange(-n)  
}

top_topics_fr <- map(unique(df_fr$sourcetype),find_top_topics) %>%
  compact() %>%
  bind_rows()

tibble(topic = colnames(as.data.frame(lda_fr_35$theta)),
       new_name = labels_facet) %>%
  right_join(top_topics_fr) %>%
  arrange(sourcetype, -n) %>%
  select(-topic) %>%
  write_csv("Analysis/prevalent_topics_fr.csv")
  
####### RQ4: STM EN #######
# Prepare EN DFM for STM & POS tagging with UDPIPE
corpus_stm_en <- corpus_subset(corpus_stm, language_gdelt == "EN")
tokens_stm_en <- udpipe(texts(corpus_stm_en), "english")
dtm_en <- tokens_stm_en %>%
  filter(upos %in% list_upos)
list_docid_en <- df_stm %>%
  filter(language_gdelt == "EN") %>%
  filter(cld3 %in% c("sw","de","fr","es")) %>%
  select(stm_id)
list_docid_en <- list_docid_en$stm_id
dtm_en <- dtm_en %>%
  filter(doc_id %ni% list_docid_en) 
dtm_en <- as.tokens(split(dtm_en$lemma, dtm_en$doc_id)) %>%
  tokens_remove(c("more", "also", "be", "year", "day", "so", "january", "february", "march", "april", "may",
                  "june", "july", "august", "september", "october", "november", "december", "monday", "tuesday",
                  "wednesday","thursday","friday","saturday", "sunday", "go", "such", "as", "how", "when",
                  "what","where","even","get","month", "say", "read", "story", "de", "que", "la", "blog",
                  "see", "full", "have", "xinhua", "afp", "reuters", "global", "times", "china", 
                  "los", "con", "el", "las", "y", "time", "post", "report", "pm", "crucero", "cuále", "dominicana", 
                  "francesa", "haití", "baita", "feng/chinadaily.com.", "feb", "feb.", "pic", "headlines", "44th", 
                  "-95'", "ccl.n", "do", "now", "very", "know", "come", "just", "really", "lot", "think", "thing", 
                  "sure", "do", "want", "中文", "分享按钮", "viagra", "login", "uid", "haha", "credits", "600029.ss", 
                  "601111.ss", "blok", "darlak", "deadlygospel", "harshcs", "icag.l", "guglielmo", "hassebroek", 
                  "mangiapane", "t.b.", "nc0v", "e-vending", "subscrib", "bvn", "tmsnrt.rs/2w7hx9t", 
                  "advertising@creamermedia.co.za", "citinewsroom", "downloadbuy", "-natal", "2fast4", "abdool", 
                  "fikile", "groote", "heever", "177th", "olander", "1024*", "-mini", "-week", "quads_screen_width", 
                  "adsbygoogle", "window.adsbygoogle", "document.write", "}if", "document.body.clientwidth", "ap", 
                  "https://apnews.com/understandingtheoutbreak", "https://apnews.com/virusoutbreak", "gov.", "associated", 
                  "bbccoronavirus", "bbchow", "http://apne.ws/2kbx8bd", "https://twitter.com/apfactcheck", "gzero", 
                  "73rd", "146th", "chinaculture.org", "http://www.facebook.com/xinhuanewsagency",
                  "http://www.youtube.com/user/chinaviewtv", "siasun", "tmirob"
                  
  ))
docvars_en <- docvars(corpus_stm_en) %>%
  mutate(doc_id = docid(corpus_stm_en)) %>%
  select(doc_id,
         date,
         sourcetype) %>%
  mutate(day_of_year = yday(date)) %>%
  filter(doc_id %ni% list_docid_en) 
docvars(dtm_en) <- docvars_en
sources_en <- unique(as.character(corpus_stm_en$source))
dtm_en <- tokens_remove(dtm_en, sources_en)
dtm_en <- dfm(dtm_en)
#topfeatures(dtm_en,100)
nfeat(dtm_en) # 113682
# Get a sense of the size of k
sense_of_k_en <- stm(dtm_en, 
                     K = 0, 
                     verbose = TRUE, 
                     prevalence = ~ sourcetype + s(day_of_year), 
                     init.type = "Spectral")
# Identify best k
lda_60 <- stm(dtm_fr, 
              K = 70, 
              verbose = TRUE, 
              prevalence = ~ sourcetype + s(day_of_year), 
              init.type = "Spectral")
lda_65 <- stm(dtm_fr, 
              K = 80, 
              verbose = TRUE, 
              prevalence = ~ sourcetype + s(day_of_year), 
              init.type = "Spectral")
lda_70 <- stm(dtm_fr, 
              K = 85, 
              verbose = TRUE, 
              prevalence = ~ sourcetype + s(day_of_year), 
              init.type = "Spectral")

dtm_en_trimmed <- dfm_trim(dtm_en, 
                           min_termfreq = 5, termfreq_type = "count",
                           min_docfreq = 10, docfreq_type = "count")
nfeat(dtm_en_trimmed) # 24416 
lda_en_35 <- stm(dtm_en_trimmed, 
                 K = 35, 
                 verbose = TRUE, 
                 prevalence = ~ sourcetype + s(day_of_year), 
                 init.type = "Spectral")

lda_en_45 <- stm(dtm_en_trimmed, 
                 K = 45, 
                 verbose = TRUE, 
                 prevalence = ~ sourcetype + s(day_of_year), 
                 init.type = "Spectral")

lda_en_40 <- stm(dtm_en_trimmed, 
                 K = 40, 
                 verbose = TRUE, 
                 prevalence = ~ sourcetype + s(day_of_year), 
                 init.type = "Spectral")
lda_en_50 <- stm(dtm_en_trimmed, 
                 K = 50, 
                 verbose = TRUE, 
                 prevalence = ~ sourcetype + s(day_of_year), 
                 init.type = "Spectral")

lda_en_55 <- stm(dtm_en_trimmed, 
                 K = 55, 
                 verbose = TRUE, 
                 prevalence = ~ sourcetype + s(day_of_year), 
                 init.type = "Spectral")

labelTopics(lda_en_40)
labelTopics(lda_en_45)
labelTopics(lda_en_50)
labelTopics(lda_en_55)

shortdocs_en <- convert(corpus_stm_en, "data.frame") %>%
  select(text,
         doc_id) %>%
  filter(doc_id %ni% list_docid_en) %>%
  mutate(text = substr(text, 1,400))

oolong_test_en_rater1 <- create_oolong(lda_en_45, shortdocs_en$text)
oolong_test_en_rater2 <- clone_oolong(oolong_test_en_rater1)
oolong_test_en_rater1$do_word_intrusion_test()
oolong_test_en_rater1$do_topic_intrusion_test()
oolong_test_en_rater1$lock(force = TRUE)

oolong_test_rater2$do_word_intrusion_test()
oolong_test_rater2$do_topic_intrusion_test()
oolong_test_rater2$lock(force = TRUE)

summarize_oolong(oolong_test_en_rater1)

findThoughts(lda_en_45, texts = shortdocs_en$text, n = 5, topics = c(41:45))
labels_facet_en <- c("Situation Wuhan, Hubei", "Manufacturing Resumes in China", "Situation in East Asia", 
                     "Commodity Prices", "Chinese Global Cooperation", "Education", "Nigerian Politics and COVID", 
                     "Religion", "People most affected", "Chinese Medical Support", "Vaccine and 5g", 
                     "Nigeria's Federal Response", "Global Cases of COVID-19", "Quick Health Response Wuhan", 
                     "Businesses and Trade", "Racism in Guangzhou", "Life under lockdown", "Situation in Europe", 
                     "Racism against Asians", "Disinformation and US", "Situation Worldwide", 
                     "Chinese Medical Supplies", "Negative Impact on Africa", "Traditional Chinese Medicine", 
                     "Impact on Economic Growth", "Reopening Worldwide", "WHO and multilateralism", 
                     "Xi and CCP's response", "COVID Zoonotic Virus", "Stock Exchange", "US Domestic Situation", 
                     "Geoolitics", "Drug Clinical Trials", "Nigeria's Response", "China is in Control", 
                     "Situation in Far East", "Airlines and COVID-19", "COVID and Supply Chains", "China's Reopening", 
                     "Early days of outbreak", "Situation in Iran and Middle East", "Situation in Southern Africa", 
                     "Situation in South Africa", "Sports", "Daily Life under COVID")
names(labels_facet_en) <- colnames(as.data.frame(lda_en_45$theta))

topic_en_proportions <- as.data.frame(lda_en_45$theta) %>%
  cbind(docvars(dtm_en_trimmed)) %>%
  left_join(df_stm) %>%
  select(V1:V45,
         sourcetype) %>%
  group_by(sourcetype) %>%
  summarise_all(list(mean)) %>%
  melt() %>%
  ggplot(aes(x = sourcetype, y = value, fill = sourcetype)) +
  geom_bar(stat="identity") +
  coord_flip() + facet_wrap(~variable, labeller = labeller(variable = labels_facet_en))

find_top_topics <- function(country_iso_name){
  as.data.frame(lda_en_45$theta) %>%
    cbind(docvars(dtm_en_trimmed)) %>%
    left_join(df_stm) %>%
    select(V1:V45,
           sourcetype) %>%
    group_by(sourcetype) %>%
    summarise_all(list(mean)) %>%
    filter(sourcetype == country_iso_name) %>%
    gather(key = topic, value = n, V1:V44) %>%
    top_n(7) %>%
    arrange(-n)  
}

country_names_stm_en <- unique(df_stm %>% filter(language_gdelt == "EN") %>% select(sourcetype))

top_topics_en <- map(country_names_stm_en$sourcetype,find_top_topics) %>%
  compact() %>%
  bind_rows()

tibble(topic = colnames(as.data.frame(lda_en_45$theta)),
       new_name = labels_facet_en) %>%
  right_join(top_topics_en) %>%
  arrange(sourcetype, -n) %>%
  select(-topic) %>%
  write_csv("Analysis/prevalent_topics_en.csv")