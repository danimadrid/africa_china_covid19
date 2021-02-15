###### OA1 - Breakdown of sources by country ######

df_en <- list.files("/Volumes/LaCieOrange/COVID19/GMAC/Analysis/", 
                    pattern = "Full_EN", full.names = TRUE) %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble() %>%
      mutate(publish_date = as.Date(publish_date))}) %>%
  bind_rows() %>%
  select(country_iso,
         source)

df_fr <- list.files("/Volumes/LaCieOrange/COVID19/GMAC/Analysis/", 
                    pattern = "Full_FR", full.names = TRUE) %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble() %>%
      mutate(publish_date = as.Date(publish_date))}) %>%
  bind_rows() %>%
  select(country_iso,
         source) %>%
  rbind(df_en)

total_sources <- df_fr %>%
  group_by(country_iso, source) %>%
  summarise(totals = n()) %>%
  ungroup() %>%
  group_by(country_iso) %>%
  summarise(sources = n())

write_csv(path = "Analysis/OA1_sources.csv", x = total_sources)
rm(df_en,
   df_fr,
   total_sources)

###### OA2a - Frequency of keywords in EN corpus ######
df_en <- list.files("/Volumes/LaCieOrange/COVID19/GMAC/Analysis/", 
                    pattern = "Full_EN", full.names = TRUE) %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble() %>%
      mutate(publish_date = as.Date(publish_date))}) %>%
  bind_rows()
df_en %<>%
  select(publish_date,
         data_source,
         full_text) %>%
  mutate(count_covid = str_count(full_text, "COVID|covid|COVID-19|Covid-19"),
         count_corona = str_count(full_text, "coronavirus|Corona|Coronavirus|corona"),
         count_pandemic = str_count(full_text, "pandemic|Pandemic"),
         count_epidemic = str_count(full_text, "epidemic|Epidemic"),
         count_sars = str_count(full_text, "SARS|sars"),
         count_influenza = str_count(full_text, "influenza|Influenza"),
         count_pneumonia = str_count(full_text, "pneumonia|Pneumonia"),
         count_outbreak = str_count(full_text, "outbreak|Outbreak"),
         count_flu = str_count(full_text, "flu|Flu")) %>%
  select(-full_text)
  
total_stories <- df_en %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  group_by(data_source, month_pub) %>%
  summarise(totals = n()) %>%
  ungroup()

descriptive_covid <- df_en %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_covid > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_covid = n()) %>%
  ungroup()
descriptive_corona <- df_en %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_corona > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_corona = n()) %>%
  ungroup()
descriptive_pandemic <- df_en %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_pandemic > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_pandemic = n()) %>%
  ungroup()
descriptive_epidemic <- df_en %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_epidemic > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_epidemic = n()) %>%
  ungroup()
descriptive_sars <- df_en %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_sars > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_sars = n()) %>%
  ungroup()
descriptive_influenza <- df_en %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_influenza > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_influenza = n()) %>%
  ungroup()
descriptive_pneumonia <- df_en %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_pneumonia > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_pneumonia = n()) %>%
  ungroup()
descriptive_flu <- df_en %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_flu > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_flu = n()) %>%
  ungroup()
descriptive_outbreak <- df_en %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_outbreak > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_outbreak = n()) %>%
  ungroup()

total_stories %<>%
  full_join(descriptive_covid) %>%
  full_join(descriptive_corona) %>%
  full_join(descriptive_pandemic) %>%
  full_join(descriptive_epidemic) %>%
  full_join(descriptive_sars) %>%
  full_join(descriptive_influenza) %>%
  full_join(descriptive_flu) %>%
  full_join(descriptive_pneumonia) %>%
  full_join(descriptive_outbreak)

percent_stories <- total_stories %>%
  mutate_at(vars(starts_with("count_")), funs((. / totals)*100))
write_csv(path = "Analysis/OA1.csv", x = percent_stories)


###### OA2b - Frequency of keywords in FR corpus ######

df_fr <- list.files("/Volumes/LaCieOrange/COVID19/GMAC/Analysis/", 
                    pattern = "Full_FR", full.names = TRUE) %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble() %>%
      mutate(publish_date = as.Date(publish_date))}) %>%
  bind_rows()
df_fr %<>%
  select(publish_date,
         data_source,
         full_text) %>%
  mutate(count_covid = str_count(full_text, "COVID|covid|COVID-19|Covid-19"),
         count_corona = str_count(full_text, "coronavirus|Corona|Coronavirus|corona"),
         count_pandemic = str_count(full_text, "pandémie|Pandémie"),
         count_epidemic = str_count(full_text, "épidémie|Épidémie|épidémiq*|Épidémiq*"),
         count_sars = str_count(full_text, "SRAS|sras|SARS|sars"),
         count_influenza = str_count(full_text, "grippe|Grippe"),
         count_pneumonia = str_count(full_text, "pneumonie|Pneumonie"),
         count_outbreak = str_count(full_text, "flambée|Flambée"),
         count_flu = str_count(full_text, "rheum|Rheum")) %>%
  select(-full_text)

total_stories <- df_fr %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  group_by(data_source, month_pub) %>%
  summarise(totals = n()) %>%
  ungroup()

descriptive_covid <- df_fr %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_covid > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_covid = n()) %>%
  ungroup()
descriptive_corona <- df_fr %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_corona > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_corona = n()) %>%
  ungroup()
descriptive_pandemic <- df_fr %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_pandemic > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_pandemic = n()) %>%
  ungroup()
descriptive_epidemic <- df_fr %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_epidemic > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_epidemic = n()) %>%
  ungroup()
descriptive_sars <- df_fr %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_sars > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_sars = n()) %>%
  ungroup()
descriptive_influenza <- df_fr %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_influenza > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_influenza = n()) %>%
  ungroup()
descriptive_pneumonia <- df_fr %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_pneumonia > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_pneumonia = n()) %>%
  ungroup()
descriptive_flu <- df_fr %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_flu > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_flu = n()) %>%
  ungroup()
descriptive_outbreak <- df_fr %>%
  mutate(month_pub = month(publish_date, label = TRUE)) %>%
  filter(count_outbreak > 0) %>%
  group_by(data_source, month_pub) %>%
  summarise(count_outbreak = n()) %>%
  ungroup()

total_stories %<>%
  full_join(descriptive_covid) %>%
  full_join(descriptive_corona) %>%
  full_join(descriptive_pandemic) %>%
  full_join(descriptive_epidemic) %>%
  full_join(descriptive_sars) %>%
  full_join(descriptive_influenza) %>%
  full_join(descriptive_flu) %>%
  full_join(descriptive_pneumonia) %>%
  full_join(descriptive_outbreak)

percent_stories <- total_stories %>%
  mutate_at(vars(starts_with("count_")), funs((. / totals)*100))
write_csv(path = "Analysis/OA1FR.csv", x = percent_stories)

rm(descriptive_corona,
   descriptive_covid,
   descriptive_epidemic,
   descriptive_flu,
   descriptive_influenza,
   descriptive_outbreak,
   descriptive_pandemic,
   descriptive_pneumonia,
   descriptive_sars,
   percent_stories,
   total_stories)

###### OA3a - Frequency of sources in FR corpus ######
df_en <- list.files("/Volumes/LaCieOrange/COVID19/GMAC/Analysis/", 
                    pattern = "Full_EN", full.names = TRUE) %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble() %>%
      mutate(publish_date = as.Date(publish_date))}) %>%
  bind_rows()
df_en %<>%
  select(country_iso,
         data_source,
         full_text,
         source) %>%
  filter(source != "xinhuanet.com",
         source != "reuters.com",
         source != "afp.com",
         source != "bbc.com",
         source != "lemonde.fr") %>%
  mutate(count_xinhua = str_count(full_text, "Xinhua"),
         count_reuters = str_count(full_text, "Reuters"),
         count_ap = str_count(full_text, "Associated Press"),
         count_afp = str_count(full_text, "AFP|Agence France-Presse"),
         count_bbc = str_count(full_text, "BBC"),
         count_cgtn = str_count(full_text, "CGTN"),
         count_cnn = str_count(full_text, "CNN"),
         count_cd = str_count(full_text, "China Daily"),
         count_lemonde = str_count(full_text, "Le Monde")) %>%
  select(-full_text)

total_stories <- df_en %>%
  group_by(country_iso) %>%
  summarise(totals = n()) %>%
  ungroup()

descriptive_xinhua <- df_en %>%
  filter(count_xinhua > 0) %>%
  group_by(country_iso) %>%
  summarise(count_xinhua = n()) %>%
  ungroup()
descriptive_reuters <- df_en %>%
  filter(count_reuters > 0) %>%
  group_by(country_iso) %>%
  summarise(count_reuters = n()) %>%
  ungroup()
descriptive_ap <- df_en %>%
  filter(count_ap > 0) %>%
  group_by(country_iso) %>%
  summarise(count_ap = n()) %>%
  ungroup()
descriptive_afp <- df_en %>%
  filter(count_afp > 0) %>%
  group_by(country_iso) %>%
  summarise(count_afp = n()) %>%
  ungroup()
descriptive_bbc <- df_en %>%
  filter(count_bbc > 0) %>%
  group_by(country_iso) %>%
  summarise(count_bbc = n()) %>%
  ungroup()
descriptive_cgtn <- df_en %>%
  filter(count_cgtn > 0) %>%
  group_by(country_iso) %>%
  summarise(count_cgtn = n()) %>%
  ungroup()
descriptive_cnn <- df_en %>%
  filter(count_cnn > 0) %>%
  group_by(country_iso) %>%
  summarise(count_cnn = n()) %>%
  ungroup()
descriptive_cd <- df_en %>%
  filter(count_cd > 0) %>%
  group_by(country_iso) %>%
  summarise(count_cd = n()) %>%
  ungroup()
descriptive_lemonde <- df_en %>%
  filter(count_lemonde > 0) %>%
  group_by(country_iso) %>%
  summarise(count_lemonde = n()) %>%
  ungroup()

total_stories %<>%
  full_join(descriptive_xinhua) %>%
  full_join(descriptive_reuters) %>%
  full_join(descriptive_ap) %>%
  full_join(descriptive_afp) %>%
  full_join(descriptive_bbc) %>%
  full_join(descriptive_cgtn) %>%
  full_join(descriptive_cnn) %>%
  full_join(descriptive_cd) %>%
  full_join(descriptive_lemonde)

percent_stories <- total_stories %>%
  mutate_at(vars(starts_with("count_")), funs((. / totals)*100))
write_csv(path = "Analysis/OA2EN.csv", x = percent_stories)

rm(descriptive_afp,
   descriptive_ap,
   descriptive_xinhua,
   descriptive_reuters,
   descriptive_lemonde,
   descriptive_cnn,
   descriptive_cgtn,
   descriptive_cd,
   descriptive_bbc,
   total_stories,
   percent_stories, 
   df_en)

###### OA3b - Frequency of sources in FR corpus ######
df_fr <- list.files("/Volumes/LaCieOrange/COVID19/GMAC/Analysis/", 
                    pattern = "Full_FR", full.names = TRUE) %>%
  map(function(x){
    temp <- fread(x) %>%
      tibble() %>%
      mutate(publish_date = as.Date(publish_date))}) %>%
  bind_rows()
df_fr %<>%
  select(country_iso,
         data_source,
         full_text,
         source) %>%
  filter(source != "xinhuanet.com",
         source != "reuters.com",
         source != "afp.com",
         source != "bbc.com",
         source != "lemonde.fr") %>%
  mutate(count_xinhua = str_count(full_text, "Xinhua"),
         count_reuters = str_count(full_text, "Reuters"),
         count_ap = str_count(full_text, "Associated Press"),
         count_afp = str_count(full_text, "AFP|Agence France-Presse"),
         count_bbc = str_count(full_text, "BBC"),
         count_cgtn = str_count(full_text, "CGTN"),
         count_cnn = str_count(full_text, "CNN"),
         count_cd = str_count(full_text, "China Daily"),
         count_lemonde = str_count(full_text, "Le Monde")) %>%
  select(-full_text)

total_stories <- df_fr %>%
  group_by(country_iso) %>%
  summarise(totals = n()) %>%
  ungroup()

descriptive_xinhua <- df_fr %>%
  filter(count_xinhua > 0) %>%
  group_by(country_iso) %>%
  summarise(count_xinhua = n()) %>%
  ungroup()
descriptive_reuters <- df_fr %>%
  filter(count_reuters > 0) %>%
  group_by(country_iso) %>%
  summarise(count_reuters = n()) %>%
  ungroup()
descriptive_ap <- df_fr %>%
  filter(count_ap > 0) %>%
  group_by(country_iso) %>%
  summarise(count_ap = n()) %>%
  ungroup()
descriptive_afp <- df_fr %>%
  filter(count_afp > 0) %>%
  group_by(country_iso) %>%
  summarise(count_afp = n()) %>%
  ungroup()
descriptive_bbc <- df_fr %>%
  filter(count_bbc > 0) %>%
  group_by(country_iso) %>%
  summarise(count_bbc = n()) %>%
  ungroup()
descriptive_cgtn <- df_fr %>%
  filter(count_cgtn > 0) %>%
  group_by(country_iso) %>%
  summarise(count_cgtn = n()) %>%
  ungroup()
descriptive_cnn <- df_fr %>%
  filter(count_cnn > 0) %>%
  group_by(country_iso) %>%
  summarise(count_cnn = n()) %>%
  ungroup()
descriptive_cd <- df_fr %>%
  filter(count_cd > 0) %>%
  group_by(country_iso) %>%
  summarise(count_cd = n()) %>%
  ungroup()
descriptive_lemonde <- df_fr %>%
  filter(count_lemonde > 0) %>%
  group_by(country_iso) %>%
  summarise(count_lemonde = n()) %>%
  ungroup()

total_stories %<>%
  full_join(descriptive_xinhua) %>%
  full_join(descriptive_reuters) %>%
  full_join(descriptive_ap) %>%
  full_join(descriptive_afp) %>%
  full_join(descriptive_bbc) %>%
  full_join(descriptive_cgtn) %>%
  full_join(descriptive_cnn) %>%
  full_join(descriptive_cd) %>%
  full_join(descriptive_lemonde)

percent_stories <- total_stories %>%
  mutate_at(vars(starts_with("count_")), funs((. / totals)*100))
write_csv(path = "Analysis/OA2FR.csv", x = percent_stories)

rm(descriptive_afp,
   descriptive_ap,
   descriptive_xinhua,
   descriptive_reuters,
   descriptive_lemonde,
   descriptive_cnn,
   descriptive_cgtn,
   descriptive_cd,
   descriptive_bbc,
   total_stories,
   percent_stories, 
   df_fr)