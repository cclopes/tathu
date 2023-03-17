library(sf)
library(tidyverse)
library(lubridate)

# Loading shapefiles
# cities <- st_read("/home/camila/git/amazon-storms-aerosols/data/general/shapefiles/AM_Municipios_2019.shp",
#                   stringsAsFactors = F)
# rivers <-
#   st_read("/home/camila/git/amazon-storms-aerosols/data/general/shapefiles/ne_10m_rivers_lake_centerlines.shp",
#           stringsAsFactors = F)

# Loading GoAmazon sites
goam_sites <-
  read_csv(
    "/home/camila/git/amazon-storms-aerosols/data/general/goamazon_sites.csv",
    locale = locale(decimal_mark = ",", grouping_mark = ".")
  ) %>%
  gather(has, answer, c(has_aerosol, has_meteo, has_cloud)) %>%
  filter(answer == "yes") %>%
  distinct(goamazon_reference, .keep_all = T) %>% 
  filter(goamazon_reference == "T3")

# Loading GLD data
gld <- read_csv("/home/camila/git/amazon-storms-aerosols/data/lightning/GLD/GLD360_20150619.txt",
                         col_types = cols(t = col_datetime(format = "%Y-%m-%d %H:%M:%S")))

# Reading TATHU data
tathu <- read_csv("/home/camila/git/tathu/sipam-tracking/out/carol_20150619.csv",
                            col_types = cols(timestamp = col_datetime(format = "%Y-%m-%d %H:%M:%S"))) %>%
  mutate(count = count * 4, event = str_replace(event, "_", "\n"))
tathu_sums <- tathu %>%
  group_by(name) %>%
  summarize(duration = difftime(max(timestamp), min(timestamp), units = "mins"),
            max_area = max(count))
total_families <- tathu %>%
  group_by(name) %>%
  summarise() %>%
  count() %>%
  unlist()
# Converting strings to geometries
tathu_geoms <- st_as_sf(tathu, wkt = "geom", crs = 4326)

# Selecting GLD data within geoms

# - Matching dates
dates_tathu <- tathu %>% 
  select(timestamp) %>% 
  unique() %>% 
  mutate(time_interval = interval(timestamp, timestamp + 10*60)) # Considering 10 min, but VARIES A LOT
gld <- gld %>% 
  mutate(t_radar = ymd_hms(NA))
for(j in 1:length(gld$t)){
  inter <- gld$t[j] %within% dates_tathu$time_interval
  if(any(inter)){
    gld$t_radar[j] <- dates_tathu$timestamp[which(inter)[1]]
  }
}
# - Matching within centroids
gld_points <- st_as_sf(gld, coords = c("lon","lat"), crs = 4326)
T3 <- st_as_sf(goam_sites, coords = c("longitude", "latitude"), crs = 4326)
tathu$gld_count <- NA
tathu$within_T3_10km <- NA
tathu$within_T3_50km <- NA
tathu$within_T3_100km <- NA
for(i in 1:length(tathu$name)){
  gld <- gld_points %>% filter(t_radar == tathu$timestamp[i])
  poly <- tathu_geoms$geom[i]
  points <- st_filter(gld, poly, .predicate = st_is_within_distance, dist = 10000) %>% nrow()
  tathu$gld_count[i] <- points
  within_T3 <- st_filter(T3, poly, .predicate = st_is_within_distance, dist = 10000) %>% nrow()
  tathu$within_T3_10km[i] <- ifelse(within_T3 == 1, TRUE, FALSE)
  within_T3 <- st_filter(T3, poly, .predicate = st_is_within_distance, dist = 50000) %>% nrow()
  tathu$within_T3_50km[i] <- ifelse(within_T3 == 1, TRUE, FALSE)
  within_T3 <- st_filter(T3, poly, .predicate = st_is_within_distance, dist = 100000) %>% nrow()
  tathu$within_T3_100km[i] <- ifelse(within_T3 == 1, TRUE, FALSE)
  
}

write_csv(tathu, "/home/camila/git/tathu/sipam-tracking/out/carol_20150619_mod.csv")
  

# Quick plot
# for(tstamp in tathu$timestamp %>% unique()){
#   print(as_datetime(tstamp, origin = "1970-01-01 00:00:00"))
#   ggplot() +
#     geom_sf(
#       data = cities,
#       fill = NA,
#       size = 0.4,
#       color = "gray"
#     ) +
#     geom_sf(
#       data = rivers,
#       fill = NA,
#       size = 0.4,
#       color = "lightblue"
#     ) +
#     geom_rect(
#       aes(
#         xmin = -61.343496,
#         xmax = -58.640505,
#         ymin = -4.505793,
#         ymax = -1.792021
#       ),
#       fill = NA,
#       color = "black"
#     ) +
#     geom_sf(
#       data = tathu_geoms %>% filter(timestamp == tstamp),
#       fill = NA,
#       size = 0.5,
#       show.legend = F
#     ) +
#     # geom_sf(
#     #   data = tathu %>% filter(timestamp == tstamp, within_T3 == T),
#     #   color = "darkred",
#     #   fill = NA,
#     #   size = 0.5,
#     # ) +
#     geom_sf(
#       data = gld_points %>% filter(t_radar == tstamp),
#       color = "darkgoldenrod",
#       shape = 16,
#       fill = NA,
#     ) +
#     geom_point(
#       data = goam_sites,
#       aes(longitude, latitude),
#       color = "red",
#       shape = 16,
#       size = 2,
#       show.legend = F
#     ) +
#     geom_text(
#       data = goam_sites,
#       aes(longitude, latitude, label = goamazon_reference),
#       position = position_dodge2(0.2, preserve = "single"),
#       vjust = -0.5,
#       color = "red",
#       size = 3.5,
#       show.legend = F
#     ) +
#     coord_sf(
#       xlim = c(-61.343496,-58.640505),
#       ylim = c(-4.505793,-1.792021),
#       expand = T
#     ) +
#     labs(
#       x = NULL,
#       y = NULL,
#       title = paste("Test TATHU -", as_datetime(tstamp, origin = "1970-01-01 00:00:00")),
#     )
#   ggsave(
#     paste("/home/camila/git/tathu/sipam-tracking/out/figs/100km/test tathu", as_datetime(tstamp, origin = "1970-01-01 00:00:00"), ".png"),
#     width = 5,
#     height = 5,
#     dpi = 300,
#     bg = "transparent"
#   )
# }

