library(sf)
library(tidyverse)
library(cowplot)

# Function to draw circle
dfCircle <- function(LonDec, LatDec, Km) {
  # - LatDec = latitude in decimal degrees of the center of the circle
  # - LonDec = longitude in decimal degrees
  # - Km = radius of the circle in kilometers
  
  # Mean Earth radius in kilometers
  # - Change this to 3959 and you will have your function working in miles
  ER <- 6371
  # Angles in degrees
  AngDeg <- seq(1:360)
  # Latitude of the center of the circle in radians
  Lat1Rad <- LatDec * (pi / 180)
  # Longitude of the center of the circle in radians
  Lon1Rad <- LonDec * (pi / 180)
  # Angles in radians
  AngRad <- AngDeg * (pi / 180)
  # Latitude of each point of the circle rearding to angle in radians
  Lat2Rad <- asin(sin(Lat1Rad) * cos(Km / ER) +
                    cos(Lat1Rad) * sin(Km / ER) * cos(AngRad))
  # Longitude of each point of the circle rearding to angle in radians
  Lon2Rad <-
    Lon1Rad + atan2(sin(AngRad) * sin(Km / ER) * cos(Lat1Rad),
                    cos(Km / ER) - sin(Lat1Rad) * sin(Lat2Rad))
  # Latitude of each point of the circle rearding to angle in radians
  Lat2Deg <- Lat2Rad * (180 / pi)
  # Longitude of each point of the circle rearding to angle in degrees
  # - Conversion of radians to degrees deg = rad*(180/pi)
  Lon2Deg <- Lon2Rad * (180 / pi)
  return(data.frame(lon = Lon2Deg, lat = Lat2Deg))
}


# Loading shapefiles
cities <- st_read(
  "/home/camilacl/git/amazon-storms-aerosols/data/general/shapefiles/AM_Municipios_2019.shp",
  stringsAsFactors = F)
rivers <- st_read(
  "/home/camilacl/git/amazon-storms-aerosols/data/general/shapefiles/ne_10m_rivers_lake_centerlines.shp",
  stringsAsFactors = F)

# Loading GoAmazon sites
goam_sites <-
  read_csv(
    "/home/camilacl/git/amazon-storms-aerosols/data/general/goamazon_sites.csv",
    locale = locale(decimal_mark = ",", grouping_mark = ".")
  ) %>%
  gather(has, answer, c(has_aerosol, has_meteo, has_cloud)) %>%
  filter(answer == "yes") %>%
  distinct(goamazon_reference, .keep_all = T)
goam_circles_50 <-
  map2_dfr(goam_sites$longitude,
           goam_sites$latitude,
           ~ dfCircle(.x, .y, 50),
           .id = "id") %>%
  mutate(
    id = ifelse(id == 1, goam_sites$goamazon_reference[1], id),
    id = ifelse(id == 2, goam_sites$goamazon_reference[2], id),
    id = ifelse(id == 3, goam_sites$goamazon_reference[3], id),
    id = ifelse(id == 4, goam_sites$goamazon_reference[4], id),
    id = ifelse(id == 5, goam_sites$goamazon_reference[5], id),
    id = ifelse(id == 6, goam_sites$goamazon_reference[6], id)
  )

# Plot settings
theme_set(theme_bw())
theme_update(
  plot.title = element_text(hjust = 0.5),
  plot.background = element_rect(fill = "transparent", colour = NA)
)

# DRY SEASON
# Reading data
test_dry_season <- read_csv("out/test_dry_season_filter.csv",
                            col_types = cols(timestamp = col_datetime(format = "%Y-%m-%d %H:%M:%S"))) %>%
  mutate(count = count, event = str_replace(event, "_", "\n")) # count * 4 for 2-km res
test_dry_season_sums <- test_dry_season %>%
  group_by(name) %>%
  summarize(duration = difftime(max(timestamp), min(timestamp), units = "mins"),
            max_area = max(count))
total_families <- test_dry_season %>%
  group_by(name) %>%
  summarise() %>%
  count() %>%
  unlist()

# Converting strings to geometries
centroids <- st_as_sf(test_dry_season, wkt = "centroid")
geoms <- st_as_sf(test_dry_season, wkt = "geom")
st_crs(centroids) <- 4326
st_crs(geoms) <- 4326

# Plotting distributions
plots <- list()
# Size (count)
plots[[1]] <- ggplot(test_dry_season) +
  geom_histogram(
    aes(x = count, fill = "All"),
    binwidth = 100,
    color = NA,
    alpha = 0.5
  ) +
  geom_histogram(
    data = test_dry_season_sums,
    aes(x = max_area, fill = "Max"),
    binwidth = 100,
    color = NA,
    alpha = 0.5
  ) +
  labs(x = "Area (km²)", y = "Count", fill = NULL) +
  scale_fill_manual(values = c("black", "red")) +
  theme(legend.position = c(0.85, 0.85),
        legend.background = element_blank())
# Duration
plots[[2]] <- ggplot(test_dry_season_sums) +
  geom_histogram(aes(x = duration), binwidth = 12) +
  labs(x = "Duration (min)", y = "Count")
# Max, Mean Z
plots[[3]] <- ggplot(test_dry_season) +
  geom_histogram(
    aes(x = max, fill = "Max"),
    binwidth = 1,
    color = NA,
    alpha = 0.5
  ) +
  geom_histogram(
    aes(x = mean, fill = "Mean"),
    binwidth = 1,
    color = NA,
    alpha = 0.5
  ) +
  labs(x = "Reflectivity (dBZ)", y = "Count", fill = NULL) +
  scale_fill_manual(values = c("red", "darkgreen")) +
  theme(legend.position = c(0.85, 0.85),
        legend.background = element_blank())
# Type of event
plots[[4]] <- ggplot(test_dry_season) +
  geom_histogram(aes(x = event), stat = "count") +
  stat_count(
    aes(x = event, y = ..count.., label = ..count..),
    geom = "text",
    vjust = -0.5,
    size = 3
  ) +
  labs(x = "Type", y = "Count") +
  scale_y_continuous(expand = expansion(mult = c(0, .15)))
# Joining filter plots
grid <- plot_grid(plotlist = plots, labels = "auto")
title <- ggdraw() +
  draw_label(
    paste(
      "Dry Season Test - 2014-09-18 to 2014-09-19 -",
      total_families,
      "families"
    ),
    x = 0,
    hjust = -0.35,
    size = 13
  ) +
  theme(plot.margin = margin(0, 0, 0, 0))
save_plot(
  "out/figs/test_dry_season_stats.png",
  plot_grid(
    title,
    grid,
    ncol = 1,
    rel_heights = c(0.1, 1),
    greedy = F
  ),
  base_width = 7.5,
  base_height = 4.5,
  dpi = 300,
  bg = "transparent"
)

# Trajectories
plt <- ggplot(geoms) +
  # geom_sf(
  #   data = cities,
  #   fill = NA,
  #   size = 0.4,
  #   color = "gray"
  # ) +
  geom_sf(
    data = rivers,
    fill = NA,
    size = 0.4,
    color = "lightblue"
  ) +
  geom_rect(
    aes(
      xmin = -61.343496,
      xmax = -58.640505,
      ymin = -4.505793,
      ymax = -1.792021
    ),
    fill = NA,
    color = "black"
  ) +
  geom_sf(
    data = geoms,
    aes(color = name),
    fill = NA,
    size = 0.5,
    show.legend = F
  ) +
  # geom_sf(
  #   data = centroids,
  #   aes(color = name),
  #   fill = NA,
  #   show.legend = F
  # ) +
  coord_sf(
    xlim = c(-61.343496, -58.640505),
    ylim = c(-4.505793, -1.792021),
    expand = T
  ) +
  facet_wrap(vars(as.character(timestamp)), nrow = 5)
ggsave(
  plot = plt,
  filename = "out/figs/test_dry_season_overlap.png",
  width = 18,
  height = 10,
  dpi = 300
)

# New centroids + geoms + GoAmazon sites
ggplot() +
  geom_sf(
    data = cities,
    fill = NA,
    size = 0.4,
    color = "gray"
  ) +
  geom_sf(
    data = rivers,
    fill = NA,
    size = 0.4,
    color = "lightblue"
  ) +
  geom_rect(
    aes(
      xmin = -61.343496,
      xmax = -58.640505,
      ymin = -4.505793,
      ymax = -1.792021
    ),
    fill = NA,
    color = "black"
  ) +
  geom_path(data = goam_circles_50 %>% filter(id == "T3"),
            aes(lon, lat, group = id, color = id),
            size = 0.5) +
  geom_sf(
    data = geoms %>% filter(event == "SPONTANEOUS\nGENERATION"),
    fill = NA,
    size = 0.5,
    show.legend = F
  ) +
  # geom_sf(
  #   data = centroids %>% filter(event == "SPONTANEOUS\nGENERATION"),
  #   color = "darkblue",
  #   fill = NA,
  #   show.legend = F
  # ) +
  geom_point(
    data = goam_sites %>% filter(goamazon_reference == "T3"),
    aes(longitude, latitude, color = goamazon_reference),
    shape = 16,
    size = 2,
  ) +
  # geom_text(
  #   data = goam_sites,
  #   aes(longitude, latitude, label = goamazon_reference, color = goamazon_reference),
  #   position = position_dodge2(0.2, preserve = "single"),
  #   vjust = -0.5,
  #   size = 3.5
  # ) +
  coord_sf(
    xlim = c(-61.343496,-58.640505),
    ylim = c(-4.505793,-1.792021),
    expand = T
  ) +
  labs(
    x = NULL,
    y = NULL,
    title = "Spontaneous Generation - Dry Season Test",
    color = "Site"
  ) +
  theme(legend.position = "bottom") +
  guides(color = guide_legend(nrow = 1))
ggsave(
  "out/figs/test_dry_season_new_systems.png",
  width = 4.5,
  height = 5,
  dpi = 300,
  bg = "transparent"
)

#-------------------------------------------------------------------------------
# DRY-TO-WET SEASON
# Reading data
test_drytowet_season <-
  read_csv("out/test_drytowet_season_filter.csv",
           col_types = cols(timestamp = col_datetime(format = "%Y-%m-%d %H:%M:%S"))) %>%
  mutate(count = count, event = str_replace(event, "_", "\n")) # count * 4 for 2-km res
test_drytowet_season_sums <- test_drytowet_season %>%
  group_by(name) %>%
  summarize(duration = difftime(max(timestamp), min(timestamp), units = "mins"),
            max_area = max(count))
total_families <- test_drytowet_season %>%
  group_by(name) %>%
  summarise() %>%
  count() %>%
  unlist()

# Converting strings to geometries
centroids <- st_as_sf(test_drytowet_season, wkt = "centroid")
geoms <- st_as_sf(test_drytowet_season, wkt = "geom")
st_crs(centroids) <- 4326
st_crs(geoms) <- 4326

# Plotting distributions
plots <- list()
# Size (count)
plots[[1]] <- ggplot(test_drytowet_season) +
  geom_histogram(
    aes(x = count, fill = "All"),
    binwidth = 1000,
    color = NA,
    alpha = 0.5
  ) +
  geom_histogram(
    data = test_drytowet_season_sums,
    aes(x = max_area, fill = "Max"),
    binwidth = 1000,
    color = NA,
    alpha = 0.5
  ) +
  labs(x = "Area (km²)", y = "Count", fill = NULL) +
  scale_fill_manual(values = c("black", "red")) +
  theme(legend.position = c(0.85, 0.85),
        legend.background = element_blank())
# Duration
plots[[2]] <- ggplot(test_drytowet_season_sums) +
  geom_histogram(aes(x = duration), binwidth = 12) +
  labs(x = "Duration (min)", y = "Count")
# Max, Mean Z
plots[[3]] <- ggplot(test_drytowet_season) +
  geom_histogram(
    aes(x = max, fill = "Max"),
    binwidth = 1,
    color = NA,
    alpha = 0.5
  ) +
  geom_histogram(
    aes(x = mean, fill = "Mean"),
    binwidth = 1,
    color = NA,
    alpha = 0.5
  ) +
  labs(x = "Reflectivity (dBZ)", y = "Count", fill = NULL) +
  scale_fill_manual(values = c("red", "darkgreen")) +
  theme(legend.position = c(0.85, 0.85),
        legend.background = element_blank())
# Type of event
plots[[4]] <- ggplot(test_drytowet_season) +
  geom_histogram(aes(x = event), stat = "count") +
  stat_count(
    aes(x = event, y = ..count.., label = ..count..),
    geom = "text",
    vjust = -0.5,
    size = 3
  ) +
  labs(x = "Type", y = "Count") +
  scale_y_continuous(expand = expansion(mult = c(0, .15)))
# Joining filter plots
grid <- plot_grid(plotlist = plots, labels = "auto")
title <- ggdraw() +
  draw_label(
    paste(
      "Dry-to-Wet Season Test - 2014-11-20 to 2014-11-21 -",
      total_families,
      "families"
    ),
    x = 0,
    hjust = -0.25,
    size = 13
  ) +
  theme(plot.margin = margin(0, 0, 0, 0))
save_plot(
  "out/figs/test_drytowet_season_stats.png",
  plot_grid(
    title,
    grid,
    ncol = 1,
    rel_heights = c(0.1, 1),
    greedy = F
  ),
  base_width = 7.5,
  base_height = 4.5,
  dpi = 300,
  bg = "transparent"
)

# Trajectories
plt <- ggplot(geoms) +
  # geom_sf(
  #   data = cities,
  #   fill = NA,
  #   size = 0.4,
  #   color = "gray"
  # ) +
  geom_sf(
    data = rivers,
    fill = NA,
    size = 0.4,
    color = "lightblue"
  ) +
  geom_rect(
    aes(
      xmin = -61.343496,
      xmax = -58.640505,
      ymin = -4.505793,
      ymax = -1.792021
    ),
    fill = NA,
    color = "black"
  ) +
  geom_sf(
    data = geoms,
    aes(color = name),
    fill = NA,
    size = 0.5,
    show.legend = F
  ) +
  # geom_sf(
  #   data = centroids,
  #   aes(color = name),
  #   fill = NA,
  #   show.legend = F
  # ) +
  coord_sf(
    xlim = c(-61.343496, -58.640505),
    ylim = c(-4.505793, -1.792021),
    expand = T
  ) +
  facet_wrap(vars(as.character(timestamp)), nrow = 5)
ggsave(
  plot = plt,
  filename = "out/figs/test_drytowet_season_overlap.png",
  width = 18,
  height = 10,
  dpi = 300
)

# New centroids + geoms + GoAmazon sites
ggplot() +
  geom_sf(
    data = cities,
    fill = NA,
    size = 0.4,
    color = "gray"
  ) +
  geom_sf(
    data = rivers,
    fill = NA,
    size = 0.4,
    color = "lightblue"
  ) +
  geom_rect(
    aes(
      xmin = -61.343496,
      xmax = -58.640505,
      ymin = -4.505793,
      ymax = -1.792021
    ),
    fill = NA,
    color = "black"
  ) +
  geom_path(data = goam_circles_50 %>% filter(id == "T3"),
            aes(lon, lat, group = id, color = id),
            size = 0.5) +
  geom_sf(
    data = geoms %>% filter(event == "SPONTANEOUS\nGENERATION"),
    fill = NA,
    size = 0.5,
    show.legend = F
  ) +
  # geom_sf(
  #   data = centroids %>% filter(event == "SPONTANEOUS\nGENERATION"),
  #   color = "darkblue",
  #   fill = NA,
  #   show.legend = F
  # ) +
  geom_point(
    data = goam_sites %>% filter(goamazon_reference == "T3"),
    aes(longitude, latitude, color = goamazon_reference),
    shape = 16,
    size = 2,
  ) +
  # geom_text(
  #   data = goam_sites,
  #   aes(longitude, latitude, label = goamazon_reference, color = goamazon_reference),
  #   position = position_dodge2(0.2, preserve = "single"),
  #   vjust = -0.5,
  #   size = 3.5
  # ) +
  coord_sf(
    xlim = c(-61.343496,-58.640505),
    ylim = c(-4.505793,-1.792021),
    expand = T
  ) +
  labs(
    x = NULL,
    y = NULL,
    title = "Spontaneous Generation - Dry-to-Wet Season Test",
    color = "Site"
  ) +
  theme(legend.position = "bottom") +
  guides(color = guide_legend(nrow = 1))
ggsave(
  "out/figs/test_drytowet_season_new_systems.png",
  width = 4.5,
  height = 5,
  dpi = 300,
  bg = "transparent"
)

#-------------------------------------------------------------------------------
# WET SEASON
# Reading data
test_wet_season <- read_csv("out/test_wet_season_filter.csv",
                            col_types = cols(timestamp = col_datetime(format = "%Y-%m-%d %H:%M:%S"))) %>%
  mutate(count = count, event = str_replace(event, "_", "\n")) # count * 4 for 2-km res
test_wet_season_sums <- test_wet_season %>%
  group_by(name) %>%
  summarize(duration = difftime(max(timestamp), min(timestamp), units = "mins"),
            max_area = max(count))
total_families <- test_wet_season %>%
  group_by(name) %>%
  summarise() %>%
  count() %>%
  unlist()

# Converting strings to geometries
centroids <- st_as_sf(test_wet_season, wkt = "centroid")
geoms <- st_as_sf(test_wet_season, wkt = "geom")
st_crs(centroids) <- 4326
st_crs(geoms) <- 4326

# Plotting distributions
plots <- list()
# Size (count)
plots[[1]] <- ggplot(test_wet_season) +
  geom_histogram(
    aes(x = count, fill = "All"),
    binwidth = 100,
    color = NA,
    alpha = 0.5
  ) +
  geom_histogram(
    data = test_wet_season_sums,
    aes(x = max_area, fill = "Max"),
    binwidth = 100,
    color = NA,
    alpha = 0.5
  ) +
  labs(x = "Area (km²)", y = "Count", fill = NULL) +
  scale_fill_manual(values = c("black", "red")) +
  theme(legend.position = c(0.85, 0.85),
        legend.background = element_blank())
# Duration
plots[[2]] <- ggplot(test_wet_season_sums) +
  geom_histogram(aes(x = duration), binwidth = 12) +
  labs(x = "Duration (min)", y = "Count")
# Max, Mean Z
plots[[3]] <- ggplot(test_wet_season) +
  geom_histogram(
    aes(x = max, fill = "Max"),
    binwidth = 1,
    color = NA,
    alpha = 0.5
  ) +
  geom_histogram(
    aes(x = mean, fill = "Mean"),
    binwidth = 1,
    color = NA,
    alpha = 0.5
  ) +
  labs(x = "Reflectivity (dBZ)", y = "Count", fill = NULL) +
  scale_fill_manual(values = c("red", "darkgreen")) +
  theme(legend.position = c(0.85, 0.85),
        legend.background = element_blank())
# Type of event
plots[[4]] <- ggplot(test_wet_season) +
  geom_histogram(aes(x = event), stat = "count") +
  stat_count(
    aes(x = event, y = ..count.., label = ..count..),
    geom = "text",
    vjust = -0.5,
    size = 3
  ) +
  labs(x = "Type", y = "Count") +
  scale_y_continuous(expand = expansion(mult = c(0, .15)))
# Joining filter plots
grid <- plot_grid(plotlist = plots, labels = "auto")
title <- ggdraw() +
  draw_label(
    paste(
      "Wet Season Test - 2015-04-03 to 2015-04-04 -",
      total_families,
      "families"
    ),
    x = 0,
    hjust = -0.25,
    size = 13
  ) +
  theme(plot.margin = margin(0, 0, 0, 0))
save_plot(
  "out/figs/test_wet_season_stats.png",
  plot_grid(
    title,
    grid,
    ncol = 1,
    rel_heights = c(0.1, 1),
    greedy = F
  ),
  base_width = 7.5,
  base_height = 4.5,
  dpi = 300,
  bg = "transparent"
)

# Trajectories
plt <- ggplot(geoms) +
  # geom_sf(
  #   data = cities,
  #   fill = NA,
  #   size = 0.4,
  #   color = "gray"
  # ) +
  geom_sf(
    data = rivers,
    fill = NA,
    size = 0.4,
    color = "lightblue"
  ) +
  geom_rect(
    aes(
      xmin = -61.343496,
      xmax = -58.640505,
      ymin = -4.505793,
      ymax = -1.792021
    ),
    fill = NA,
    color = "black"
  ) +
  geom_sf(
    data = geoms,
    aes(color = name),
    fill = NA,
    size = 0.5,
    show.legend = F
  ) +
  # geom_sf(
  #   data = centroids,
  #   aes(color = name),
  #   fill = NA,
  #   show.legend = F
  # ) +
  coord_sf(
    xlim = c(-61.343496, -58.640505),
    ylim = c(-4.505793, -1.792021),
    expand = T
  ) +
  facet_wrap(vars(as.character(timestamp)), nrow = 5)
ggsave(
  plot = plt,
  filename = "out/figs/test_wet_season_overlap.png",
  width = 18,
  height = 10,
  dpi = 300
)


# New centroids + geoms + GoAmazon sites
ggplot() +
  geom_sf(
    data = cities,
    fill = NA,
    size = 0.4,
    color = "gray"
  ) +
  geom_sf(
    data = rivers,
    fill = NA,
    size = 0.4,
    color = "lightblue"
  ) +
  geom_rect(
    aes(
      xmin = -61.343496,
      xmax = -58.640505,
      ymin = -4.505793,
      ymax = -1.792021
    ),
    fill = NA,
    color = "black"
  ) +
  geom_path(data = goam_circles_50 %>% filter(id == "T3"),
            aes(lon, lat, group = id, color = id),
            size = 0.5) +
  geom_sf(
    data = geoms %>% filter(event == "SPONTANEOUS\nGENERATION"),
    fill = NA,
    size = 0.5,
    show.legend = F
  ) +
  # geom_sf(
  #   data = centroids %>% filter(event == "SPONTANEOUS\nGENERATION"),
  #   color = "darkblue",
  #   fill = NA,
  #   show.legend = F
  # ) +
  geom_point(
    data = goam_sites %>% filter(goamazon_reference == "T3"),
    aes(longitude, latitude, color = goamazon_reference),
    shape = 16,
    size = 2,
  ) +
  # geom_text(
  #   data = goam_sites,
  #   aes(longitude, latitude, label = goamazon_reference, color = goamazon_reference),
  #   position = position_dodge2(0.2, preserve = "single"),
  #   vjust = -0.5,
  #   size = 3.5
  # ) +
  coord_sf(
    xlim = c(-61.343496,-58.640505),
    ylim = c(-4.505793,-1.792021),
    expand = T
  ) +
  labs(
    x = NULL,
    y = NULL,
    title = "Spontaneous Generation - Wet Season Test",
    color = "Site"
  ) +
  theme(legend.position = "bottom") +
  guides(color = guide_legend(nrow = 1))
ggsave(
  "out/figs/test_wet_season_new_systems.png",
  width = 4.5,
  height = 5,
  dpi = 300,
  bg = "transparent"
)

