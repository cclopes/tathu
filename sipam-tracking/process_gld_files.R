library(sf)
library(tidyverse)
library(lubridate)

# Loading radar filenames to get timestamps
files_radar <- fs::dir_ls(path = "/data2/GOAMAZON/radar/sipam_manaus/cptec_cappi", 
                          glob = "*.dat.gz", recurse = T) %>% 
  str_extract("cappi_\\d+.dat.gz") %>% 
  str_extract("\\d+") %>% 
  parse_date_time("ymdHM")
dates_radar <- tibble(timestamp = files_radar) %>% 
  mutate(time_interval = interval(timestamp, timestamp + 12*60))

# Loading GLD data
files_gld <- fs::dir_ls(path = "/data2/GOAMAZON/lightning/GLD360/GLD360_txt_by_day", 
                        glob = "*.txt")
gld <- read_csv(files_gld, id = "path",
                col_types = cols(t = col_datetime(format = "%Y-%m-%d %H:%M:%S")))

# Creating new GLD files based on radar timestamps
for(j in 1:length(dates_radar$timestamp)){
  gld %>% 
    filter(t %within% dates_radar$time_interval[j]) %>% 
    mutate(t_radar = dates_radar$timestamp[j]) %>% 
    write_csv(paste0("/home/camilacl/git/amazon-storms-aerosols/data/lightning/GLD_mod/GLD360_mod_", format(dates_radar$timestamp[j], format="%Y%m%d%H%M"), ".csv"))
}


