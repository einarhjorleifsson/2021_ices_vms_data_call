library(raster)
library(lubridate)
library(tidyverse)
library(viridis)
library(leaflet)
library(mapview)

YEARS <- 2020:2009

# some checks
LGSc <- 
  read_rds("data/LGS_collapsed.rds") %>% 
  # fix upstream, or better still delete upstream
  mutate(month = ifelse(is.na(month), month(date), month))
table(!is.na(LGSc$date))
table(LGSc$year, useNA = "ifany")
table(LGSc$month, useNA = "ifany")
table(LGSc$dcf4, useNA = "ifany")
table(LGSc$dcf5, useNA = "ifany")
table(LGSc$dcf6, useNA = "ifany")
table(LGSc$length_class, useNA = "ifany")
table(!is.na(LGSc$total))


# ------------------------------------------------------------------------------
# Annex 2 - logbooks

# really do not understand this anonymized vid stuff - ignored in the process 
#  futher downstream
twovessels <-
  LGSc %>%
  select(year, month, ices, dcf4, dcf5, dcf6, length_class, vid0) %>%
  distinct() %>%
  group_by(year, month, ices, dcf4, dcf6, length_class) %>%
  mutate(n_vessel = n_distinct(vid0)) %>%
  ungroup() %>%
  filter(n_vessel %in% 1:2) %>%
  group_by(year, month, ices, dcf4, dcf6, length_class) %>%
  mutate(vids = case_when(n_vessel == 1 ~ vid0,
                          n_vessel == 2 ~ paste0(vid0, ";", lead(vid0)))) %>%
  slice(1) %>%
  ungroup() %>% 
  select(-vid0)

annex2 <-
  LGSc %>%
  # First for each vessel
  group_by(year, month, ices, dcf4, dcf5, dcf6, length_class, vid) %>%
  summarise(FishingDays = n_distinct(date),
            kwdays = sum(FishingDays * kw),
            catch = sum(total),
            .groups = "drop") %>%
  # then just summarise over vessels
  group_by(year, month, ices, dcf4, dcf5, dcf6, length_class) %>%
  summarise(n_vessel = n_distinct(vid),
            FishingDays = sum(FishingDays),
            kwdays = sum(kwdays),
            catch = sum(catch),
            .groups = "drop") %>%
  ungroup() %>% 
  #left_join(twovessels) %>%
  left_join(twovessels) %>% 
  mutate(type = "LE",
         country = "IS",
         vms = "Y",
         value = NA_real_,
         lowermeshsize = NA,
         uppermeshsize = NA) %>% 
  # CHECK with datacall where lower/uppermeshsize is in the order of things
  select(type, country, year, month, n_vessel, vids, ices, dcf4, dcf5, 
         lowermeshsize, uppermeshsize,
         dcf6, length_class,
         vms, FishingDays, kwdays, catch, value)

# some double testing with orginal data
LGS <- read_rds("data/LGS_corrected.rds")
# LGS %>% 
#   group_by(year) %>% 
#   summarise(total.raw = sum(total) / 1000) %>% 
#   full_join(annex2 %>% 
#               group_by(year) %>% 
#               summarise(total.a2 = sum(catch) / 1000)) %>% 
#   gather(var, val, -year) %>% 
#   ggplot(aes(year, val, colour = var)) +
#   geom_point()

# NOTE: corrections done here in relation to uploading.
#       SHOULD BE FIXED UPSTREAM
# NOTE: Why so few dredges
annex2 %>% 
  count(dcf4)


library(sf)
sq <- read_sf("ftp://ftp.hafro.is/pub/data/shapes/ices_rectangles.gpkg")
#plot(sq["ecoregion"])

annex2 <- 
  annex2 %>% 
  filter(ices %in% sq$icesname) %>% 
  mutate(dcf4 = ifelse(dcf4 == "GSN", "GNS", dcf4),
         dcf5 = str_sub(dcf6, 5, 7),
         length_class = case_when(length_class == "<8" ~ "A",
                                  length_class == "08-10" ~ "B",
                                  length_class == "10-12" ~ "C",
                                  length_class == "12-15" ~ "D",
                                  length_class == ">=15" ~ "E"))

annex2 <- 
  annex2 %>% 
  mutate(vids = ifelse(is.na(vids), "-9", vids))
#annex2 %>%
# write_csv("delivery/iceland_annex2_2009_2020_2021-05-05_01.csv",
#           na = "",
#           col_names = FALSE)

# end: Annex 2 - logbooks
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Annex 1 - ais/vms
speed.criterion <-
  tribble(~gid,  ~s1,  ~s2,
          -199, 1.000, 3.000,
          1,    0.375, 2.750,
          2,    0.125, 2.500,
          3,    0.025, 2.000,
          5,    0.250, 3.250,
          6,    2.625, 5.500,
          7,    2.625, 6.000,
          9,    2.375, 4.000,
          12,    0.050, 2.250,
          14,    1.750, 3.250,
          15,    0.500, 5.500,
          38,    0.250, 4.750,
          40,    0.250, 6.000)
fil <- paste0("data/is_vms_visir2", YEARS, ".rds")

res <- list()

for(y in 1:length(YEARS)) {
  
  print(YEARS[y])
  
  res[[y]] <-
    read_rds(fil[y]) %>%
    # do not include the extrapolated data points
    filter(vms) %>% 
    left_join(speed.criterion,
              by = "gid") %>% 
    filter(speed >= s1, speed <= s2)
  
}
ais <- 
  bind_rows(res) %>% 
  # don't think i need gear here
  select(vid, visir:speed) %>% 
  group_by(visir) %>%
  # each ping is a minute
  mutate(n.pings = n()) %>%
  ungroup()
n0.ais <- nrow(ais)
rm(res)

# comparison of visir
LGS <- 
  read_rds("data/LGS_collapsed.rds") %>% 
  mutate(dcf4 = ifelse(dcf4 == "GSN", "GNS", dcf4),
         dcf5 = str_sub(dcf6, 5, 7),
         length_class = case_when(length_class == "<8" ~ "A",
                                  length_class == "08-10" ~ "B",
                                  length_class == "10-12" ~ "C",
                                  length_class == "12-15" ~ "D",
                                  length_class == ">=15" ~ "E"))
# challenge
# d <-
#   ais %>%
#   mutate(year = year(time)) %>%
#   group_by(year) %>%
#   summarise(nvisir.ais = n_distinct(visir)) %>%
#   full_join(LGS %>%
#               group_by(year) %>%
#               summarise(nvisir.lgs = n_distinct(visir)))
# d %>% 
#   mutate(p = round(nvisir.ais / nvisir.lgs, 2)) %>% 
#   knitr::kable()
ais <- 
  ais %>% 
  left_join(LGS %>%
              select(visir, vid0, gid, catch = total, gear.width, length, length_class, kw, dcf4, dcf5, dcf6)) %>%
  mutate(catch = catch / n.pings,
         #towtime = towtime / n.pings,
         csquare = vmstools::CSquare(lon, lat, 0.05))
n1.ais_lgs.merged <- nrow(ais)
print(c(n0.ais, n1.ais_lgs.merged))

# CHECKS
# ais %>% 
#   mutate(has.gear.width = !is.na(gear.width)) %>% 
#   count(dcf4, has.gear.width) %>% 
#   knitr::kable()
# range(ais$lon)
# hist(ais$lon)
# range(ais$lat)
# hist(ais$lat)
# m <- ggplot2::map_data("world",
#                        xlim = range(ais$lon) * c(0.9, 1.1),
#                        ylim = range(ais$lat) * c(0.9, 1.1))
# ais %>% 
#   mutate(lon = round(lon),
#          lat = round(lat)) %>% 
#   count(lon, lat) %>% 
#   ggplot() +
#   geom_polygon(data = m, aes(long, lat, group = group), fill = "grey") +
#   geom_tile(aes(lon, lat, fill = n)) +
#   coord_quickmap(xlim = range(ais$lon),
#                  ylim = range(ais$lat))
ais <- 
  ais %>% 
  filter(lat > 48) %>%
  # get rid of points on greenland
  filter(!(lat > 69.5 & lon < -19)) %>% 
  # get rid of points in sweden & finland 
  filter(!(lon > 18.5 & lat <= 69)) %>% 
  # get rid of points in norway
  filter(!(lon >= 5 & lat <= 66)) %>% 
  mutate(year = year(time),
         month = month(time))
n2.ais.filtered <- nrow(ais)
print(c(n0.ais, n2.ais.filtered))
# ais %>% 
#   mutate(lon = round(lon),
#          lat = round(lat)) %>% 
#   count(lon, lat) %>% 
#   ggplot() +
#   geom_polygon(data = m, aes(long, lat, group = group), fill = "grey") +
#   geom_tile(aes(lon, lat, fill = n)) +
#   coord_quickmap(xlim = range(ais$lon),
#                  ylim = range(ais$lat))
ais %>% write_rds("data/is_vms_2009-2020-speed_filter_lgs-merged.rds")
twovessels <-
  ais %>%
  select(year, month, csquare, dcf4, dcf5, dcf6, length_class, vid0) %>%
  distinct() %>%
  group_by(year, month, csquare, dcf4, dcf5, dcf6, length_class) %>%
  mutate(n_vessel = n_distinct(vid0)) %>%
  ungroup() %>%
  filter(n_vessel %in% 1:2) %>%
  group_by(year, month, csquare, dcf4, dcf6, length_class) %>%
  mutate(vids = case_when(n_vessel == 1 ~ vid0,
                          n_vessel == 2 ~ paste0(vid0, ";", lead(vid0)))) %>%
  slice(1) %>%
  ungroup() %>% 
  select(-vid0)
annex1 <-
  ais %>%
  group_by(year, month, csquare, dcf4, dcf5, dcf6, length_class) %>%
  summarise(speed  = mean(speed),
            time   = sum(n.pings) / 60,  # hours
            length = mean(length),
            kw     = mean(kw),
            kwh    = kw * time,
            catch  = sum(catch),
            spread = mean(gear.width),
            n_vessel = n_distinct(vid),
            .groups = "drop")
n.annex1 <- nrow(annex1)
annex1 <- 
  annex1 %>% 
  left_join(twovessels) %>% 
  mutate(type = "VE",
         country = "IS",
         value = NA,
         lowermeshsize = NA,
         uppermeshsize = NA) %>% 
  select(type, country, year, month, n_vessel, vids, csquare,
         dcf4, dcf5, lowermeshsize, uppermeshsize, dcf6,
         length_class, 
         speed, time, length, kw, kwh, catch, value, spread)
print(c(n.annex1, nrow(annex1)))
# error from ICES on the first and the last record of the slice below:
annex1 %>% 
  select(type:csquare) %>% 
  mutate(record_line = 1:n()) %>% 
  select(record_line, everything()) %>% 
  slice(456:459) %>% 
  knitr::kable()


annex1 <- 
  annex1 %>% 
  mutate(vids = ifelse(is.na(vids), "-9", vids))

annex1 %>% 
  write_csv("delivery/iceland_annex1_2009_2020_2021-05-05_01.csv",
            na = "", 
            col_names = FALSE)


# # CHECKS
# annex1 %>% count(type)
# annex1 %>% count(country)
# annex1 %>% count(year)
# annex1 %>% count(month)
# annex1 %>% count(n_vessel)
# table(!is.na(annex1$n_vessel))
# table(!is.na(annex1$vids))
# table(!is.na(annex1$csquare))
# annex1 %>% count(dcf4)
# annex1 %>% count(dcf5)
# annex1 %>% count(dcf6)
# annex1 %>% count(length_class)
# table(!is.na(annex1$speed))
# table(!is.na(annex1$time))
# table(!is.na(annex1$length))
# table(!is.na(annex1$kw))
# table(!is.na(annex1$kwh))
# table(!is.na(annex1$catch))
# table(!is.na(annex1$spread))
# 
# 
# 
# 
# # CLEAN STUFF BELOW
# 
# 
# 
# 
# 
# 
# 
# 
# # some checks
# tmp <- 
#   read_rds(fil[5]) 
# 
# 
# 
# filter(!is.na(gid)) %>% 
#   left_join(speed.criterion) %>% 
#   filter(speed >= s1,
#          speed <= s2) %>% 
#   # drop_extrapolations
#   filter(vms)
# table(!is.na(tmp$visir))
# 
# # ------------------------------------------------------------------------------
# # leaflet test
# inf <- inferno(12, alpha = 1, begin = 0, end = 1, direction = -1)
# Q <- 0.9995
# rasterize_vms <- function(vms, GID, dx = 0.0013) {
#   
#   vmsr <-
#     vms %>%
#     filter(gid %in% GID) %>%
#     mutate(lon = gisland::grade(lon, dx),
#            lat = gisland::grade(lat, dx/2)) %>%
#     group_by(lon, lat) %>%
#     summarise(no.pings = n(),
#               .groups = "drop") %>%
#     filter(no.pings != 0,
#            !is.na(no.pings)) %>%
#     mutate(no.pings = ifelse(no.pings > quantile(no.pings, p = Q),
#                              quantile(no.pings, p = Q),
#                              no.pings)) %>%
#     #mutate(no.pings = ifelse(no.pings > 40, 40, no.pings)) %>%
#     raster::rasterFromXYZ()
#   raster::crs(vmsr) <- sp::CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")
#   return(vmsr)
# }
# #r <- rasterize_vms(tmp2, GID = 1, dx = 0.0013)
# r <- rasterize_vms(tmp, GID = 7, dx = 0.05)
# pal <- colorNumeric(inf, raster::values(r), na.color = "transparent")
# 
# leaflet(options = leafletOptions(minZoom = 4, maxZoom = 11)) %>%
#   addTiles(urlTemplate = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
#            group = "Image",
#            attribution = 'Data source: <a href="https://www.hafogvatn.is">Marine Rearch Institute</a>') %>%
#   addRasterImage(r, colors = pal, opacity = 1, group = "Fiskibotnvarpa",
#                  maxBytes = Inf)
# # end: leaflet test
# # ------------------------------------------------------------------------------
# 
# 
# 
# LGS2 %>% 
#   left_join(LGS)
# lgs <-
#   read_rds("dataIS/is_lb_wide.rds") %>%
#   filter(gid %in% c(6, 9, 14, 15, 38, 40),
#          year %in% YEARS)
# IDS <-
#   lgs %>%
#   pull(visir)
# 
# fil <- paste0("data/is_vms_visir", YEARS, ".rds")
# 
# res <- list()
# 
# for(y in 1:length(YEARS)) {
#   
#   print(YEARS[y])
#   
#   res[[y]] <-
#     read_rds(fil[y]) %>%
#     filter(!is.na(mobileid),
#            visir %in% IDS,
#            between(speed, 1, 5.5))
#   
# }
# 
# vms <-
#   bind_rows(res) %>%
#   group_by(visir) %>%
#   mutate(n.pings = n()) %>%
#   ungroup() %>%
#   mutate(year = year(time),
#          month = month(time)) %>%
#   left_join(lgs %>%
#               select(visir, gid, towtime = effort, gear.width, catch, dcf4:grt)) %>%
#   mutate(catch = catch / n.pings,
#          towtime = towtime / n.pings,
#          csquare = vmstools::CSquare(lon, lat, 0.05))
# 
# # get rid of noise
# i <- vms$lon < 0 & vms$lat > 70
# vms <- vms[!i, ]
# i <- vms$lon > 0 & vms$lat < 65
# vms <- vms[!i, ]
# i <- vms$lat < 57.5
# vms <- vms[!i, ]
# 
# # not really needed
# #xxx <- data.frame(SI_LONG = vms$lon,
# #                  SI_LATI = vms$lat)
# #vms$ices <- vmstools::ICESrectangle(xxx)
# 
# # where door spread not available use the median from the metier
# vms <-
#   vms %>%
#   group_by(dcf6) %>%
#   mutate(gear.width = ifelse(!is.na(gear.width), gear.width, median(gear.width, na.rm = TRUE))) %>%
#   ungroup()
# 
# write_rds(vms, "dataIS/is_vms_summary.rds")
# 
# anonymize.vid <- read_rds("dataIS/anonymize_vid.rds")
# twovessels <-
#   vms %>%
#   mutate(type = "VE",
#          country = "ICE") %>%
#   left_join(anonymize.vid) %>%
#   select(type, country, year, month, csquare, dcf4, dcf6, vessel_length_class, gid, vid0) %>%
#   distinct() %>%
#   group_by(type, country, year, month, csquare, dcf4, dcf6, vessel_length_class, gid) %>%
#   mutate(n.vessels = n_distinct(vid0)) %>%
#   ungroup() %>%
#   filter(n.vessels %in% 1:2) %>%
#   group_by(type, country, year, month, csquare, dcf4, dcf6, vessel_length_class, gid) %>%
#   mutate(vids = case_when(n.vessels == 1 ~ vid0,
#                           n.vessels == 2 ~ paste0(vid0, ";", lead(vid0)))) %>%
#   slice(1) %>%
#   ungroup()
# 
# 
# annex1 <-
#   vms %>%
#   group_by(year, month, csquare, vessel_length_class, dcf4, dcf6) %>%
#   summarise(speed  = mean(speed, na.rm = TRUE),
#             time   = sum(towtime, na.rm = TRUE),
#             length = mean(length, na.rm = TRUE),
#             kw     = mean(kw, na.rm = TRUE),
#             kwh    = kw * time,
#             catch  = sum(catch, na.rm = TRUE),
#             spread = mean(gear.width, na.rm = TRUE),
#             n_vessel = n_distinct(vid)) %>%
#   ungroup() %>%
#   mutate(type = "VE",
#          country = "ICE",
#          value = NA) %>%
#   left_join(twovessels) %>%
#   select(type, country, year, month, n_vessel, vids, csquare,
#          vessel_length_class, dcf4, dcf6,
#          speed, time, length, kw, kwh, catch, value, spread)
# 
# write_csv(annex1, "delivery/iceland_annex1_2009_2020.csv", col_names = FALSE)
