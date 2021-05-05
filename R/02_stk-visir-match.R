# ------------------------------------------------------------------------------
# run this as:
#  nohup R < R/02_stk-visir-match.R --vanilla > logs/02_stk-visir-match.log &

library(tidyverse)
library(lubridate)
library(mar)
con <- connect_mar()
YEARS <- 2020:2009


LB <- read_rds("data/LGS_collapsed.rds")
VID_MID <- 
  read_rds("data/VID_MID.rds") %>% 
  select(vid, mid) %>% 
  filter(!is.na(mid))

for(y in 1:length(YEARS)) {
  
  YEAR <- YEARS[y]
  print(YEAR)
  
  LB.year <-
    LB %>%
    filter(year %in% YEAR) %>% 
    select(vid, visir, gid, t1, t2) %>%
    pivot_longer(cols = c(t1, t2),
                 names_to = "startend",
                 values_to = "time") %>%
    arrange(vid, time) %>%
    mutate(year = year(time),
           time = round_date(time, "minutes"))
  
  vidmid.year <-
    VID_MID %>% 
    filter(vid %in% unique(LB.year$vid))
    
  print(paste("Number of unique vid:", length(unique(vidmid.year$vid))))
  print(paste("Number of unique mid:", length(unique(vidmid.year$mid))))
  
  MIDs <-
    vidmid.year %>%
    pull(mid) %>%
    unique()
  
  VMS <-
    vms(con, YEAR) %>%
    collect(n = Inf) %>% 
    filter(mobileid %in% MIDs) %>%
    select(mid = mobileid, time = date, lon, lat, speed) %>%
    collect(n = Inf) %>%
    filter(between(lon, -44, 68.50),
           between(lat,  36, 85.50)) %>%
    distinct() %>%
    mutate(vms = TRUE) %>%
    arrange(mid, time) %>%
    select(mid, time, lon, lat, speed, vms) %>% 
    mutate(time = round_date(time, "minutes"))
  
  # loop though each vessel, in some cases it has more than on mid
  VIDs <- 
    vidmid.year %>%
    pull(vid) %>%
    unique()
  
  res <- list()
  
  for(v in 1:length(VIDs)) {
    print(paste(YEAR, v, VIDs[v]))
    
    MIDforVID <- 
      vidmid.year %>% 
      filter(vid == VIDs[v]) %>% 
      pull(mid)
    
    VMS.vessel <-
      VMS %>%
      filter(mid %in% MIDforVID) %>%
      # HERE: get rid of wacky points via derived speed
      arrange(time) %>%
      #group_by(vid) %>%
      #mutate(dist = geo::arcdist(lead(lat), lead(lon), lat, lon),   # distance to next point
      #       time2 = as.numeric(lead(time) - time) / (60 * 60),     # duration to next point
      #       speed2 = dist/time2) %>%                               # speed on next "leg"
      #filter(speed2 <= 20 | is.na(speed2)) %>%
      #select(-c(time2, speed2, dist)) %>%
      # end of wacky
      # HERE: get rid of wacky points again via derived speed
      #arrange(vid, time) %>%
      #group_by(vid) %>%
      #mutate(dist = geo::arcdist(lead(lat), lead(lon), lat, lon),   # distance to next point
    #       time2 = as.numeric(lead(time) - time) / (60 * 60),     # duration to next point
    #       speed2 = dist/time2) %>%                               # speed on next "leg"
    #filter(speed2 <= 20 | is.na(speed2)) %>%
    #select(-c(time2, speed2, dist)) %>%
    # end of wacky
    mutate(time = round_date(time, "minutes"))
    
    LB.year.vid <-
      LB.year %>%
      filter(vid == VIDs[v])
    
    T1 <- max(min(LB.year.vid$time) - days(2), ymd_hms(paste0(YEAR, "-01-01 00:00:00")))
    T2 <- min(max(LB.year.vid$time) + days(2), ymd_hms(paste0(YEAR, "-12-31 23:59:00")))
    
    res[[v]] <-
      # dataframe of minutes over the whole year
      tibble(time = seq(T1,
                        T2,
                        by = "1 min")) %>%
      left_join(VMS.vessel, by = "time") %>%
      mutate(y = 1:n(),
             vid = VIDs[v])
    
    if(sum(!is.na(res[[v]]$lon)) > 1) {
      
      res[[v]] <-
        res[[v]] %>%
        mutate(lon = approx(y, lon, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
               lat = approx(y, lat, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
               speed = approx(y, speed, y, method = "linear", rule = 1, f = 0, ties = mean)$y) %>%
        select(-y) %>%
        bind_rows(LB.year.vid) %>%
        arrange(time) %>%
        mutate(x = if_else(startend == "t1", 1, 0, 0)) %>%
        mutate(x = case_when(startend == "t1" ~ 1,
                             startend == "t2" ~ -1,
                             TRUE ~ 0)) %>%
        mutate(x = cumsum(x)) %>%
        fill(visir) %>%
        mutate(visir = ifelse(x == 1 | startend == "t2", visir, NA_integer_)) %>%
        mutate(y = 1:n()) %>%
        mutate(lon = approx(y, lon, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
               lat = approx(y, lat, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
               speed = approx(y, speed, y, method = "linear", rule = 1, f = 0, ties = mean)$y) %>%
        select(vid, gid, visir, time, lon, lat, speed, mid, vms) %>% 
        # drop values that have no visir to save space
        filter(!is.na(visir)) %>% 
        fill(gid)
    }
    
  }
  bind_rows(res) %>%
    write_rds(paste0("data/is_vms_visir2", YEAR, ".rds"))
}
