library(tidyverse)
library(lubridate)
library(mar)
source("R/functions.R")
con <- connect_mar()
YEARS <- 2020:2009

fcon <- "data/VID_MID.log"
cat(rep("-", 80) %>% glue::glue_collapse(),
    file = fcon, append = FALSE)
cat_lh(NULL, file = fcon)
cat_lh("Matching vessel id (vid) and mobileid (mid)", file = fcon)
cat_lh(now(), file = fcon)
cat_lh(NULL, file = fcon)


# ------------------------------------------------------------------------------
# import corrected logbook data
#   NOTE: In theory one could just start with the mar:::lb_base data since
#         both sources should have the same vid
LGS <- read_rds("data/LGS_corrected.rds")
# summary statistics
summary.lgs <-
  LGS %>% 
  group_by(vid) %>% 
  summarise(n.lgs = n(),
            n.gid = n_distinct(gid),
            min.date = min(date),
            max.date = max(date)) %>% 
  # TODO: Move upstream
  # one record which has missing vid dropped
  drop_na()

# ------------------------------------------------------------------------------
# get mobileid for vid
vid.stk <-
  mar:::stk_mobile_icelandic(con, correct = TRUE, vidmatch = TRUE) %>% 
  collect(n = Inf)
MIDs <- 
  summary.lgs %>% 
  left_join(vid.stk) %>% 
  pull(mid) %>% 
  unique()
# unexpected
MIDs <- MIDs[!is.na(MIDs)]
mids1 <- MIDs[1:1000]
mids2 <- MIDs[1001:length(MIDs)]
summary.stk <-
  bind_rows(stk_trail(con) %>% 
              filter(mid %in% mids1,
                     time >= to_date("2009-01-01", "YYYY:MM:DD"),
                     time <  to_date("2020-12-31", "YYYY:MM:DD")) %>% 
              group_by(mid) %>% 
              summarise(n.stk = n(),
                        min.time = min(time, na.rm = TRUE),
                        max.time = max(time, na.rm = TRUE)) %>% 
              collect(n = Inf),
            stk_trail(con) %>% 
              filter(mid %in% mids2,
                     time >= to_date("2009-01-01", "YYYY:MM:DD"),
                     time <  to_date("2020-12-31", "YYYY:MM:DD")) %>% 
              group_by(mid) %>% 
              summarise(n.stk = n(),
                        min.time = min(time, na.rm = TRUE),
                        max.time = max(time, na.rm = TRUE)) %>% 
              collect(n = Inf))
print(c(nrow(summary.lgs), nrow(summary.stk)))

# NOTE: get here more records than in the original summary.*
d <- 
  summary.stk %>% 
  left_join(vid.stk %>% 
              select(mid, vid), by = "mid") %>% 
  full_join(summary.lgs, by = "vid") %>% 
  group_by(vid) %>% 
  mutate(n.mid = n()) %>% 
  ungroup() %>% 
  left_join(mar:::vessel_registry(con, standardize = TRUE) %>% 
              select(vid, name) %>% 
              collect(n = Inf)) %>% 
  select(vid, mid, name, n.lgs, n.stk, n.mid, everything())
cat_lh("Vessels with no matching mobileid", file = fcon)
d %>% 
  filter(is.na(mid)) %>% 
  select(vid, name, n.lgs, n.gid:max.date) %>% 
  knitr::kable() %>% 
  cat_lh(file = fcon)
cat_lh("Vessels with low logbook records", file = fcon)
d %>% 
  filter(!is.na(mid),
         n.lgs <= 10) %>% 
  arrange(n.lgs, -n.stk) %>% 
  knitr::kable() %>% 
  cat_lh(file = fcon)

# write out, do a separate analysis, see checks.Rmd
d %>% write_rds("data/VID_MID.rds")

cat_lh(NULL)
cat_lh(NULL)
devtools::session_info() %>% capture.output(file = fcon, append = TRUE)
