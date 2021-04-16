# ------------------------------------------------------------------------------
# A single logbook munging to then be used downstream for stk match
# This script is based on merging the 2020 ices datacall script and that
# used in the 2019 anr-request. And then some
#
# The output has the same number of records as the input. Gears that are not
#  corrected and should not be used downstream "as is" are reorded as gid = -666
#
# TODO:
#      Higher resolution of dredge than just gid ==  15
#      Higher resolution of nets than just  gid == 2
#      Correct/standardize mesh size for gid == 2
#      Should ICES metier be set here or further downstream?
#      Should mobile id be allocated here or further downstream?  - downstream
#
# PROCESSING STEPS:
#  1. Get and merge logbook and landings data
#  2. Gear correction, stored in variable gid
#  3. Lump gears
#  4. Miscellaneous "corrections"
#  5. Set gear width proxy
#  6. Get rid of intermediary variables
#  7. gear class of corrected gid
#  8. Save file for downstream processing (data/LGS_corrected.rds)

YEARS <- 2020:2009

library(data.table)
#library(sf)
library(tidyverse)
library(lubridate)
library(mar)
source("R/functions.R")
#source("~/r/Pakkar2/omar/R/stk.R")
con <- connect_mar()

# ------------------------------------------------------------------------------
# 1. Get and merge logbook and landings data
tmp.lb.base <-
  mar:::lb_base(con) %>%
  filter(year %in% YEARS)
# catch is needed to establish target species when doing gear corrections
tmp.lb.catch <-
  tmp.lb.base %>%
  select(visir, gid) %>%
  left_join(mar:::lb_catch(con) %>%
              mutate(catch = catch / 1e3),
            by = "visir") %>%
  collect(n = Inf) %>% 
  arrange(visir, desc(catch), sid) %>%
  group_by(visir) %>%
  mutate(total = sum(catch),
         p = catch / total,
         n.sid = n()) %>%
  ungroup()
tmp.lb.mobile <-
  tmp.lb.base %>%
  inner_join(mar:::lb_mobile(con),
             by = c("visir")) %>%
  collect(n = Inf)
tmp.lb.static <-
  tmp.lb.base %>%
  inner_join(mar:::lb_static(con),
             by = "visir") %>%
  collect(n = Inf)
LGS <-
  bind_rows(tmp.lb.mobile, tmp.lb.static) %>%
  mutate(date = as_date(date),
         datel = as_date(datel),
         gid = as.integer(gid))
# used to double-check if and where we "loose" or for that matter accidentally
#  "add" data (may happen in joins) downstream
n0 <- nrow(LGS)
# get the gear from landings
tmp.ln.base <-
  mar:::ln_catch(con) %>%
  filter(!is.na(vid), !is.na(date)) %>%
  mutate(year = year(date)) %>%
  filter(year %in% YEARS) %>%
  select(vid, gid.ln = gid, datel = date) %>%
  distinct() %>%
  collect(n = Inf) %>%
  mutate(datel = as_date(datel),
         gid.ln = as.integer(gid.ln))
tmp.lb.ln.match <-
  LGS %>% 
  match_nearest_date(tmp.ln.base) %>% 
  select(visir, gid.ln) %>% 
  # just take the first match, i.e. ignore second landing within a day
  distinct(visir, .keep_all = TRUE)
LGS <- 
  LGS %>% 
  left_join(tmp.lb.ln.match,
            by = "visir")
print(c(n0, nrow(LGS)))
LGS <- 
  LGS %>%
  # the total catch and the "dominant" species
  left_join(tmp.lb.catch %>%
              group_by(visir) %>%
              slice(1) %>%
              ungroup() %>% 
              rename(sid.target = sid),
            by = c("visir", "gid")) %>% 
  rename(gid.lb = gid)
LGS <- 
  LGS %>% 
  select(visir, vid, gid.lb, gid.ln, sid.target, everything())
# rm(tmp.lb.base, tmp.lb.catch, tmp.lb.mobile, tmp.lb.static, tmp.ln.base, tmp.lb.ln.match)
# end: Get and merge logbook and landings data
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 2. Gear corrections
gears <-
  tbl_mar(con, "ops$einarhj.gear") %>% collect(n = Inf) %>%
  mutate(description = ifelse(gid == 92, "G.halibut net", description),
         gid = as.integer(gid),
         gclass = as.integer(gclass))
LGS <- 
  LGS %>% 
  left_join(gears %>% select(gid.lb = gid, gc.lb = gclass), by = "gid.lb") %>% 
  left_join(gears %>% select(gid.ln = gid, gc.ln = gclass), by = "gid.ln") %>% 
  select(visir:gid.ln, gc.lb, gc.ln, everything()) %>% 
  mutate(gid = NA_integer_,
         gid.source = NA_character_) %>% 
  mutate(i = gid.lb == gid.ln,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.lb=gid.ln", gid.source),
         step = ifelse(i, 1L, NA_integer_)) %>% 
  mutate(i = is.na(gid) & gc.lb == gc.ln,
         gid = ifelse(i, gc.lb, gid),
         gid.source = ifelse(i, "gc.lb=gc.ln   -> gid.lb", gid.source),
         step = ifelse(i, 2L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 15L & gid.lb %in% c(5L, 6L),
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=15, gid.lb=5,6   -> gid.ln", gid.source),
         step = ifelse(i, 3L, step)) %>% 
  mutate(i = is.na(gid) & 
           gid.ln == 21 & 
           gid.lb == 6 &
           !sid.target %in% c(30, 36, 41),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6, sid.target != 30,36,41   -> gid.lb",
                             gid.source),
         step = ifelse(i, 4L, step)) %>% 
  mutate(i = is.na(gid) &
           gid.ln == 21 &
           gid.lb == 6 & 
           sid.target %in% c(22, 41),
         gid = ifelse(i, 14, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6, sid.target = 22,41   -> 14", gid.source),
         step = ifelse(i, 5L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 6 & gid.lb == 7 & sid.target %in% c(11, 19, 30:36),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=6, gid.lb=7, sid.target = 11,19,30:36   -> gid.lb", gid.source),
         step = ifelse(i, 6L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 5,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=5   -> gid.ln", gid.source),
         step = ifelse(i, 7L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 40,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=40   -> gid.lb", gid.source),
         step = ifelse(i, 8L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 14 & gid.lb == 6 & sid.target == 41,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=14, gid.lb=6, sid.target = 41   -> gid.ln", gid.source),
         step = ifelse(i, 9L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 7 & gid.lb == 6,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=7, gid.lb=6   -> gid.ln", gid.source),
         step = ifelse(i, 10L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 9 & gid.lb == 6,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=9, gid.lb=6   -> gid.ln", gid.source),
         step = ifelse(i, 11L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 38,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=38   -> gid.ln", gid.source),
         step = ifelse(i, 12L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 6 & gid.lb == 14,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=6, gid.lb=14   -> gid.lb", gid.source),
         step = ifelse(i, 13L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 15 & gid.lb == 39,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=15, gid.lb=39   -> gid.lb", gid.source),
         step  = ifelse(i, 14L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 40 & gid.lb %in% c(5, 6),
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=40, gid.lb=5,6   -> gid.ln", gid.source),
         step = ifelse(i, 15L, step)) %>% 
  mutate(i = is.na(gid) & is.na(gid.ln) & gid.lb %in% c(1:3),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "is.na(gid.ln), gid.lb=1:3   -> gid.lb", gid.source),
         step = ifelse(i, 16L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 21 & gid.lb == 6,
         gid = ifelse(i, 7, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6   -> 7", gid.source),
         step = ifelse(i, 17L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 21 & gid.lb == 14,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=14   -> gid.lb", gid.source),
         step = ifelse(i, 18L, step))
print(c(n0, nrow(LGS)))
LGS %>% 
  count(step, gid, gid.lb, gid.ln, gid.source) %>% 
  mutate(p = round(n / sum(n) * 100, 3)) %>% 
  arrange(-n)
# percentage with no corrected gear
LGS %>% 
  mutate(missing = is.na(gid)) %>% 
  count(missing) %>% 
  mutate(p = n / sum(n) * 100)
# end: Gear corrections
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 3. Lump some gears
LGS <-
  LGS %>% 
  mutate(gid = case_when(gid %in% c(10, 12) ~ 4,   # purse seines
                         gid %in% c(18, 39) ~ 18,      # traps
                         TRUE ~ gid)) %>% 
  # make the rest a negative number
  mutate(gid = ifelse(is.na(gid), -666, gid)) %>% 
  # "skip" these also in downstream processing
  mutate(gid = ifelse(gid %in% c(4, 12, 42), -666, gid)) %>% 
  # lump dredges into one single gear
  mutate(gid = ifelse(gid %in% c(15, 37, 38, 40), 15, gid))
print(c(n0, nrow(LGS)))
# end: Lump some gears
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 4. Miscellaneous "corrections"
LGS <- 
  LGS %>% 
  # cap effort hours
  mutate(effort = case_when(effort > 12 & gid ==  6 ~ 12,
                            effort > 24 & gid ==  7 ~ 24,
                            effort > 15 & gid == 14 ~ 15,
                            TRUE ~ effort)) %>% 
  # Cap on the t2 so not overlapping with next setting
  #    NOTE: Effort not adjusted accordingly
  arrange(vid, t1) %>%
  group_by(vid) %>%
  mutate(overlap = if_else(t2 > lead(t1), TRUE, FALSE, NA),
         t22 = if_else(overlap,
                       lead(t1) - minutes(1), # need to subtract 1 minute but get the format right
                       t2,
                       as.POSIXct(NA)),
         t22 = ymd_hms(format(as.POSIXct(t22, origin="1970-01-01", tz="UTC"))),
         t2 = if_else(overlap & !is.na(t22), t22, t2, as.POSIXct(NA))) %>%
  ungroup() %>% 
  select(-t22) 

# Mesh size "corrections"
LGS <- 
  LGS %>% 
  # "correct" mesh size
  mutate(mesh = ifelse(gid == 7, mesh_min, mesh),
         mesh.std = case_when(gid ==  9 ~ 80,
                              gid %in% c(7, 10, 12, 14) ~ 40,
                              gid %in% c(5, 6) & (mesh <= 147 | is.na(mesh)) ~ 135,
                              gid %in% c(5, 6) &  mesh >  147 ~ 155,
                              gid %in% c(15, 38, 40) ~ 100,
                              TRUE ~ NA_real_)) %>% 
  # gill net stuff
  mutate(tommur = ifelse(gid == 2, round(mesh / 2.54), NA)) %>% 
  mutate(tommur = case_when(tommur <= 66 ~ 60,
                            tommur <= 76 ~ 70,
                            tommur <= 87 ~ 80,
                            tommur <= 100000 ~ 90,
                            TRUE ~ NA_real_)) %>% 
  mutate(mesh.std = ifelse(gid == 2, round(tommur * 2.54), mesh.std)) %>% 
  mutate(mesh.std = ifelse(gid == 2 & is.na(mesh.std),  203, mesh.std)) 
LGS %>% 
  count(gid, mesh.std)



print(c(n0, nrow(LGS)))
# end: Miscellaneous "corrections"
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 5. Set gear width proxy
# NOTE: not a correction specify gear width proxy
LGS <- 
  LGS %>% 
  mutate(gear.width = case_when(gid %in% c(6L, 7L, 9L, 14L) ~ as.numeric(sweeps),
                                gid %in% c(15L, 38L, 40L) ~ as.numeric(plow_width),
                                TRUE ~ NA_real_)) %>% 
  # cap gear width
  mutate(gear.width = case_when(gid ==  6L & gear.width > 250 ~ 250,
                                gid ==  7L & gear.width > 250 ~ 250,
                                gid ==  9L & gear.width >  75 ~  75,
                                gid == 14L & gear.width >  55 ~  55,
                                TRUE ~ gear.width))

gear.width <- 
  LGS %>% 
  filter(gid %in% c(6, 7, 9, 14, 15, 37, 38, 40)) %>% 
  group_by(gid) %>% 
  summarise(gear.width.median = median(gear.width, na.rm = TRUE),
            .groups = "drop")
# use median gear width by gear, if gear width is missing
#   could also try to do this by vessels. time scale (year) may also matter here
LGS <- 
  LGS %>% 
  left_join(gear.width,
            by = "gid") %>% 
  mutate(gear.width = ifelse(gid %in% c(6, 7, 9, 14, 15, 37, 38, 40) & is.na(gear.width),
                             gear.width.median,
                             gear.width)) %>% 
  select(-gear.width.median)

# Put gear width of 5 as 500 - this is taken from thin air
#   TODO: What gear width should be used
LGS <- 
  LGS %>% 
  mutate(gear.width = ifelse(gid == 5, 500, gear.width))
print(c(n0, nrow(LGS)))

table(LGS$gid, !is.na(LGS$gear.width))

LGS %>% 
  group_by(gid) %>% 
  summarise(gear.width = median(gear.width))
# end: Set gear width proxy
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 6. Get rid of intermediary variables
LGS <- 
  LGS %>% 
  select(-c(gid.lb, gid.ln, gc.lb, gc.ln, sid.target, catch, p, n.sid, i,
            overlap))


# ------------------------------------------------------------------------------
# 7. gear class of corrected gid
LGS <- 
  LGS %>% 
  left_join(gears %>% select(gid, gclass))

# 8. Save file for downstream processing
# should here drop records of mobile gear with not t1 and t2??

LGS %>% write_rds("data/LGS_corrected.rds")


