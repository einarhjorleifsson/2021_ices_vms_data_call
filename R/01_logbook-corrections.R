# ------------------------------------------------------------------------------
# A single logbook munging to then be used downstream for stk match
# This script is based on merging the 2020 ices datacall script and that
# used in the 2019 anr-request
# The idea is that the whole process is more or less in one script.
#
# run using
# nohup R < R/001_munging.R --vanilla &
#
# TODO:
#      0. Access data only once
#      1. filter data (like only mobile bottom contact gear) as downstream as possible
#      2. Include/adapt the code in prj2/vms/ICES_vms_datacall (metier etc)
#      3. Higher resolution of dredge than just GID 15 - DONE
#      4. Add the file for speed filtering by gid
#
# PROCESSING STEPS:
#  A. Logbooks
#      1. Get support tables
#      2. Munge logbooks, merge mobile and static gear into one file
#      3. Correct gear code
#      4. Miscalenous
#         Cap effort (tow time)
#         Create gear width proxy
#         Standardize mesh size
#         Cap t2 so does not overlap with next t1
#             NOTE: effort not adjusted accordingly




#      3. add mobileid
#      5. add ICES metier
#      6. where either t1 and t2 within a fishing day is not defined
#         group data within a fishing day
#  B. stk data merging
#      0. data is processed within a year loop, each year then stored separately
#      1. year data compiled by each vessel separately
# ------------------------------------------------------------------------------

do.logbooks <- FALSE
do.stk      <- TRUE


YEARS <- 2020:2009

library(data.table)
library(sf)
library(tidyverse)
library(lubridate)
library(mar)
source("R/functions.R")
source("~/r/Pakkar2/omar/R/stk.R")
con <- connect_mar()




# ------------------------------------------------------------------------------
# get logbook data
#     No need to run this each time

tmp.lb.base <-
  mar:::lb_base(con) %>%
  filter(year %in% YEARS)
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

# used to double-check if and where we "loose" data downstream
n0 <- nrow(LGS)

# here get the gear from landings
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
  # just take the first match, i.e. only 1 visir per day
  distinct(visir, .keep_all = TRUE)

LGS <- 
  LGS %>% 
  left_join(tmp.lb.ln.match,
            by = "visir")

n.lbmatch <- nrow(LGS)
print(c(n0, n.lbmatch))

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

n.catchmatch <- nrow(LGS)
print(c(n0, n.catchmatch))

LGS <- 
  LGS %>% 
  select(visir, vid, gid.lb, gid.ln, sid.target, everything())

#rm(tmp.lb.base, tmp.lb.catch, tmp.lb.mobile, tmp.lb.static, tmp.ln.base, tmp.lb.ln.match)

# ------------------------------------------------------------------------------
# Gear corrections

gears <-
  tbl_mar(con, "ops$einarhj.gear") %>% collect(n = Inf) %>%
  mutate(description = ifelse(gid == 92, "G.halibut net", description),
         gid = as.integer(gid),
         gclass = as.integer(gclass))

LGS2 <- 
  LGS %>% 
  left_join(gears %>% select(gid.lb = gid, gc.lb = gclass), by = "gid.lb") %>% 
  left_join(gears %>% select(gid.ln = gid, gc.ln = gclass), by = "gid.ln") %>% 
  select(visir:gid.ln, gc.lb, gc.ln, everything()) %>% 
  mutate(gid = NA_integer_,
         gid.source = NA_character_) %>% 
  mutate(i = gid.lb == gid.ln,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.lb=gid.ln", gid.source)) %>% 
  mutate(i = is.na(gid) & gc.lb == gc.ln,
         gid = ifelse(i, gc.lb, gid),
         gid.source = ifelse(i, "gc.lb=gc.ln->gid.lb", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 15L & gid.lb %in% c(5L, 6L),
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=15, gid.lb=5,6->gid.ln", gid.source)) %>% 
  mutate(i = is.na(gid) & 
           gid.ln == 21 & 
           gid.lb == 6 &
           !sid.target %in% c(30, 36, 41),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6, sid.target != 30,36,41->gid.lb",
                             gid.source)) %>% 
  mutate(i = is.na(gid) &
           gid.ln == 21 &
           gid.lb == 6 & 
           sid.target %in% c(22, 41),
         gid = ifelse(i, 14, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6, sid.target = 22,41->14", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 6 & gid.lb == 7 & sid.target %in% c(11, 19, 30:36),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=6, gid.lb=7, sid.target = 11,19,30:36->gid.lb", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 5,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=5->gid.ln", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 40,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=40->gid.lb", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 14 & gid.lb == 6 & sid.target == 41,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=14, gid.lb=6, sid.target = 41->gid.ln", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 7 & gid.lb == 6,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=7, gid.lb=6->gid.ln", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 9 & gid.lb == 6,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=9, gid.lb=6->gid.ln", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 38,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=38->gid.ln", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 6 & gid.lb == 14,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=6, gid.lb=14->gid.lb", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 15 & gid.lb == 39,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=15, gid.lb=39->gid.lb", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 40 & gid.lb %in% c(5, 6),
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=40, gid.lb=5,6->gid.ln", gid.source)) %>% 
  mutate(i = is.na(gid) & is.na(gid.ln) & gid.lb %in% c(1:3),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "is.na(gid.ln), gid.lb=1:3->gid.lb", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 21 & gid.lb == 6,
         gid = ifelse(i, 7, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6->7", gid.source)) %>% 
  mutate(i = is.na(gid) & gid.ln == 21 & gid.lb == 14,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=14->gid.lb", gid.source))
# older mutate trials
# gid = case_when(is.na(gid) & sid.target == 199L ~ -199L,
#                 # sea urchin
#                 is.na(gid) & sid.target == 191L ~   40L,
#                 # kúfiskur
#                 is.na(gid) & sid.target ==  46L ~   38L,
#                 # scallops, sid 77 is some error, NA means zero catch?
#                 is.na(gid) & sid.target %in% c(43L, 77L, NA_integer_) ~   15L,
#                 # all trap fishier into one gid
#                 # rock crab
#                 is.na(gid) & sid.target == 689L ~   18L,
#                 # whelk 
#                 is.na(gid) & sid.target ==  45L  ~  18L,
#                 # lobster
#                 is.na(gid) & sid.target == 40L & gid.lb == 9L ~ gid.lb,
#                 is.na(gid) & sid.target == 40L & gid.ln == 9L ~ gid.ln,
#                 # shrimp
#                 is.na(gid) & sid.target == 41L & gid.lb == 14L ~ gid.lb,
#                 is.na(gid) & sid.target == 41L & gid.ln == 14L ~ gid.ln,
#                 # lumpfish
#                 is.na(gid) & sid.target == 961L & gid.lb == 2L ~ 2L,
#                 # pelagic redfish
#                 is.na(gid) & sid.target == 11L & gid.lb == 7L ~ gid.lb,
#                 # atlantic mackerel
#                 is.na(gid) & sid.target == 36L & gid.lb == 7L ~ gid.lb,
#                 TRUE ~ NA_integer_),
# gid.source = ifelse(!is.na(gid) & is.na(gid.source), "sid-target_and-then-some", NA_character_)) %>% 
# Lump some gears
LGS2 <-
  LGS2 %>% 
  mutate(gid = case_when(gid %in% c(8, 10, 12) ~ 4,   # purse seines
                         gid %in% c(18, 39) ~ 17,      # traps
                         TRUE ~ gid)) %>% 
  # make the rest a negative number
  mutate(gid = ifelse(is.na(gid), -666, gid)) %>% 
  # "skip" these also
  mutate(gid = ifelse(gid %in% c(4, 12, 42), -666, gid)) %>% 
  # lump dredges into one single gear
  mutate(gid = ifelse(gid %in% c(15, 37, 38, 40), 15, gid))

table(LGS2$gid, useNA = "ifany")


# end: Gear corrections
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Miscallenous "corrections"
LGS2 <- 
  LGS2 %>% 
  # cap effort hours
  mutate(effort = case_when(effort > 12 & gid ==  6 ~ 12,
                            effort > 24 & gid ==  7 ~ 24,
                            effort > 15 & gid == 14 ~ 15,
                            TRUE ~ effort)) %>% 
  # specify gear width proxy
  mutate(gear.width = case_when(gid %in% c(6L, 7L, 9L, 14L) ~ as.numeric(sweeps),
                                gid %in% c(15L, 38L, 40L, -199L) ~ as.numeric(plow_width),
                                TRUE ~ NA_real_)) %>% 
  # standardize mesh size
  mutate(mesh = ifelse(gid == 7, mesh_min, mesh)) %>%
  # "standardize" mesh size
  mutate(mesh.std = case_when(gid ==  9 ~ 80,
                              gid %in% c(7, 10, 12, 14) ~ 40,
                              gid %in% c(5, 6) & (mesh <= 147 | is.na(mesh)) ~ 135,
                              gid %in% c(5, 6) &  mesh >  147 ~ 155,
                              gid %in% c(15, 38, 40) ~ 100,
                              TRUE ~ NA_real_)) %>%
  # set mesh size for traps as zero
  #  TODO: double check if that is actually what the code below achieves
  mutate(mesh.std = ifelse(is.na(mesh.std), 0, mesh.std)) %>%
  select(-c(mesh_min, plow_width)) %>% 
  # Cap on the t2
  #    NOTE: Effort not adjusted accordingly
  arrange(vid, t1) %>%
  group_by(vid) %>%
  mutate(overlap = if_else(t2 > lead(t1), TRUE, FALSE, NA),
         # testing
         t22 = if_else(overlap,
                       lead(t1) - minutes(1), # need to subtract 1 minute but get the format right
                       t2,
                       as.POSIXct(NA)),
         t22 = ymd_hms(format(as.POSIXct(t22, origin="1970-01-01", tz="UTC"))),
         t2 = if_else(overlap & !is.na(t22), t22, t2, as.POSIXct(NA))) %>%
  ungroup() %>% 
  select(-t22)


# end: Miscalenous "corrections"
# ------------------------------------------------------------------------------

# gear class of coerrected gid
LGS2 <- 
  LGS2 %>% 
  left_join(gears %>% select(gid, gclass))

table(LGS2$gid, useNA = "ifany")

# should here drop records of mobile gear with not t1 and t2??

LGS2 %>% write_rds("data/LGS_corrected.rds")


