# ------------------------------------------------------------------------------
# run this as:
#  nohup R < R/01_logbook-corrections.R --vanilla > logs/logbooks.log &
lubridate::now()

# ------------------------------------------------------------------------------
# A single logbook munging to then be used downstream for stk match
# This script is based on merging 2020 ices datacall internal script and that
# used in the 2019 anr-request. Added then gear corrections and other things.
#
# The output has the same number of records as the input. In the downstream
# process the data used should be those where variable **use** is TRUE.
#
# TODO:
#      Should check the lb_functions in the mar-package
#      Check vids in landings - could be used to filter out wrong vids in
#       logbooks
#      Higher resolution of dredge than just gid == 15, domestic purpose only
#      Higher resolution of nets than just  gid == 2, domestic purpose only
#      Should ICES metier be set here or further downstream?
#
# PROCESSING STEPS:
#  1. Get and merge logbook and landings data
#  2. Gear corrections
#  3. Lump some gears
#  4. Cap effort and end of action
#  5. Mesh size "corrections"
#  6. Set gear width proxy
#  7. gear class of corrected gid
#  8. Add vessel information
#  9. Add metier

YEARS <- 2020:2009

library(data.table)
library(tidyverse)
library(lubridate)
library(mar)
con <- connect_mar()

# ------------------------------------------------------------------------------
# 0. Functions
match_nearest_date <- function(lb, ln) {
  
  lb.dt <-
    lb %>%
    select(vid, datel) %>%
    distinct() %>%
    setDT()
  
  ln.dt <-
    ln %>%
    select(vid, datel) %>%
    distinct() %>%
    mutate(dummy = datel) %>%
    setDT()
  
  res <-
    lb.dt[, date.ln := ln.dt[lb.dt, dummy, on = c("vid", "datel"), roll = "nearest"]] %>%
    as_tibble()
  
  lb %>%
    left_join(res,
              by = c("vid", "datel")) %>%
    left_join(ln %>% select(vid, date.ln = datel, gid.ln),
              by = c("vid", "date.ln"))
  
}
# end: 0. Functions
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 1. Get and merge logbook and landings data
vessels <- 
  mar:::vessel_registry(con, standardize = TRUE) %>% 
  # no dummy vessels
  filter(!vid %in% c(0, 1, 3:5),
         !is.na(name)) %>% 
  arrange(vid)
tmp.lb.base <-
  mar:::lb_base(con) %>%
  filter(year %in% YEARS) %>% 
  inner_join(vessels %>% select(vid),
             by = "vid")
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
  # make sure some static gears in the mobile are not include
  filter(!gid %in% c(1, 2, 3)) %>% 
  inner_join(mar:::lb_mobile(con),
             by = c("visir")) %>%
  collect(n = Inf)
tmp.lb.static <-
  tmp.lb.base %>%
  inner_join(mar:::lb_static(con),
             by = "visir") %>%
  collect(n = Inf)
LGS <-
  bind_rows(tmp.lb.mobile %>% mutate(source = "mobile"), 
            tmp.lb.static %>% mutate(source = "static")) %>%
  mutate(date = as_date(date),
         datel = as_date(datel),
         gid = as.integer(gid))
# nrows to double-check if and where we "loose" or for that matter accidentally
#  "add" data (may happen in joins) downstream
n0 <- nrow(LGS)
paste("Original number of records:", n0)
# Flag vessels with less than 5 records (assume they are vid errors)
LGS <- 
  LGS %>% 
  group_by(vid) %>% 
  mutate(n.records = n()) %>% 
  ungroup() %>% 
  mutate(use = ifelse(n.records > 5, TRUE, FALSE)) %>% 
  select(-n.records)
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
# Vessels per year - few vessels in 2020 is because of records for some
#  small vessels have not been entered
LGS %>% 
  group_by(year, gid.lb) %>% 
  summarise(n.vids = n_distinct(vid)) %>% 
  spread(gid.lb, n.vids)
paste("Number of records:", nrow(LGS))
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
paste("Number of records:", nrow(LGS))
LGS %>% 
  count(step, gid, gid.lb, gid.ln, gid.source) %>% 
  mutate(p = round(n / sum(n) * 100, 3)) %>% 
  arrange(-n) %>% 
  knitr::kable(caption = "Overview of gear corrections")
LGS %>% 
  mutate(missing = is.na(gid)) %>% 
  count(missing) %>% 
  mutate(p = n / sum(n) * 100) %>% 
  knitr::kable(caption = "Number and percentage of missing gear")
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
paste("Number of records:", nrow(LGS))
# end: Lump some gears
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 4. Cap effort and end of action
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
# if either t1 or t2 is missing from mobile towing gear except dredges, 
#   flag that records should not be used donwstream
LGS <- 
  LGS %>% 
  mutate(use = ifelse(gid %in% c(6, 7, 9, 14) & (is.na(t1) | is.na(t2)),
                      FALSE,
                      use))
paste("Number of records:", nrow(LGS))
# end: 4. Cap effort and end of action
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 5. Mesh size "corrections"
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
# Fill missing values with zeros
LGS <- 
  LGS %>% 
  mutate(mesh.std = ifelse(gid %in% c(-666, 1, 3, 17, 18), 0, mesh.std))
LGS %>% 
  count(gid, mesh.std) %>% 
  knitr::kable(caption = "Mesh sizes and number of records by gear")
paste("Number of records:", nrow(LGS))
# end:5. Mesh size "corrections"
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 6. Set gear width proxy
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
LGS %>% 
  group_by(gid) %>% 
  summarise(median.gear.width = median(gear.width),
            mean.gear.width = mean(gear.width),
            sd.gear.width = sd(gear.width)) %>% 
  knitr::kable(caption = "Statistics on gear width")
paste("Number of records:", nrow(LGS))
# end: Set gear width proxy
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# Get rid of intermediary variables
LGS <- 
  LGS %>% 
  select(-c(gid.lb, gid.ln, gc.lb, gc.ln, sid.target, catch, p, n.sid, i,
            overlap))
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 7. gear class of corrected gid
LGS <- 
  LGS %>% 
  left_join(gears %>% select(gid, gclass),
            by = "gid")
paste("Number of records:", nrow(LGS))
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# 8. Add vessel information
vessels <- 
  mar:::vessel_registry(con, standardize = TRUE) %>% 
  filter(!vid %in% c(0, 1, 3:5),
         !is.na(name)) %>% 
  arrange(vid) %>% 
  collect(n = Inf)
LGS <- 
  LGS %>% 
  left_join(vessels %>% 
              select(vid, kw = engine_kw, length, length_class))
paste("Number of records:", nrow(LGS))
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 9. Add metier
metier <-
  tribble(
    ~gid, ~dcf4, ~dcf5, ~dcf5b,
    1, "LLS", "Fish",      "DEF",     # Long line
    2, "GSN", "Fish",      "DEF",     # Gill net
    3, "LHM", "Fish",      "DEF",     # Jiggers (hooks)
    4, "PS",  "Fish",      "SPF",     # "Cod" seine
    5, "SDN", "Fish",      "DEF",     # Scottish seine
    6, "OTB", "Fish",      "DEF",     # Bottom fish trawl
    7, "OTM", "Fish",      "SPF",     # Pelagic trawl
    9, "OTB", "Nephrops",  "MCD",     # Bottom nephrops trawl
    10, "PS",  "Fish",      "SPF",     # "Herring" seine
    12, "PS",  "Fish",      "SPF",     # "Capelin" seine
    14, "OTB", "Shrimp",    "MCD",     # Bottom shrimp trawl
    15, "DRB", "Miscellaneous",       "MOL",     # Mollusc (scallop) dredge
    17, "TRP", "Misc",      "MIX",     # Traps
    38, "DRB", "Mollusc",   "MOL",     # Mollusc	(cyprine) dredge
    39, "TRP", "Mollusc",   "MOL",     # Buccinum trap
    40, "DRB", "Echinoderm","MOL",     # Sea-urchins dredge
    42, NA_character_,    NA_character_, NA_character_)
# metiers used
metier %>% 
  filter(gid %in% unique(LGS$gid)) %>% 
  knitr::kable(caption = "Metiers used")
LGS <- 
  LGS %>% 
  left_join(metier, by = "gid")
# Flag gear not to be used downstream, derive dcf6
LGS <-
  LGS %>% 
  # NOTE: FILTER OUT UNKNOWN GID and then some
  mutate(use = ifelse(gid %in% c(-666, 17, 18), FALSE, use)) %>% 
  select(-mesh) %>% 
  rename(mesh = mesh.std) %>% 
  mutate(dcf6 = paste(dcf4, dcf5b, mesh, "0_0", sep = "_")) %>% 
  mutate(type = "LE",
         country = "ICE")
LGS %>% 
  count(gid, dcf4, dcf5, dcf5b, mesh, dcf6) %>% 
  knitr::kable(caption = "Records by metier")
paste("Number of records:", nrow(LGS))
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# 10. ICES rectangles
res <- data.frame(SI_LONG = LGS$lon1,
                  SI_LATI = LGS$lat1)
LGS <- 
  LGS %>% 
  mutate(ices = vmstools::ICESrectangle(res)) %>% 
  mutate(valid.ices = !is.na(vmstools::ICESrectangle2LonLat(ices)$SI_LONG))
LGS %>% 
  count(valid.ices) %>% 
  knitr::kable(caption = "Valid ICES rectangles")
paste("Number of records:", nrow(LGS))
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# 11. Anonymize vid
#     May want to do this more downstream
anonymize.vid <-
  LGS %>%
  select(vid) %>%
  distinct() %>%
  mutate(vid0 = 1:n(),
         vid0 = paste0("ICE", str_pad(vid0, 4, pad = "0")))
LGS <- 
  LGS %>% 
  left_join(anonymize.vid, by = "vid")
paste("Number of records:", nrow(LGS))
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 11. Save file for downstream processing
LGS %>% write_rds("data/LGS_corrected.rds")
# ------------------------------------------------------------------------------


# ADDENDUM ---------------------------------------------------------------------
#  This is still in the making, needs to be reviewed carefully
#  Deals with matching vid and mobileid
#  Deals with all but gid 6, 7, 9, and 14 need to be done on daily summary in
#   order to process the ais data. Hence loose the original resolution of
#   visir
#  Question is what should the logbook annex vs the ais annex source file be
#   (lumped visir by day or not). Prefer that same data is used for both.
#   Check if "nest"-ing may be a good way to handle that
#   CURRENTLY THE AIS MUNGING IS BASED ON data/LGS_corrected_use.rds
#    that is generated below


LGS2 <- 
  LGS %>% 
  # NOTE: FILTER OUT UNKNOWN GID, where t1 and t2 not reported for 
  #   gid 6, 7, 9 & 14 and then some
  filter(use)
n0 <- nrow(LGS2)
(nrow(LGS) - nrow(LGS2)) / nrow(LGS) * 100

# easiest approach: for gear class not 6, 7, 9, 14 summarise the catch for the day
# and generate a new visir and set t1 and t2 as start and end time of the day
lgs1 <-
  LGS2 %>%
  filter(gid %in% c(6, 7, 9, 14),
         !is.na(t1), !is.na(t2))
lgs2 <-
  LGS2 %>%
  filter(!gid %in% c(6, 7, 9, 14))
nrow(lgs1) + nrow(lgs2) == nrow(LGS2)

table(!is.na(lgs2$vid))
table(!is.na(lgs2$date))
table(!is.na(lgs2$gid))
visir_visir_lookup <- 
  lgs2 %>% 
  group_by(vid, date, gid) %>% 
  mutate(visir1 = min(visir)) %>% 
  ungroup() %>% 
  select(visir1, visir)
nrow(lgs2)
length(unique(visir_visir_lookup$visir))
length(unique(lgs2$visir))

lgs2B <- 
  lgs2 %>% 
  group_by(vid, date, gid, dcf4, dcf5, dcf5b) %>%
  # get here all essential variables that are needed downstream
  summarise(visir = min(visir),
            n.sets = n(),
            towtime = sum(towtime),
            effort = sum(effort),
            total = sum(total),
            gear.width = mean(gear.width),
            .groups = "drop") %>%
  mutate(t1 = ymd_hms(paste0(year(date), "-", month(date), "-", day(date),
                             " 00:00:00")),
         t2 = ymd_hms(paste0(year(date), "-", month(date), "-", day(date),
                             " 23:59:00")))

nrow(lgs2B)
length(unique(visir_visir_lookup$visir1))

# 1. Transform the logbook data such time is stored as a single variable,
#     using an additional variable (startend) to indicate the if the time
#     value refers to the start or end of the haul.


# ------------------------------------------------------------------------------
# LAST MINUTE STUFF - moved R/02_match_vid-mid.R here
# Note this is a bit recursive because LGS_corrected.rds is used to 
# find the match. Should possible source the file here
# put the R/02_match_vid-mid.r code here
summary.lgs <-
  LGS %>% 
  group_by(vid) %>% 
  summarise(n.lgs = n(),
            n.gid = n_distinct(gid),
            min.date = min(date),
            max.date = max(date),
            .groups = "drop")
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
table(!is.na(MIDs), useNA = "ifany")
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

# NOTE: get here more records than in the original summary because 
#       some vessels have multiple mid
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
# Vessels with no matching mobileid
d %>% 
  filter(is.na(mid)) %>% 
  select(vid, name, n.lgs, n.gid:max.date) %>% 
  knitr::kable()
#Vessels with low logbook records
d %>% 
  filter(!is.na(mid),
         n.lgs <= 10) %>% 
  arrange(n.lgs, -n.stk) %>% 
  knitr::kable()

# write out, do a separate analysis, see checks.Rmd
d %>% write_rds("data/VID_MID.rds")

vidmid_lookup <- 
  read_rds("data/VID_MID.rds") %>% 
  select(vid, mid)

#
# end: LAST MINUTE STUFF - moved R/02_match_vid-mid.R here
# ------------------------------------------------------------------------------

LB <-
  bind_rows(lgs1 %>% select(visir, vid, gid, t1, t2),
            lgs2B %>% select(visir, vid, gid, t1, t2)) %>%
  left_join(vidmid_lookup,
            by = "vid") %>%
  select(vid, mid, visir, gid, t1, t2) %>%
  pivot_longer(cols = c(t1, t2),
               names_to = "startend",
               values_to = "time") %>%
  arrange(vid, time) %>%
  mutate(year = year(time))

# missing vessels (surprised how few) - need to be filtered out
LB %>%
  filter(is.na(mid)) %>%
  count(vid) %>% 
  as.data.frame()

# Note that below we do not filter by mid - need to double check
LB <-
  LB %>%
  filter(!is.na(mid))

LB %>% write_rds("data/LGS_corrected_use.rds")
visir_visir_lookup %>% write_rds("data/LGS_visir_visir_lookup.rds")


devtools::session_info()
