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


cat_lh <- function(x, file = "data/LGS_corrected.log") {
  
  if(class(x)[1] == "character") {
    
    if(length(x) > 1) x <- glue::glue_collapse(x, sep = ",")
    cat(x, file = file, append = TRUE)
    
  }
  
  if(class(x)[1] %in% c("tbl_df", "tbl", "data.frame")) {
    capture.output(as.data.frame(x), file = file, append = TRUE)
  }
  
  if(class(x)[1] == "knitr_kable") {
    capture.output(x, file = file, append = TRUE)
  }
  
  cat("\n", file = file, append = TRUE)
  
}