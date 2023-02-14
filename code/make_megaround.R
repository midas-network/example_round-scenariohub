#### DUMMY SCRIPT

## Sample file format ----

# data frame with one group
df <- data.frame(
  origin_date = "2023-02-01",
  scenario_id = "A-2023-01-25",
  location = "US",
  target = "inc death",
  horizon = 1,
  type = "sample",
  type_id = c(1:100),
  value = sample(seq(11900.00, 12500.00, by = 0.001), 100)
)

# for all horizon
df_tot <- df
for (i in 2:104) { #104 #156
  df2 <- data.frame(
    origin_date = "2023-02-01",
    scenario_id = "A-2023-01-25",
    location = "US",
    target = "inc death",
    horizon = i,
    type = "sample",
    type_id = c(1:100),
    value = df$value * as.numeric(paste0("1.", i))
  )
  df_tot <- rbind(df_tot, df2)
}
# for all scenario
dfscen <- df_tot
scen_list <- c("A-2023-01-25", "B-2023-01-25", "C-2023-01-25", "D-2023-01-25",
               "E-2023-01-25", "F-2023-01-25", "G-2023-01-25", "H-2023-01-25"
)
for (i in 2:length(scen_list)) {
  df2 <- data.frame(
    origin_date = "2023-02-01",
    scenario_id = scen_list[[i]],
    location = "US",
    target = "inc death",
    horizon = dfscen$horizon,
    type = "sample",
    type_id = dfscen$type_id,
    value = dfscen$value * as.numeric(paste0("1.", i))
  )
  df_tot <- rbind(df_tot, df2)
}

# for all target
dftarget <- df_tot
target_list <- c("inc death", "inc hosp", "inc case")
for (i in 2:length(target_list)) {
  df2 <- data.frame(
    origin_date = "2023-02-01",
    scenario_id = dftarget$scenario_id,
    location = "US",
    target = target_list[[i]],
    horizon = dftarget$horizon,
    type = "sample",
    type_id = dftarget$type_id,
    value = dftarget$value * as.numeric(paste0("1.", i))
  )
  df_tot <- rbind(df_tot, df2)
}

# for all location
dfloc <- df_tot
loc_list <- c("US", "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
              "13", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24",
              "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
              "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47",
              "48", "49", "50", "51", "53", "54", "55", "56", "60", "66", "69",
              "72", "74", "78"
)
for (i in 2:length(loc_list)) {
  df2 <- data.frame(
    origin_date = "2023-02-01",
    scenario_id = dfloc$scenario_id,
    location = loc_list[[i]],
    target = dfloc$target,
    horizon = dfloc$horizon,
    type = "sample",
    type_id = dfloc$type_id,
    value = dfloc$value * as.numeric(paste0("0.", i))
  )
  df_tot <- rbind(df_tot, df2)
}

# write output
filename <- "../model-output/team1-modela/2023-02-01-team1-modela.gz.parquet"
arrow::write_parquet(df_tot, filename, compression = "gzip", 
                     compression_level = 9)

##### TESTS -----


## old format
#library(dplyr)
#df_old <- df_tot %>% mutate(
#  scenario_name = case_when(
#    scenario_id == "A-2023-01-25" ~ "ScenA",
#    scenario_id == "B-2023-01-25" ~ "ScenB",
#    scenario_id == "C-2023-01-25" ~ "ScenC",
#    scenario_id == "D-2023-01-25" ~ "ScenD",
#    scenario_id == "E-2023-01-25" ~ "ScenE",
#    scenario_id == "F-2023-01-25" ~ "ScenF",
#    scenario_id == "G-2023-01-25" ~ "ScenG",
#    scenario_id == "H-2023-01-25" ~ "ScenH"
#  ),
#  full_target = paste0(horizon, " wk ahead ", target),
#  target_end_date = as.Date(df$origin_date) + (horizon * 7) - 1) %>%
#  select(model_projection_date = origin_date, target = full_target,
#         target_end_date, location, type, type_id, value, scenario_id,
#         scenario_name)
#
#df_tot_sample <- df_tot
#df_old_sample <- df_old

## Write output ----

#df_tot <- dplyr::filter(df_tot_quantile, horizon < 157)
#df_old <- dplyr::filter(df_old_quantile, target_end_date < "2026-02-03")
#
## write output for 6 scenarios: (data frame of dimension: 10857600 X 8)
## write output old version for 6 scenarios: (data frame of dimension: 10857600 X 9)
## write output for 8 scenarios: (data frame of dimension: 14476800 X 8)
## write output old version for 8 scenarios: (data frame of dimension: 14476800 X 9)
#filename <- "../model-output/team1-modela/2023-02-01-team1-modela.csv"
#filename_old <- "~/Documents/test/megaround_test/2023-02-01old.csv"
#write.csv(df_tot, filename, row.names = FALSE)
#write.csv(df_old, filename_old, row.names = FALSE)

# file size CSV
## FILE with 6 scenarios
# file with 10 digits value:
# file with 5 digits value: 815.5568*
# file with 3 digits value: 794.0924
# file with 3 digits value and old format: 1098.105
# file with integer value: 761.4895*
## FILE with 8 scenarios
# file with 3 digits value: 1060.945
# file with 3 digits value and old format: 1466.296
# file with 0 digits value: 990.8988  (911.8093, rm territories )
# file with 0 digits value and old format: 1424.045 (1275.227, rm territories )
#file.size(filename) / 1e6
#file.size(filename_old) / 1e6

# try zip file
## FILE with 6 scenarios
# file with 10 digits value:
# file with 5 digits value: 120.2007*
# file with 3 digits value: 107.03
# file with 3 digits value and old format: 109.4759
# file with integer value: 87.7611*
## FILE with 8 scenarios
# file with 3 digits value: 144.1
# file with 3 digits value and old format: 147.3415
# file with 0 digits value: 104.4981 (105.9633, rm territories)
# file with 0 digits value and old format: 122.4783 (109.4989, rm territories)
#wd0 <- getwd()
#setwd(dirname(filename))
#zip(gsub(".csv$", "", basename(filename)), basename(filename))
#setwd(wd0)
#filezip <- "~/Documents/test/megaround_test/2023-02-01.zip"
#file.size(filezip) / 1e6
#
#wd0 <- getwd()
#setwd(dirname(filename_old))
#zip(gsub(".csv$", "", basename(filename_old)), basename(filename_old))
#setwd(wd0)
#filezip_old <- "~/Documents/test/megaround_test/2023-02-01old.zip"
#file.size(filezip_old) / 1e6


# try gz file
## FILE with 6 scenarios
# file with 10 digits value:
# file with 5 digits value:
# file with 3 digits value:
# file with 3 digits value and old format:
# file with integer value:
## FILE with 8 scenarios
# file with 3 digits value:
# file with 3 digits value and old format:
# file with 0 digits value:
# file with 0 digits value and old format:
#filegz <- "~/Documents/test/megaround_test/2023-02-01.gz"
#gz <- gzfile(filegz, "w")
#write.csv(df_tot, gz)
#close(gz)
#file.size(filegz) / 1e6
#
#fileoldgz <- "~/Documents/test/megaround_test/2023-02-01.gz"
#gz <- gzfile(fileoldgz, "w")
#write.csv(df_old, gz)
#close(gz)
#file.size(fileoldgz) / 1e6

# try parquet file
## FILE with 6 scenarios
# file with 10 digits value:
# file with 5 digits value: 87.304*
# file with 3 digits value: 87.29537
# file with 3 digits value and old format: 87.33541
# file with integer value: 82.54004*
## FILE with 8 scenarios
# file with 3 digits value: 116.3704
# file with 3 digits value and old format: 116.4237
# file with 0 digits value: 111.6033 (85 gz.pqt) ( 99.4933, rm territories)
# file with 0 digits value and old format: 111.6567 (85 gz.pqt) (99.54127, rm territories)
#filepqt <- "~/Documents/test/megaround_test/2023-02-01.pqt"
#arrow::write_parquet(df_tot, filepqt)
#file.size(filepqt) / 1e6
#
#filepqt_old <- "~/Documents/test/megaround_test/2023-02-01old.pqt"
#arrow::write_parquet(df_old, filepqt_old)
#file.size(filepqt_old) / 1e6
#
#filepqt <- "~/Documents/test/megaround_test/2023-02-01.gz.parquet"
#arrow::write_parquet(df_tot, filepqt, compression = "gzip", compression_level = 5)
#file.size(filepqt) / 1e6
#
#filepqt_old <- "~/Documents/test/megaround_test/2023-02-01old.gz.parquet"
#arrow::write_parquet(df_old, filepqt_old, compression = "gzip", compression_level = 5)
#file.size(filepqt_old) / 1e6
#
#
## try RDS file
#filerds <- "~/Documents/test/megaround_test/2023-02-01.rds"
#saveRDS(df_tot, filerds)
#file.size(filerds) / 1e6
#
#filerds_old <- "~/Documents/test/megaround_test/2023-02-01old.rds"
#saveRDS(df_old, filerds_old)
#file.size(filerds_old) / 1e6
#
## try feather file
#filefst <- "~/Documents/test/megaround_test/2023-02-01.fst"
#fst::write_fst(df_tot, filefst, compress = 100)
#file.size(filefst) / 1e6
#
