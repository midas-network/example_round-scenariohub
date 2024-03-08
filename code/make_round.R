# Clean environment
rm(list = ls())

# Library
library(dplyr)
library(hubUtils) # from GitHub: Infectious-Disease-Modeling-Hubs/hubUtils
library(magrittr)

# Prerequisite
source("code/utils.R")
config <- hubUtils::read_config(getwd(), config = "tasks")

# RSV ROUND 1 -------------
rsv1 <- config$rounds[[4]]
obs_file <- paste0("https://raw.githubusercontent.com/",
                   "midas-network/rsv-scenario-modeling-hub/main/target-data/",
                   "archive/2023-10-16_rsvnet_hospitalization.csv")
rsv_net <- read.csv(obs_file)
# - Add missing age group by aggregating detailed age group
rsv_data <- new_age_group(rsv_net, c("0-0.49", "0.5-0.99"), "0-0.99")
rsv_data <- new_age_group(rsv_data, c("1-1.99", "2-4"), "1-4")
rsv_data <- new_age_group(rsv_data, c("0-130", "65-130", "0-4"), "5-64",
                          "minus")

# Worfklow
req_df <- make_df_sample(rsv1$model_tasks, max_sample = max_sample(rsv1))
all_data <- lapply(unique(req_df$team_model), function(model_id) {
  df <- dplyr::filter(req_df, team_model == model_id)
  df <- update_val_obs(df, rsv_data, add_col = "age_group")
  df <- make_quantiles(df)
  df <- make_peak(df, horizon_max = max(df$horizon, na.rm = TRUE))
}) %>%
  setNames(unique(req_df$team_model)) %>%
  team_sample_id(def_grp(rsv1),  max_sample(rsv1)) %>%
  write_output_parquet()

# EQUITY ROUNDS ------------
# Phase I
lapply(config$rounds[5], function(x) {
  req_df <- make_df_sample(x$model_tasks,
                           max_sample =  max_sample(x))
  lapply(unique(req_df$team_model), function(model_id) {
    df <- dplyr::filter(req_df, team_model == model_id)
    df <- update_df_val_sample(df)
  }) %>%
    setNames(unique(req_df$team_model)) %>%
    team_sample_id(def_grp(x), max_sample(x)) %>%
    write_output_parquet(date_file = x$round_id)
})

# COVID Round 18 ------
obs_death <- read.csv("data-goldstandard/deaths_incidence_num.csv") %>%
  dplyr::select(date = time_value, location = fips, value) %>%
  dplyr::mutate(target = "inc death")
obs_hosp <- read.csv("data-goldstandard/hospitalization.csv") %>%
  dplyr::select(date = time_value, location = fips, value) %>%
  dplyr::mutate(target = "inc hosp")
obs_data <- rbind(obs_death, obs_hosp)
obs_data0 <- obs_data %>% dplyr::mutate(age_group = "0-130")
obs_data1 <- obs_data %>% dplyr::mutate(age_group = "0-64", value = 0.6 * value)
obs_data2 <- obs_data %>% dplyr::mutate(age_group = "65-130",
                                        value = 0.4 * value)
obs_data3 <- obs_data %>% dplyr::mutate(age_group = "0-0.99",
                                        value = 0.05 * value)
obs_data4 <- obs_data %>% dplyr::mutate(age_group = "1-4", value = 0.1 * value)
obs_data5 <- obs_data %>% dplyr::mutate(age_group = "5-64", value = 0.5 * value)
obs_data <- rbind(obs_data0, obs_data1, obs_data2, obs_data3, obs_data4,
                  obs_data5)
rm(list = grep("obs_data\\d|obs_death|obs_hosp", ls(), value = TRUE))


lapply(config$rounds[7], function(x) {
  req_df <- make_df_sample(x$model_tasks,
                           max_sample =  max_sample(x))
  df_all <- lapply(unique(req_df$team_model), function(model_id) {
    df <- dplyr::filter(req_df, team_model == model_id)
    df <- update_val_obs(df, obs_data, start_date = NULL,
                         end_date = NULL, add_col = "age_group",
                         target = c("inc death", "inc hosp"),
                         limit_date = "2022-02-15") %>%
      dplyr::distinct() %>%
      dplyr::mutate(value = ifelse(is.na(value) | value < 0, 0, value))
    df <- dplyr::mutate(df, value = round(value, 3))
    df <- make_quantiles(df,
                         quant_group = c("origin_date", "scenario_id",
                                         "location", "target", "horizon",
                                         "team_model", "age_group"),
                         cumul_group = c("origin_date",  "scenario_id",
                                         "location", "target", "output_type",
                                         "output_type_id", "team_model",
                                         "age_group"),
                         keep_cumul = FALSE)
    gc()
    return(df)
  })
  list_all <- df_all %>%
    setNames(unique(req_df$team_model)) %>%
    team_sample_id(def_grp(x), max_sample(x)) %>%
    write_output_parquet(partition = c("origin_date", "target"))
})

# Clean environment
rm(list = ls())
