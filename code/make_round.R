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
lapply(config$rounds[5:6], function(x) {
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
lapply(config$rounds[7], function(x) {
  req_df <- make_df_sample(x$model_tasks,
                           max_sample =  max_sample(x))
  lapply(unique(req_df$team_model), function(model_id) {
    df <- dplyr::filter(req_df, team_model == model_id)
    df <-
      update_df_val_sample(df, quantile = TRUE,
                           quant_group = c("origin_date", "scenario_id",
                                          "location", "target", "horizon",
                                          "team_model"),
                           cumul_group = c("origin_date",  "scenario_id",
                                           "location", "target", "output_type",
                                           "output_type_id", "team_model"))
  }) %>%
    setNames(unique(req_df$team_model)) %>%
    team_sample_id(def_grp(x), max_sample(x)) %>%
    write_output_parquet()
})

# Clean environment
rm(list = ls())
