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
equity_p1 <- config$rounds[[5]]
req_df <- make_df_sample(equity_p1$model_tasks,
                         max_sample =  max_sample(equity_p1))
all_data <- lapply(unique(req_df$team_model), function(model_id) {
  df <- dplyr::filter(req_df, team_model == model_id)
  df <- update_df_val_sample(df)
}) %>%
  setNames(unique(req_df$team_model)) %>%
  team_sample_id(def_grp(equity_p1), max_sample(equity_p1)) %>%
  write_output_parquet(date_file = equity_p1$round_id)

equity_p2 <- config$rounds[[6]]
req_df <- make_df_sample(equity_p2$model_tasks,
                         max_sample =  max_sample(equity_p2))
all_data <- lapply(unique(req_df$team_model), function(model_id) {
  df <- dplyr::filter(req_df, team_model == model_id)
  df <- update_df_val_sample(df)
}) %>%
  setNames(unique(req_df$team_model)) %>%
  team_sample_id(def_grp(equity_p2), max_sample(equity_p2)) %>%
  write_output_parquet(date_file = equity_p2$round_id)

# Clean environment
rm(list = ls())
