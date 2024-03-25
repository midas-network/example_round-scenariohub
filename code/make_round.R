# Clean environment
rm(list = ls())

# Library
library(dplyr)
library(hubUtils) # from GitHub: Infectious-Disease-Modeling-Hubs/hubUtils
library(magrittr)

# Prerequisite
source("code/utils.R")
config <- hubUtils::read_config(getwd(), config = "tasks")

## RSV ROUND 1 -------------
#rsv1 <- config$rounds[[4]]
#obs_file <- paste0("https://raw.githubusercontent.com/",
#                   "midas-network/rsv-scenario-modeling-hub/main/target-data/",
#                   "archive/2023-10-16_rsvnet_hospitalization.csv")
#rsv_net <- read.csv(obs_file)
## - Add missing age group by aggregating detailed age group
#rsv_data <- new_age_group(rsv_net, c("0-0.49", "0.5-0.99"), "0-0.99")
#rsv_data <- new_age_group(rsv_data, c("1-1.99", "2-4"), "1-4")
#rsv_data <- new_age_group(rsv_data, c("0-130", "65-130", "0-4"), "5-64",
#                          "minus")
#
## Worfklow
#req_df <- make_df_sample(rsv1$model_tasks, max_sample = max_sample(rsv1))
#all_data <- lapply(unique(req_df$team_model), function(model_id) {
#  df <- dplyr::filter(req_df, team_model == model_id)
#  df <- update_val_obs(df, rsv_data, add_col = "age_group")
#  df <- make_quantiles(df)
#  df <- make_peak(df, horizon_max = max(df$horizon, na.rm = TRUE))
#}) %>%
#  setNames(unique(req_df$team_model)) %>%
#  team_sample_id(def_grp(rsv1),  max_sample(rsv1)) %>%
#  write_output_parquet()
#
## EQUITY ROUNDS ------------
## Phase I
#lapply(config$rounds[5], function(x) {
#  req_df <- make_df_sample(x$model_tasks,
#                           max_sample =  max_sample(x))
#  lapply(unique(req_df$team_model), function(model_id) {
#    df <- dplyr::filter(req_df, team_model == model_id)
#    df <- update_df_val_sample(df)
#  }) %>%
#    setNames(unique(req_df$team_model)) %>%
#    team_sample_id(def_grp(x), max_sample(x)) %>%
#    write_output_parquet(date_file = x$round_id)
#})
#
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
obs_data <- rbind(obs_data0, obs_data1, obs_data2)
rm(list = grep("obs_data\\d|obs_death|obs_hosp", ls(), value = TRUE))

teams <- c("team1-modela", "team2-modelb", "team3-modelc", "team4-modeld",
           "team5-modele", "team6-modelf", "team7-modelg", "team8-modelh",
           "team9-modeli", "team10-modelj")

lapply(config$rounds[7], function(x) {
  req_df <- make_df_sample(x$model_tasks,
                           max_sample =  max_sample(x), team_model = "test")
  var_n <- setNames(c(seq(0.1, 0.9, by = 0.1), 0.99), teams)
  default_pairing <- def_grp(x)
  max_sample <- max_sample(x)
  lapply(teams, function(model_id) {
    # Print team information
    print(paste0("Generate example data for: ", model_id))
    # Prepare value
    print("-- Generate value")
    df <- dplyr::select(req_df, -team_model)
    if (model_id %in% c("team1-modela", "team2-modelb")) {
      df <- update_df_val_sample(df, max_value = 1e+07, quantile = TRUE,
                                 quant_group = c("origin_date", "scenario_id",
                                                 "location", "target",
                                                 "horizon", "age_group"),
                                 cumul_group = c("origin_date",  "scenario_id",
                                                 "location", "target",
                                                 "output_type",
                                                 "output_type_id", "age_group"))
    } else {
      df <- update_val_obs(df, obs_data, start_date = NULL,
                           end_date = NULL, add_col = "age_group",
                           target = c("inc death", "inc hosp"),
                           limit_date = "2022-02-15", var = var_n[model_id]) %>%
        dplyr::distinct()
      df <- lapply(unique(df$age_group), function(age) {
        df_age <- dplyr::filter(df, age_group == age)
        if (age != "0-130") {
          df_age <- dplyr::mutate(df_age,
                                  value = value +
                                    (value * sample(seq(-0.9, 0.9, by = 0.01),
                                                    nrow(df_age),
                                                    replace = TRUE)))
        }
        return(df_age)
      }) %>%
        dplyr::bind_rows() %>%
        dplyr::mutate(value = ifelse(is.na(value) | value < 0, 0, value))
      df <- make_quantiles(df,
                           quant_group = c("origin_date", "scenario_id",
                                           "location", "target", "horizon",
                                           "age_group"),
                           cumul_group = c("origin_date",  "scenario_id",
                                           "location", "target", "output_type",
                                           "output_type_id", "age_group"),
                           keep_cumul = FALSE)
    }
    # Prepare output type ID information
    print("-- Generate sample ID")
    df <- dplyr::mutate(df, value = round(value, 1),
                        run_grouping = ifelse(output_type == "sample", 1, NA),
                        stochastic_run = ifelse(output_type == "sample", 1, NA),
                        output_type_id = ifelse(output_type == "sample", NA,
                                                output_type_id))
    gc()
    if (model_id %in% c("team1-modela", "team6-modelf")) {
      df <- prep_sample_information(df, "stochastic_run", default_pairing,
                                    rep = max_sample)
    } else if (model_id %in% c("team2-modelb", "team7-modelg")) {
      df <- prep_sample_information(df, "run_grouping", default_pairing,
                                    rep = max_sample)
    } else if (model_id %in% c("team3-modelc", "team8-modelh")) {
      df <-
        prep_sample_information(df, "run_grouping", default_pairing,
                                rep = max_sample) %>%
        prep_sample_information("stochastic_run", default_pairing,
                                rep = max_sample)
    } else if (model_id %in% c("team4-modeld", "team9-modeli")) {
      df <-
        prep_sample_information(df, "run_grouping", default_pairing,
                                rep = max_sample,  same_rep = TRUE) %>%
        prep_sample_information("stochastic_run", default_pairing,
                                rep = max_sample)
    } else {
      df <-
        prep_sample_information(df, "run_grouping", c(default_pairing,
                                                      "scenario_id"),
                                rep = max_sample,  same_rep = TRUE) %>%
        prep_sample_information("stochastic_run", default_pairing,
                                rep = max_sample)
    }
    gc()
    # Write output
    print("-- Write output")
    folder_name <-  paste0("data-processed/", model_id, "/")
    if (model_id %in% c("team1-modela", "team6-modelf",
                        "team7-modelg", "team8-modelh", "team10-modelj")) {
      df$value <- round(df$value, 1)
    } else {
      df$value <- as.integer(df$value)
    }
    if (!dir.exists(folder_name)) dir.create(folder_name)
    arrow::write_dataset(df, folder_name,
                         partitioning = c("origin_date", "target"),
                         hive_style = FALSE, compression = "gzip",
                         compression_level = 9,
                         basename_template = paste0(model_id,
                                                    "{i}.gz.parquet"))

    return(NULL)
  })
})

# Clean environment
rm(list = ls())
