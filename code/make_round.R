# Clean environment
rm(list = ls())

# Library
library(dplyr)
library(hubUtils) # from GitHub: Infectious-Disease-Modeling-Hubs/hubUtils
library(magrittr)

# Prerequisite
source("code/utils.R")
config <- hubUtils::read_config(getwd(), config = "tasks")

# Validate hub config
a <- hubAdmin::validate_config()
hubAdmin::view_config_val_errors(a)

## EQUITY ROUNDS ------------
## Phase II
lapply(config$rounds[9], function(x) {
  req_df <- lapply(c(1:2), function(i) {
    req_df <- make_df_sample(x$model_tasks[i],
                             max_sample =  max_sample(x))
  }) %>% dplyr::bind_rows()
  default_pairing <- def_grp(x)
  max_sample <- max_sample(x)
  lapply(unique(req_df$team_model), function(model_id) {
    print(paste0("Generate example data for: ", model_id))
    df <- dplyr::filter(req_df, team_model == model_id)
    df <- dplyr::select(df, -team_model)
    print("-- Generate value")
    if (!model_id %in% c("team1-modela", "team2-modelb")) {
      df <- update_df_val_sample(df)
    } else {
      df <- update_df_val_sample(df, quantile = TRUE,
                                 quant_group = c("origin_date", "scenario_id",
                                                 "location", "target", "horizon",
                                                 "age_group"),
                                 cumul_group = c("origin_date",  "scenario_id",
                                                 "location", "target",
                                                 "output_type",
                                                 "output_type_id",
                                                 "age_group"),
                                 peak = TRUE)
    }
    print("-- Generate sample ID")
    df <- dplyr::mutate(df, value = round(value, 1),
                        run_grouping = ifelse(output_type == "sample", 1, NA),
                        stochastic_run = ifelse(output_type == "sample", 1, NA),
                        output_type_id = ifelse(output_type == "sample", NA,
                                                output_type_id))
    if (model_id %in% c("team1-modela")) {
      df <- prep_sample_information(df, "stochastic_run", default_pairing,
                                    id_rep = max_sample)
    } else if (model_id %in% c("team2-modelb")) {
      df <- prep_sample_information(df, "run_grouping", default_pairing,
                                    id_rep = max_sample)
    } else if (model_id %in% c("team3-modelc")) {
      df <-
        prep_sample_information(df, "run_grouping", default_pairing,
                                id_rep = max_sample) %>%
        prep_sample_information("stochastic_run", default_pairing,
                                id_rep = max_sample)
    } else if (model_id %in% c("team4-modeld")) {
      df <-
        prep_sample_information(df, "run_grouping", default_pairing,
                                id_rep = max_sample,  same_rep = TRUE) %>%
        prep_sample_information("stochastic_run", default_pairing,
                                id_rep = max_sample)
    } else {
      df <-
        prep_sample_information(df, "run_grouping", c(default_pairing,
                                                      "location"),
                                id_rep = max_sample,  same_rep = TRUE) %>%
        prep_sample_information("stochastic_run", default_pairing,
                                id_rep = max_sample)
    }
    print("-- Write output")
    file_name <-  paste0("data-processed/", model_id, "/", "2024-07-28", "-",
                         model_id, ".gz.parquet")
    if (model_id %in% c("team1-modela")) {
      df$value <- as.integer(df$value)
    } else {
      df$value <- round(df$value, 1)
    }
    arrow::write_parquet(df, file_name, compression = "gzip",
                         compression_level = 9)
  })
})
