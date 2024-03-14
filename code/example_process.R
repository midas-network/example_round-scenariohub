library(arrow)
library(magrittr)
library(dplyr)

# Prerequisite -----
list_quantiles <- c(0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4,
                    0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9,
                    0.95, 0.975, 0.99)
# vector of quantile value to integrate in the calculation of the ensemble

group_column <- c("scenario_id", "target", "origin_date", "location",
                  "model_name", "horizon")
# group of column defining individual groups (for example, expect to have
# one projection per scenario, target, horizon (target_end_date), location,
# model and age group for the FLU projections)

ens_group <- c("scenario_id", "target", "horizon", "origin_date", "location",
               "age_group")
# group of column on which to calculate the ensemble (for example, aggregate
# by scenario, target, horizon (target_end_date), location,
# and age group for the FLU projections)

cumul_group = c("origin_date", "scenario_id", "location", "target",
                "output_type", "output_type_id", "age_group")
quant_group = c("origin_date", "scenario_id", "location", "target",
                "horizon", "age_group")
quantile_vect = list_quantiles
peak_group = c("origin_date", "scenario_id", "location", "target",
               "age_group", "output_type_id")
all_target <- c("peak time hosp", "peak size hosp", "inc hosp", "cum hosp",
                "inc death", "cum death")

schema <- arrow::schema(
  "origin_date" = arrow::string(),
  "target" = arrow::string(),
  "model_name" = arrow::string(),
  "scenario_id" = arrow::string(),
  "location" = arrow::string(),
  "horizon" = arrow::int64(),
  "age_group" = arrow::string(),
  "output_type" = arrow::string(),
  "output_type_id" = arrow::string(),
  "value" = arrow::float(),
  "run_grouping" = arrow::int64(),
  "stochastic_run" = arrow::int64()
)
source("code/utils.R")
source("code/temp_calc_ens.R")

# Add missing value ------
list_team <- grep("README", dir("data-processed/"), value = TRUE, invert = TRUE)
lapply(list_team, function(x) {
  a_time <- system.time({
    test <-
      arrow::open_dataset("data-processed/",
                          partitioning = c("model_name", "origin_date",
                                           "target"),
                          factory_options = list(exclude_invalid_files = TRUE),
                          schema = schema) %>%
      dplyr::filter(origin_date == "2024-04-28", output_type == "sample",
                    model_name == x) %>%
      dplyr::collect()

    df_team <- test %>%
      dplyr::mutate_if(is.factor, as.character) %>%
      dplyr::mutate(origin_date = as.Date(origin_date))

    df_team <- dplyr::mutate(
      df_team,
      output_type_id = ifelse(
        output_type == "sample",
        as.numeric(as.factor(paste0(run_grouping, "-", stochastic_run))),
        output_type_id))
    df_team <- dplyr::select(df_team, -run_grouping, -stochastic_run,
                             -model_name)

    ## Weekly incident and cumulative target ---
    df_all <-
      make_quantiles(df_team,
                     quant_group = c("origin_date", "scenario_id", "location",
                                     "target", "horizon",
                                     "age_group"),
                     cumul_group = c("origin_date",  "scenario_id",
                                     "location", "target", "output_type",
                                     "output_type_id",
                                      "age_group"),
                     keep_cumul = TRUE)

    ## Peak target ----
    # Time
    peak_time_target <- grep("peak time", all_target, value = TRUE)
    df_all <- calculate_peak_time(
      df_team, df_all, x, peak_group = peak_group,
      peak_time_target = peak_time_target)


    # Peak size
    peak_size_target <- grep("peak size", all_target, value = TRUE)
    df_all <- calculate_peak_size(
      df_team, df_all, x, peak_size_target = peak_size_target,
      peak_group = peak_group, quantile_vect = quantile_vect)

    # Standardization
    df_all <- dplyr::mutate(df_all, origin_date = as.Date(origin_date))

    # Write and check output with SMHvalidation package
    arrow::write_dataset(df_all, paste0("data-processed/", x, "/"),
                         partitioning = c("origin_date", "target"),
                         hive_style = FALSE, compression = "gzip",
                         compression_level = 9,
                         basename_template = paste0(x,
                                                    "{i}.gz.parquet"))
    #SMHvalidation::validate_submission(
    #  write_path, js_def = "hub-config/tasks.json", lst_gs = NULL,
    #  pop_path = "data-locations/locations.csv")
  })
  print("###########")
  print(x)
  print(a_time)
  NULL
})


# Calculate Ensemble ------
df <- arrow::open_dataset("data-processed/",
                          partitioning = c("model_name", "origin_date",
                                           "target"),
                          factory_options = list(exclude_invalid_files = TRUE),
                          schema = schema) %>%
  dplyr::filter(origin_date == "2024-04-28", output_type == "quantile") %>%
  dplyr::collect() %>%
  dplyr::select(-run_grouping, -stochastic_run)

ens_func = function(x, y) {
  matrixStats::weightedMedian(x, w = y, na.rm = TRUE) }

weight.df = data.frame(
  model_name = unique(df$model_name),
  weight = 1)

Sys.time()
system.time({
  df_tot$output_type_id <- as.numeric(df$output_type_id)
  ens <- calculate_ensemble(df, ens_func, ens_group = ens_group,
                            weight.df = weight.df)
  if (!dir.exists("data-processed/Ensemble/"))
    dir.create("data-processed/Ensemble/")
  arrow::write_dataset(ens, paste0("data-processed/Ensemble/"),
                       partitioning = c("origin_date", "target"),
                       hive_style = FALSE, compression = "gzip",
                       compression_level = 9,
                       basename_template = "Ensemble{i}.gz.parquet")
  df_lop <- ensemble_lop(df, list_quantiles,
                         weights = weight.df,
                         weighting_scheme = "cdf_exterior",
                         n_trim = 2, ens_group = ens_group)
  if (!dir.exists("data-processed/Ensemble_LOP/"))
    dir.create("data-processed/Ensemble_LOP/")
  arrow::write_dataset(df_lop, paste0("data-processed/Ensemble_LOP/"),
                       partitioning = c("origin_date", "target"),
                       hive_style = FALSE, compression = "gzip",
                       compression_level = 9,
                       basename_template = "Ensemble_LOP{i}.gz.parquet")
  df_unlop <- ensemble_lop(df, list_quantiles, weights = weight.df,
                           weighting_scheme = "user_defined",
                           n_trim = NA, ens_group = ens_group)
  if (!dir.exists("data-processed/Ensemble_LOP_untrimmed/"))
    dir.create("data-processed/Ensemble_LOP_untrimmed/")
  arrow::write_dataset(df_unlop,
                       paste0("data-processed/Ensemble_LOP_untrimmed/"),
                       partitioning = c("origin_date", "target"),
                       hive_style = FALSE, compression = "gzip",
                       compression_level = 9,
                       basename_template = "Ensemble_LOP_untrimmed{i}.gz.parquet")
})
Sys.time()

