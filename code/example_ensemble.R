rm(list = ls())
print(paste0("TOTAL Process start:", Sys.time()))
library(arrow)
library(magrittr)
library(dplyr)

# Function -------

#' Calculate LOP Ensemble(s) for peak targets
#'
#' Calculate LOP Ensemble (trimmed or untrimmed, with weight or not) for both
#' peak size hosp and peak time hosp by using
#' `CombineDistributions::aggregate_cdfs()`. The input file should match the
#' expected input format of this function.
#'
#' @param df_peak data frame containing the peak target information to calculate
#'  ensemble on, the data frame should be in the expected format
#' @param weighting_scheme string to indicate how to weight in the aggregate (
#'  see function documentation)
#' @param n_trim integer denoting the number of models to trim (
#'  see function documentation). If NA, no trim applied
#' @param weight.df data frame containing the weight information by team-model,
#'  should have 2 columns: `model_name` and `weight`.
#' @param ens_group character vector of group of column on which to calculate
#'  the ensemble (for example, aggregate by scenario, target, horizon, location,
#'  and age group for the FLU projections)
#'
#' @details
#' The function returns a data frame in the SMH standard format
#'
#'
#' @importFrom dplyr filter arrange mutate select
#' @importFrom CombineDistributions aggregate_cdfs
peak_ensemble <- function(df_peak, weighting_scheme, n_trim, weight.df,
                          ens_group, id_var = "model_name") {

  df_peak_time <- dplyr::filter(df_peak, grepl("time", target)) %>%
    dplyr::arrange(target_end_date)
  df_peak_size <- dplyr::filter(df_peak, grepl("size", target))
  if (is.na(n_trim)) {
    df_lop_peak_time <- CombineDistributions::aggregate_cdfs(
      df_peak_time, id_var = id_var,
      group_by = ens_group, method = "LOP", weighting_scheme = weighting_scheme,
      weights = weight.df, ret_quantiles = NA, reorder_quantiles = FALSE,
      ret_values = unique(df_peak_time$target_end_date)) %>%
      dplyr::mutate(target_end_date = value,
                    value = quantile,
                    output_type_id = make_epiweek(target_end_date),
                    output_type = "cdf",
                    horizon = NA) %>%
      dplyr::select(-quantile, -target_end_date)
    df_lop_peak_size <- CombineDistributions::aggregate_cdfs(
      df_peak_size, id_var = id_var,
      group_by = ens_group, method = "LOP", weighting_scheme = weighting_scheme,
      weights = weight.df, ret_quantiles = unique(df_peak_size$quantile),
      ret_values = NA)  %>%
      dplyr::mutate(horizon = NA,
                    output_type = "quantile",
                    output_type_id = quantile) %>%
      dplyr::select(-quantile)
  } else {
    df_lop_peak_time <- CombineDistributions::aggregate_cdfs(
      df_peak_time, id_var = id_var,
      group_by = ens_group, method = "LOP", weighting_scheme = weighting_scheme,
      n_trim = n_trim, ret_quantiles = NA, reorder_quantiles = FALSE,
      ret_values = unique(df_peak_time$target_end_date)) %>%
      dplyr::mutate(target_end_date = value,
                    value = quantile,
                    output_type_id = make_epiweek(target_end_date),
                    output_type = "cdf",
                    horizon = NA) %>%
      dplyr::select(-quantile, -target_end_date)
    df_lop_peak_size <- CombineDistributions::aggregate_cdfs(
      df_peak_size, id_var = id_var,
      group_by = ens_group, method = "LOP", weighting_scheme = weighting_scheme,
      n_trim = n_trim, ret_quantiles = unique(df_peak_size$quantile),
      ret_values = NA) %>%
      dplyr::mutate(horizon = NA,
                    output_type = "quantile",
                    output_type_id = quantile) %>%
      dplyr::select(-quantile)
  }
  df_lop_peak <- rbind(df_lop_peak_size, df_lop_peak_time)
  return(df_lop_peak)
}

# Prerequisite -----
list_quantiles <- c(0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4,
                    0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9,
                    0.95, 0.975, 0.99)
# vector of quantile value to integrate in the calculation of the ensemble

ens_group <- c("scenario_id", "target", "horizon", "origin_date", "location",
               "age_group")
# group of column on which to calculate the ensemble (for example, aggregate
# by scenario, target, horizon (target_end_date), location,
# and age group for the FLU projections)
quantile_vect = list_quantiles
peak_group = c("origin_date", "scenario_id", "location", "target",
               "age_group", "output_type_id")
all_target <- c("peak time hosp", "peak size hosp", "inc hosp", "cum hosp",
                "inc death", "cum death")

schema <- hubData::create_hub_schema(hubUtils::read_config(".", "tasks"))
schema <- c(schema$fields,
            arrow::Field$create("run_grouping", arrow::int64()),
            arrow::Field$create("stochastic_run", arrow::int64()))
schema <- arrow::schema(schema)

source("code/utils.R")
source("code/temp_calc_ens.R")

# Calculate Ensemble ------
message("# Quantile ensemble")
print(paste0("Process quantile ensemble start:", Sys.time()))
df <- hubData::connect_model_output("output-processed/",
                                    partition_names = c("model_id",
                                                        "origin_date", "target",
                                                        "location"),
                                    file_format = "parquet",
                                    schema = schema) %>%
  dplyr::filter(origin_date == "2024-04-28", output_type == "quantile") %>%
  dplyr::collect() %>%
  dplyr::select(-run_grouping, -stochastic_run) %>%
  janitor::remove_empty(which = c("rows", "cols"))

ens_func = function(x, y) {
  matrixStats::weightedMedian(x, w = y, na.rm = TRUE) }

weight.df = data.frame(
  model_id = unique(df$model_id),
  weight = 1)

df$output_type_id <- as.numeric(df$output_type_id)

time_ens <- system.time({
  df_hv <- hubData::as_model_out_tbl(df)
  ens <- hubEnsembles::simple_ensemble(df_hv, weight.df, agg_fun = "median")
  ens <- dplyr::select(ens, -model_id)
})
print("Time Ensemble:")
print(time_ens)
time_ens_lop <- system.time({
  df_lop <- ensemble_lop(df, list_quantiles,
                         weights = weight.df,
                         weighting_scheme = "cdf_exterior",
                         n_trim = 2, ens_group = ens_group,
                         id_var = "model_id")

})
print("Time Ensemble LOP:")
print(time_ens_lop)
time_ens_lop_un <- system.time({
  df_unlop <- ensemble_lop(df, list_quantiles, weights = weight.df,
                           weighting_scheme = "user_defined",
                           n_trim = NA, ens_group = ens_group,
                           id_var = "model_id")
})
print("Time Ensemble LOP untrimmed:")
print(time_ens_lop_un)
print(paste0("Process quantile ensemble stop:", Sys.time()))

# Peak ensemble
message("# Peak ensemble")
print(paste0("Process peak ensemble start:", Sys.time()))
df <- hubData::connect_model_output("output-processed/",
                                    partition_names = c("model_id",
                                                        "origin_date", "target",
                                                        "location"),
                                    file_format = "parquet",
                                    schema = schema) %>%
  dplyr::filter(origin_date == "2024-04-28", grepl("peak", target)) %>%
  dplyr::collect() %>%
  dplyr::select(-run_grouping, -stochastic_run, -race_ethnicity)


# Peak time
df_time <- dplyr::filter(df, grepl("peak time", target)) %>%
  dplyr::mutate(
    quantile = value,
    target_end_date = MMWRweek::MMWRweek2Date(
      as.numeric(substr(gsub("EW", "", output_type_id), 1, 4)),
      as.numeric(substr(gsub("EW", "", output_type_id), 5, 6)), 7
    ),
    value = as.numeric(MMWRweek::MMWRweek2Date(
      as.numeric(substr(gsub("EW", "", output_type_id), 1, 4)),
      as.numeric(substr(gsub("EW", "", output_type_id), 5, 6)), 7
    ))) %>%
  dplyr::arrange(target_end_date)
# Peak size
df_size <- dplyr::filter(df, grepl("peak size", target),
                         !is.na(output_type_id)) %>%
  dplyr::arrange(output_type_id) %>%
  dplyr::filter(output_type_id %in% list_quantiles) %>%
  dplyr::mutate(quantile = output_type_id,
                target_end_date = as.Date(NA))
# All
df_peak <- rbind(df_time, df_size)
df_lop_peak <- peak_ensemble(df_peak, weighting_scheme =  "cdf_exterior",
                             n_trim = 2, weight.df = weight.df,
                             ens_group = ens_group, id_var = "model_id")
df_unlop_peak <- peak_ensemble(df_peak, weighting_scheme = "user_defined",
                               n_trim = NA, weight.df = weight.df,
                               ens_group = ens_group, id_var = "model_id")

print(paste0("Process peak ensemble stop:", Sys.time()))

# Write output
if (!dir.exists("output-processed/Ensemble/"))
  dir.create("output-processed/Ensemble/")
print("Write Ensemble ...")
print(head(ens))
arrow::write_dataset(ens, paste0("output-processed/Ensemble/"),
                     partitioning = c("origin_date", "target", "location"),
                     hive_style = FALSE, compression = "gzip",
                     compression_level = 9,
                     basename_template = "Ensemble{i}.gz.parquet")

df_lop_tot <- rbind(df_lop, df_lop_peak)
if (!dir.exists("output-processed/Ensemble_LOP/"))
  dir.create("output-processed/Ensemble_LOP/")
print("Write Ensemble LOP ...")
print(head(df_lop_tot))
arrow::write_dataset(df_lop_tot, paste0("output-processed/Ensemble_LOP/"),
                     partitioning = c("origin_date", "target", "location"),
                     hive_style = FALSE, compression = "gzip",
                     compression_level = 9,
                     basename_template = "Ensemble_LOP{i}.gz.parquet")

df_unlop_tot <- rbind(df_unlop, df_unlop_peak)
if (!dir.exists("output-processed/Ensemble_LOP_untrimmed/"))
  dir.create("output-processed/Ensemble_LOP_untrimmed/")
print("Write Ensemble LOP untrimmed ...")
print(head(df_unlop_tot))
arrow::write_dataset(df_unlop_tot,
                     paste0("output-processed/Ensemble_LOP_untrimmed/"),
                     partitioning = c("origin_date", "target", "location"),
                     hive_style = FALSE, compression = "gzip",
                     compression_level = 9,
                     basename_template = "Ensemble_LOP_untrimmed{i}.gz.parquet")

print(paste0("TOTAL Process stop:", Sys.time()))
rm(list = ls())
