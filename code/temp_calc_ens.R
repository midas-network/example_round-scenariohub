# ensemble_preprocess ##########################################################
#' Aggregate submission files with model name
#'
#' Read all the files in a path containing the model ordered by folder.
#' Aggregate them all in one data frame and add one columns with model name
#' (extracted from the file name inputted)
#'
#' @param list_files, list of path to files to read (files should be in the SMH
#'  standard format)
#' @param loc_dictionary_name named vector, with the location name as value and
#' corresponding FIPS (or US for US level) as associated names.
#' @param location2number named vector, with the FIPS (or US for US level) as
#' value and corresponding names as associated names.
#'
#' @details Both named vectors are used to verify that the `location` column
#' is correctly filled.
#' The `model_name` is created by extraction the value from the file(s) name(s)
#' inputted in the parameter `list_files`. The file should be called:
#' "DATE-MODELNAME.csv" (zip, gz and pqt files are also accepted)
#'
#' @noRd
#' @importFrom dplyr filter mutate select
#' @importFrom purrr reduce
#' @importFrom stats setNames
list_model <- function(list_files, loc_dictionary_name, location2number) {
  lst_ds <- lapply(list_files, function(y) {
    df <- read_files(y)
    df <- dplyr::filter(df, !is.na(output_type_id),
                        output_type == "quantile") %>%
      dplyr::mutate(
        output_type_id = round(as.numeric(output_type_id), 3),
        origin_date = as.Date(origin_date),
        model_name = gsub(
          ".{4}-.{2}-.{2}-|.csv|.zip|.gz|.pqt|.parquet|(-)?quantile", "",
          basename(y)),
        location_name = loc_dictionary_name[as.character(location)],
        location =  ifelse(nchar(location) == 1, paste0("0", location),
                           location)) %>%
      dplyr::mutate(location2 = location2number[location_name])
    if (!(all(df$location == df$location2)))
      stop("Location translation error, please check `location` column")
    df %>% dplyr::select(-location_name, -location2)
  })
  lst_ds <- setNames(lst_ds, basename(list_files))
  lst_model <- purrr::reduce(lst_ds, rbind) %>%
    dplyr::mutate(value = round(value, 10))
  return(lst_model)
}

#' Filter Models by quantiles
#'
#' Remove models that does not provide all the expected number of quantiles
#' required in the Scenario Modeling Hub (SMH) standard and add a `round` column
#'
#' @param df data frame containing all the submissions
#' @param group_column character vector, names of the column to "group_by" the
#' df to calculate the number of quantiles in the submission. See Details' for
#' more information
#' @param n_quantile numeric, expected number of quantiles
#' @param scenario_sel named vector with scenario_id as value and round
#' information corresponding as name. For example, "A-2021-11-09" value, with
#' "round10" as name.
#'
#' @details The 'group_column' parameter is used to estimate the number
#' of quantiles in the submission. For example, the SMH standard contains 23
#' quantiles per each group of scenario, target, location and model. In this
#' case, the `group_column` parameter can be set to `c("scenario_id",
#' "scenario_name", "target", "target_end_date", "location", "model_name")`, and
#' the `n_quantile` parameter can be set to `23`. \cr\cr
#' A column `round` is also added to the output, it allows to detect submission
#' with projection for past round scenario.
#' @noRd
#'
#' @importFrom dplyr group_by across mutate filter select ungroup
filter_quantiles <- function(df, group_column,  n_quantile, scenario_sel) {
  df_model_ensemble <- df %>%
    # dplyr::group_by(all_of(group_column)) %>%
    dplyr::group_by(across({{ group_column }})) %>%
    dplyr::mutate(sel = ifelse(length(output_type_id) == n_quantile, 1, 0)) %>%
    dplyr::filter(sel == 1) %>%
    dplyr::mutate(round =  names(scenario_sel[which(
      scenario_sel %in% scenario_id)])) %>%
    dplyr::select(-sel) %>%
    dplyr::ungroup()

  if (dim(df_model_ensemble)[[1]] != dim(df)[[1]])
    print("Input submission filtered - missing quantiles")

  return(df_model_ensemble)
}

#' Flag non-shared projection dates
#'
#' Flag the model projection date that are not common in all the models
#' projections.
#'
#' @param df data frame containing all the submissions
#' @noRd
#' @importFrom dplyr select distinct group_by summarize mutate ungroup
flag_ncdate <- function(df) {
  sel_df <- df %>%
    dplyr::select(horizon, model_name, round) %>%
    dplyr::distinct() %>%
    dplyr::group_by(horizon, round) %>%
    dplyr::summarize(n_model = dplyr::n(), .groups = "drop") %>%
    dplyr::group_by(round) %>%
    dplyr::mutate(sel = ifelse(n_model == max(n_model), 0 ,1)) %>%
    dplyr::ungroup()
  return(sel_df)
}


#' Filter non-shared projection dates
#'
#' Remove the model projection date that are not shared by all the models
#' projections.
#'
#' @param df data frame containing all the submissions
#' @noRd
#' @importFrom dplyr filter bind_rows
filter_dates <- function(df) {
  sel_df <- flag_ncdate(df)
  round_n <- unique(sel_df$round)
  sel_df <- dplyr::filter(sel_df, sel == 1)
  df_model_ensemble <- lapply(round_n, function(x) {
    df_ens <- dplyr::filter(df, round == x)
    sel_ndate <- sel_df[which(sel_df$round == x), "horizon", TRUE]
    df_ens <- df_ens[which(!(df_ens$horizon %in% sel_ndate)), ]
  }) %>% dplyr::bind_rows()

  if (dim(df_model_ensemble)[[1]] != dim(df)[[1]])
    print("Input submission filtered - rm uncommun date")

  return(df_model_ensemble)
}


#' Prepare Submission for Ensemble Calculation
#'
#' Regroup all the submissions for a specific round of the Scenario Modeling
#' Hub (SMH) in one data frame that is filtered to conserved only the required
#' target, quantiles and possibility exclude one or multiple models (not
#' providing all the required quantiles, or by name), and select the projection
#' dates shared between all the submissions.
#'
#' @param path path to the folder containing all the submissions sub-floder
#' @param round_path path the a csv with round information. NUll by default
#' @param round numeric to select data for a specific round number, round number
#'  should be written  like "1", "5", ... and the parameter `round_path` should
#'  be filled too. If NULL (default), all round selected
#' @param excl_model string character, name of the model to exclude. If
#' multiple, please write as "model1|model2". By default, NULL (no exclusion)
#' @param req_target character vector, names of the required targets
#' @param list_quantiles vector of the required quantiles
#' @param group_column character vector, names of the column to "group_by" the
#' data frame of all submissions to calculate the number of quantiles in each
#' group. See Details' for more information
#' @param scenario_sel named vector with scenario_id as value and round
#' information corresponding as name. For example, "A-2021-11-09" value, with
#' "round10" as name.
#' @param loc_dictionary_name named vector, with the location name as value and
#' corresponding FIPS (or US for US level) as associated names.
#' @param location2number named vector, with the FIPS (or US for US level) as
#' value and corresponding names as associated names.
#' @param name_quantile Boolean, if TRUE includes only file with "quantile." in
#'  the name of the file.
#'
#'
#' @details The 'group_column' parameter is used to estimate the number
#' of quantiles in each group in all the submission. For example, the SMH
#' standard contains 23 quantiles per each group of scenario, target, location
#' and model. In this case, the `group_column` parameter can be set to
#' `c("scenario_id", "scenario_name", "target", "target_end_date", "location",
#' "model_name")`
#' and the `list_quantiles` should be a vector of 23 values.  \cr\cr
#' A column `round` is added to the output, it allows to detect submission
#' with projection for past round scenario. To identify the round/scenario
#' information, the paramet `scenario_sel` is used. \cr\cr
#' Both named vectors (`loc_dictionary_name` and `location2number`) are used to
#' verify that the `location` column is correctly filled.
#'
#' @export
#' @importFrom dplyr filter distinct arrange
ensemble_preprocess <- function(path, round, round_path, excl_model, req_target,
                                list_quantiles, group_column, scenario_sel,
                                loc_dictionary_name, location2number,
                                name_quantile = FALSE) {
  # List of files to read per round
  name_model <- path_files(path, ensemble = FALSE, round_path = round_path,
                           round = round, name_quantile = name_quantile)
  # Files exclusion if necessary
  if (!is.null(excl_model)) {
    name_model <- grep(excl_model, name_model, value = TRUE, invert = TRUE)
  }
  # Read, process all submissions
  df_model <- list_model(name_model, loc_dictionary_name, location2number)
  # Keep only required targets
  df_model <- dplyr::filter(df_model,
                            grepl(paste(req_target, collapse = "|"), target))
  print(unique(df_model$target)) # to be removed
  # Filter only required quantiles and common projection dates
  df_model <- dplyr::filter(df_model, output_type_id %in% list_quantiles)

  df_model_ensemble <- filter_quantiles(
    df_model, group_column, n_quantile = length(list_quantiles),
    scenario_sel = scenario_sel) %>%
    filter_dates() %>%
    dplyr::distinct() %>%
    dplyr::arrange(scenario_id, model_name, target, output_type_id, location)
  # Print name of the model(s) included in the output
  print(paste0("Model(s) included in the output: ",
               paste(unique(df_model_ensemble$model_name), collapse = ", ")))
  return(df_model_ensemble)
}

# calculate_ensemble ###########################################################
#' Apply function on a specific quantile
#'
#' For a specific quantile, summarize all the models value by applying the
#' function inputted in the parameter `ens_func`, the function should take 2
#' parameters: "value" (1st) and "weight" (2nd). For more information, please
#' see details.
#'
#' @param df data frame to summarize
#' @param quantile numeric, specific quantile to filter and summarize on
#' @param type string, type of the quantile (either "point" or "quantile")
#' @param quantile_name numeric, value of the quantile
#' @param ens_func function to apply (should have 2 parameters)
#'
#' @details The ensemble functions is applied on 2 parameters: value (1st) and
#' weight (2nd) so the input `df` should have at least these columns, the
#' quantile column and both target_end_date and target columns. \cr
#' The inputted data frame can also be grouped by other columns
#' but the groups will be dropped after summarizing.  \cr
#' An example of function that can be used to calculate the ensemble is:
#' ```
#' ens_func <- function(x, y) matrixStats::weightedMedian(x, y, na.rm = TRUE)
#' ```
#' \cr\cr
#' After applying the function, some columns are added to have a data frame
#' output following the Scenario Modeling Hub standard. We add: `type`,
#' `quantile` and `model_projection_date` columns. They are created by using
#' the parameter `type`, `quantile_name`.
#'
#' @noRd
#' @importFrom dplyr filter summarize ungroup mutate
#' @importFrom rlang !!
ensemble_quantile <- function(df, quantile, type, quantile_name, ens_func) {

  df <- df %>%
    dplyr::filter(output_type_id == !!quantile) %>%
    dplyr::summarize(value = ens_func(value, weight), .groups = "drop") %>%
    dplyr::ungroup() #%>%

  df[, "output_type"] <- type
  df[, "output_type_id"] <- quantile_name
  return(df)
}

#' Calculate Ensembles by quantile
#'
#' For each quantile, summarize all the models value on a grouped by data frame
#'  by applying the function inputted in the parameter `ens_func`. The function
#' should take 2  parameters: "value" (1st) and "weight" (2nd). For more
#' information, please see details.
#'
#' @param df data frame to summarize
#' @param ens_func function to apply (should have 2 parameters)
#' @param ens_group vector of column names to `group_by` the data frame `df`
#' @param weight.df data frame containing the weight column information, will be
#' join with `df` and the `weight ` column will be used as second parameter in
#' the `ens_func`.
#'
#' @details The ensemble functions is applied on 2 parameters: value (1st) and
#' weight (2nd) so the inputs `df` and `weight.df` (will be left_join in the
#' function) should have at least the columns `value` and `weight` and a common
#' column to join them. \cr
#' The inputted data frame can also be grouped by multiple columns inputted in
#' the `ens_group` parameter. The groups will be dropped after summarizing.  \cr
#' An example of function that can be used to calculate the ensemble is:
#' ```
#' ens_func <- function(x, y) matrixStats::weightedMedian(x, y, na.rm = TRUE)
#' ```
#' \cr\cr
#' Some columns are required to have a data frame output following the Scenario
#' Modeling Hub standard. Please refer to the documentation on the Scenario
#' Modeling Hub repositories or websites.
#'
#' @importFrom dplyr left_join group_by bind_rows mutate all_of
#' @export
calculate_ensemble <- function(df, ens_func, ens_group, weight.df) {
  df <- dplyr::left_join(df, weight.df) %>%
    dplyr::group_by(across(all_of(ens_group)))
  # dplyr::group_by(across({{ ens_group }}))
  df_ensemble <- lapply(unique(df$output_type_id), function(x) {
    ensemble_quantile(df, x, "quantile", x, ens_func)
  }) %>% dplyr::bind_rows() %>%
    dplyr::mutate(output_type_id = round(output_type_id, 3))
}

# ensemble_lop #################################################################
#' Calculate LOP Ensembles by quantile
#'
#' Calculate LOP Ensemble by using the `CombineDistributions` package, and the
#' function `aggregate_cdfs`. For more information, please look at the package
#' documentation.
#'
#' @param df data frame that contains multiple cdfs, grouped by `ens_group`
#' columns. Specify cdf with `quantile` and `value` columns
#' @param list_quantiles vector of quantiles to use for calculation
#' @param weights data frame containing the weight column information
#' @param ens_group vector containing the names of the columns to create unique
#' aggregates for
#' @param weighting_scheme character name of the method for aggregation.
#' @param n_trim integer denoting the number of models to trim
#'
#' @importFrom dplyr ungroup left_join distinct select mutate
#' @importFrom CombineDistributions aggregate_cdfs
#'
#' @export
ensemble_lop <- function(df, list_quantiles, weights, ens_group,
                         weighting_scheme, n_trim) {
  df <- dplyr::ungroup(df) %>%
    rename(quantile = output_type_id)
  # Calculate LOP
  if (is.na(n_trim)) {
    df_lop <- CombineDistributions::aggregate_cdfs(
      df, id_var = "model_name", group_by = ens_group, method = "LOP",
      ret_quantiles = list_quantiles, weights = weights, ret_values = NA,
      weighting_scheme = weighting_scheme)
  } else {
    df_lop <- CombineDistributions::aggregate_cdfs(
      df, id_var = "model_name", group_by = ens_group, method = "LOP",
      ret_quantiles = list_quantiles, weighting_scheme = weighting_scheme,
      n_trim = n_trim, ret_values = NA)
  }
  # add missing columns
  df_lop <- df_lop %>%
    dplyr::mutate(
      output_type = "quantile",
      output_type_id = round(quantile, 3)) %>%
    dplyr::select(-quantile)

  return(df_lop)

}

# df_lop_weight ################################################################
#' Calculate weighted LOP Ensembles by quantile
#'
#' Calculate weighted LOP Ensemble by using the `psSLP` package, and the
#' functions `generate_slp_predictions`, `generate_slp_ensemble`. For more
#' information, please look at the package documentation.
#'
#' @param df data frame that contains multiple cdfs
#' @param weight.df data frame containing the weight column information
#' @param draw_size (default = 2000), an integer value of the number of
#' simulated forecasts to make for each quantile function set [for
#' `generate_slp_predictions` function]
#' @param threads default 4, set integer number of threads to use for parallel
#' processing [for `generate_slp_predictions` function]
#' @param verbose (default = TRUE) a boolean indicator to show verbose status
#' updates during calculation [for `generate_slp_predictions` function]
#' @param parallel (default = "on" )string value either "auto","on","off". By
#' default, the function will run in parallel using half the available threads
#' if the number of gam models to estimates exceeds 1000 (this is "auto") mode.
#' Setting this to "on" ("off") will force parallel processing on ("off")
#' regardless of gam models [for `generate_slp_predictions` function]
#' @param fixed_unif (default = TRUE), if set to TRUE this will ensure that the
#' same set of uniform variates will be drawn for each run of the estimation
#' function; rather than pulling runif(draw_size, unif_range[0],
#' unif_range[1]), this option will pull seq(unif_range[0], unif_range[1],
#' length.out = draw_size) [for `generate_slp_predictions` function]
#' @param byvars default = `c("scenario_id", "target_end_date", "model_name",
#' "outcome")`), a character vector indicating the grouping variables. Each
#' unique group should represent a single quantile function (Q(p)). The
#' calculation is run by location too by default. [for
#' `generate_slp_predictions` function]
#' @param ens_vars (default = `c("scenario_id", "target_end_date", "outcome")`),
#' a character vector indicating the variables that uniquely define a set of
#' predictions. The calculation is run by location too by default. for
#' `generate_slp_ensemble` function]
#' @param output_col (default = `c("scenario_id", "scenario_name",
#' "target", "target_end_date", "location", "value", "type", "quantile",
#' "model_projection_date")`), selection of columns to include in the output.
#'
#' @export
#' @importFrom dplyr select distinct mutate filter left_join bind_rows
#' @importFrom psSLP generate_slp_predictions generate_slp_ensemble
#' @importFrom magrittr %>%
#' @importFrom data.table .SD .N
df_lop_weight <- function(df, weight.df,
                          draw_size=2000, threads = 4,  verbose=T,
                          parallel ="on", fixed_unif = TRUE,
                          byvars = c("scenario_id", "horizon", "origin_date",
                                     "model_name", "target"),
                          ens_vars = c("scenario_id", "horizon", "origin_date",
                                       "target"),
                          output_col = c("scenario_id",
                                         "target", "horizon",
                                         "location", "value", "type",
                                         "type_id", "origin_date")) {
  # prepare information for output
  #wk_number <- dplyr::select(df, horizon, target, origin_date) %>%
  #  dplyr::distinct() %>%
  #  dplyr::mutate(target_wk = gsub("[^[:digit:]]", "", target)) %>%
  #  dplyr::select(target_end_date, target_wk) %>%
  #  dplyr::distinct()
  # run by location, outcome
  df_lop_pred <- df
  df_lop <- lapply(unique(df_lop_pred$location), function(x) {
    df_lop_out <- lapply(unique(df_lop_pred$target), function(y) {
      print(paste(x, y, collapse = ", ")) #to be removed
      df <- dplyr::filter(df_lop_pred, grepl(y, target), location == x)
      if (dim(df)[1] > 0) {
        preds <- lapply(unique(df$model_name), function(z) {
          dfmod <- dplyr::filter(df, grepl(paste0("^", z, "$"), model_name))
          draw_size_mod <- draw_size *
            weight.df[which(weight.df$model_name == z), "weight"]
          print(paste0("Model:", unique(dfmod$model_name),
                       ", draws:", draw_size_mod))
          if (grepl("cum case", y)) {
            preds <- psSLP::generate_slp_predictions(
              x = dfmod, qp_var = "value", p_var = "output_type_id",
              byvars = byvars, draw_size = draw_size_mod, threads = threads,
              verbose = verbose, parallel = parallel, fixed_unif = fixed_unif)
          } else {
            preds <- psSLP::generate_slp_predictions(
              x = dfmod, qp_var = "value", p_var = "output_type_id",
              byvars = byvars, draw_size = draw_size_mod, threads = threads,
              verbose = verbose, parallel = parallel, fixed_unif = fixed_unif,
              model_configuration = list(type = "gam",
                                         gam_con = list("family" = "poisson")))
          }
        }) %>% dplyr::bind_rows()
        preds <- dplyr::left_join(preds, weight.df, by = "model_name")
        if (length(unique(preds$model_name)) > 1) {
          trim_level <- 2 / (unique(preds[, .(model_name,
                                              weight)])[, sum(weight)])
        } else {
          trim_level <- 0
        }
        trimmed_ensemble <- psSLP::generate_slp_ensemble(
          preds, qp_var = "predictions", trim = trim_level,
          byvars = ens_vars)
      } else {
        trimmed_ensemble <- NULL
      }
      gc()
      return(trimmed_ensemble)
    }) %>% dplyr::bind_rows()
    df_lop_out <- dplyr::mutate(
      df_lop_out,
      location = as.character(x),
      output_type = "quantile",
      value = predictions) %>%
      dplyr::select(!!output_col)
  }) %>% dplyr::bind_rows()
  return(df_lop)
}

# write_ensemble ###############################################################
#' Write Ensemble files
#'
#' Write Ensembles output by round information (in CSV or ZIP depending on the
#' size of the output, or PARQUET). If necessary, create the folder
#' corresponding
#'
#' @param df_ensemble Ensemble data frame
#' @param path_model path to the folder containing the Ensemble sub-folder
#' @param ens_name name of the Ensemble (by default "Ensemble")
#' @param ext file extension of the file ("csv" or ".parquet")
#'
#' @details The "Ensemble" object will be store in a CSV (ZIP if the CSV is
#' greater than 100 MB) or PARQUET file, in the sub-folder "Ensemble" in the
#' folder corresponding to the path_model. If the ens_name is change to another
#' name, the name of the file and the sub-folder will correspond to that new
#' name.
#' \cr\cr
#' The Ensemble file name is created by extracting the "model_projection_date"
#' information in the `df_ensemble`. The file name will be:
#' "`MODEL_PROJECTION_DATE`-`ENS_NAME`.csv". If the Ensemble data frame contains
#' multiple model_projection_date, multiple files will be created: one for each.
#'
#' @importFrom dplyr filter
#' @export
write_ensemble <- function(df_ensemble, path_model, ens_name = "Ensemble",
                           ext = ".csv") {
  lapply(unique(df_ensemble$origin_date), function(x) {
    df <- dplyr::filter(df_ensemble, grepl(x, origin_date))
    name_file <- paste0(path_model, ens_name, "/", x, "-", ens_name, ext)
    if (!dir.exists(paste0(path_model, ens_name, "/"))) {
      print(paste0("Creating Ensemble folder: ",
                   paste0(path_model, ens_name, "/")))
      dir.create(paste0(path_model, ens_name, "/"))
    }
    write_files(df, name_file, ext = ext)
  })
}
# Functions --------------------------------------------------------------------

#' Calculate the epiweek in "EWYYYWW" format
#'
#' For a specific date or a vector of date, calculate the epiweek in a
#' "EWYYYYWW" format
#'
#' @param date date or vector of date to transform
#'
#' @export
#' @importFrom MMWRweek MMWRweek
make_epiweek <- function(date) {
  epi_date <- MMWRweek::MMWRweek(date)
  epi_year <- epi_date[["MMWRyear"]]
  epi_week <- as.character(epi_date[["MMWRweek"]])
  epi_week <- ifelse(nchar(epi_week) < 2, paste0(0, epi_week), epi_week)
  epi_info <- paste0("EW", epi_year, epi_week)
  return(epi_info)
}

#' Calculate missing optional target value
#'
#' For rounds with samples output_type, the cumulative trajectories are not
#' expected and the quantiles are also not expected. This function test for
#' weekly incident and cumulative target if all the possible values
#' are presents and if not generate them for a specific team_model projection
#'
#' @param df_team model projection of a team-model
#' @param weekly_targ_name vector of weekly incident, cummulative target name
#' @param team_model name of the team-model
#' @param cumul_group character vector of column names to 'group_by' the
#' input data frame to calculate the cumulatives trajectories for each group
#' @param quant_group character vector of column names to 'group_by' the input
#' data frame to calculate the quantiles for each group
#' @param quantile_vect numeric vector of probabilities with values in [0,1],
#' to produce quantiles corresponding to the given probabilities
#'
#' @details
#' ## Weekly incident and cumulative target ---
#'  For each target (hosp or death), check that both sample and
#'  quantiles information are available:
#'  1. if sample target(s) is missing:
#'    - if cumulative is missing: calculates from incident trajectories
#'    - if incident: returns a warning message and doesn't calculate cumulative
#'      sample and quantiles
#'  2. if quantiles target(s) is missing:
#'    - calculates missing quantiles from associated trajectories
#'  3. aggregate all
#'
#' @importFrom ScenarioModelingHub.data path_file make_quantiles
#' @importFrom dplyr filter bind_rows
calculate_inccum_weekly <- function(
    df_team, weekly_targ_name, team_model,
    cumul_group = c("origin_date", "scenario_id", "location", "target",
                    "output_type", "output_type_id", "age_group"),
    quant_group = c("origin_date", "scenario_id", "location", "target",
                    "horizon", "age_group"),
    quantile_vect = c(0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4,
                      0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9,
                      0.95, 0.975, 0.99)) {

  lst_all <- lapply(weekly_targ_name, function(targ) {
    df_targ <- dplyr::filter(df_team, grepl(paste0("(inc|cum) ", targ), target))
    # Test sample first
    df_targ_sample <- dplyr::filter(df_targ, output_type == "sample")
    if (nrow(df_targ_sample) < 1) {
      warning("The submission file from: ", team_model,
              " is missing sample for the target: ", targ,
              ". The missing values cannot be calculated.")
      return(df_targ)
    } else {
      if (!any(grepl("cum ", df_targ_sample$target))) {
        message("The submission file from: ", team_model,
                " is missing cumulative sample the target: ", targ,
                ". The missing value will be calculated.")
        # Calculate cumulative
        # - Sample
        inc_sample_targ <- dplyr::filter(df_targ_sample,
                                         target == paste0("inc ", targ))
        df_targ_sample <- ScenarioModelingHub.Data::make_quantiles(
          inc_sample_targ, quant_group = NULL, quantile_vect = NULL,
          cumul_group = cumul_group)
      }
    }
    # Test quantile
    df_targ_quantile <- dplyr::filter(df_targ, output_type == "quantile")
    if (nrow(df_targ_quantile) < 1) {
      message("The submission file from: ", team_model,
              " is missing all quantiles for the target: ", targ,
              ". The missing value will be calculated.")
      df_all_targ <- ScenarioModelingHub.Data::make_quantiles(
        df_targ_sample, quant_group = quant_group,
        quantile_vect = quantile_vect, cumul_group = NULL)
    } else {
      if (any(grepl("cum", df_targ_quantile$target)) &
          any(grepl("inc", df_targ_quantile$target))) {
        message("The submission file from: ", team_model, " contains all the ",
                "quantiles for the target: ", targ)
        df_all_targ <- rbind(df_targ_quantile, df_targ_sample)
      } else {
        inc_sample_targ <- dplyr::filter(df_targ_sample,
                                         target == paste0("inc ", targ))
        cum_sample_targ <- dplyr::filter(df_targ_sample,
                                         target == paste0("cum ", targ))
        # Incidence
        if (!any(grepl("inc", df_targ_quantile$target))) {
          message("The submission file from: ", team_model,
                  " is missing incident quantiles for the target: ", targ,
                  ". The missing value will be calculated.")
          df_targ_inc <- ScenarioModelingHub.Data::make_quantiles(
            inc_sample_targ, quant_group = quant_group,
            quantile_vect = quantile_vect, cumul_group = NULL)
        } else {
          df_targ_inc <- rbind(inc_sample_targ,
                               dplyr::filter(df_targ_quantile,
                                             target == paste0("inc ", targ)))
        }
        # Cumulative
        if (!any(grepl("cum", df_targ_quantile$target))) {
          message("The submission file from: ", team_model,
                  " is missing cumulative quantiles for the target: ", targ,
                  ". The missing value will be calculated.")
          df_targ_cum <- ScenarioModelingHub.Data::make_quantiles(
            cum_sample_targ,
            quant_group = c("origin_date", "scenario_id", "location", "target",
                            "horizon", "age_group"),
            quantile_vect = quantile_vect, cumul_group = NULL)
        } else {
          df_targ_cum <- rbind(cum_sample_targ,
                               dplyr::filter(df_targ_quantile,
                                             target == paste0("cum ", targ)))
        }
        df_all_targ <- rbind(df_targ_cum, df_targ_inc)
      }
    }
    return(df_all_targ)
  })
  df_all <- dplyr::bind_rows(lst_all)
  return(df_all)
}

#' Calculate missing peak time
#'
#' For rounds with samples output_type, the peak time hosp target is optional.
#' This function test for if peak time hosp is present in a specific team_model
#' submission and if not generate them
#'
#' @param df_team model projection of a team-model (as submitted)
#' @param df_all model projection of a team-model (with samples and quantiles)
#' to append the results, NULL for no data to append
#' @param team_model name of the team-model
#' @param peak_time_target target name as in the submission file
#' @param peak_group character vector of column names to 'group_by' the input
#' data frame to calculate the peak time hospitalization target for each group
#'
#' @details
#' Generates missing target from incident hospitalization trajectories by
#' calculating cumulative probability of peak across the complete time series.
#' For each trajectories, the horizon of the maximum incident hospitalization
#' is extracted.
#' For each horizon: calculate the average of how many trajectories have peaked
#' on this week (for example: 45 trajectories on 100 trajectories have peaked on
#' horizon 4) (probability of peak).
#' Calculate the cumulative probability of peak across trajectories:
#' following previous example cumulative on horizon 4 = (45/100 + cumulative
#' probability on horizon 3)
#'
#' @importFrom dplyr filter group_by across all_of mutate ungroup bind_rows
calculate_peak_time <- function(
    df_team, df_all, team_model, peak_time_target = "peak time hosp",
    peak_group = c("origin_date", "scenario_id", "location", "target",
                   "age_group", "output_type_id")) {

  df_time_target <- dplyr::filter(df_team, target == peak_time_target)
  if (nrow(df_time_target) < 1) {
    df_inc_sample <- dplyr::filter(df_team, output_type == "sample",
                                   target == "inc hosp", age_group == "0-130") %>%
      dplyr::mutate(output_type_id = as.numeric(output_type_id),
                    origin_date = as.Date(origin_date))
    if (nrow(df_inc_sample) < 1) {
      warning("Missing inc hosp sample to calculate peak time hosp.")
      df_all <- df_all
    } else {
      message("The submission file from: ", team_model,
              " is missing value for the target: ", peak_time_target,
              ". The missing value will be calculated.")

      df_time <- dplyr::group_by(df_inc_sample, across(all_of(peak_group))) %>%
        mutate(sel = ifelse(max(value) == value, horizon, NA)) %>%
        mutate(sel2 = ifelse(all(value == max(value)), 0, 1)) %>%
        filter(sel2 != 0) %>%
        filter(!is.na(sel)) %>% arrange(horizon) %>%
        mutate(sel3 = ifelse(duplicated(output_type_id), 0, 1)) %>%
        filter(sel3 == 1) %>% select(-contains("sel")) %>%
        ungroup()

      lst_time <- split(df_time, list(df_time$scenario_id, df_time$location,
                                      df_time$age_group, df_time$target),
                        drop = TRUE)
      peak_time <- lapply(lst_time, function(dft) {
        df_epitime <- NULL
        if (nrow(dft) != 300) {
          print("No. Peaks != 300")
          print(head(dft))
          print(nrow(dft))
        }
        for (i in 1:max(df_inc_sample$horizon, na.rm = TRUE)) {
          peak_prob = nrow(dplyr::filter(dft, horizon == i)) / nrow(dft)
          if (!is.null(df_epitime)) {
            peak_cum <- filter(df_epitime, horizon == i - 1) %>% .$value
            peak_cum <- peak_cum + peak_prob
          }   else {
            peak_cum <- peak_prob
          }
          if (peak_cum >= 1) peak_cum <- 1
          date <- MMWRweek::MMWRweek(
            unique(dft$origin_date) + (i * 7) - 1)
          if (nchar(date$MMWRweek) < 2) {
            date <- paste0("EW", date$MMWRyear, "0", date$MMWRweek)
          } else {
            date <- paste0("EW", date$MMWRyear, date$MMWRweek)
          }
          df_epi <- distinct(
            dft[, grep("output_type", peak_group, value = TRUE,
                       invert = TRUE)]) %>%
            mutate(horizon = i,
                   output_type = "cdf",
                   output_type_id = date,
                   value = peak_cum,
                   target = peak_time_target)
          df_epitime <- rbind(df_epi, df_epitime)
        }
        df_epitime$horizon <- NA
        return(df_epitime)
      }) %>% bind_rows()
      df_all <- dplyr::mutate(df_all, origin_date = as.Date(origin_date))
      df_all <- rbind(df_all, peak_time)
    }
  } else {
    message("The submission file from: ", team_model,
            " contains value for the target: ", peak_time_target)
    df_all <- rbind(df_all, df_time_target)
  }
  return(df_all)
}


#' Calculate missing peak size
#'
#' For rounds with samples output_type, the peak size hosp target is optional.
#' This function test for if peak size hosp is present in a specific team_model
#' submission and if not generate them
#'
#' @param df_team model projection of a team-model (as submitted)
#' @param df_all model projection of a team-model (with samples and quantiles)
#' to append the results, NULL for no data to append
#' @param team_model name of the team-model
#' @param peak_size_target target name as in the submission file
#' @param peak_group character vector of column names to 'group_by' the input
#' data frame to calculate the peak size hospitalization target for each group
#' @param quantile_vect numeric vector of probabilities with values in [0,1],
#' to produce quantiles corresponding to the given probabilities
#'
#' @details
#' Generates missing target from incident hospitalization trajectories by
#' extracting the max incident hospitalization value for each trajectories and
#' then generates the quantiles distribution of the max valua across all
#' trajectories.
#'
#' @importFrom dplyr filter group_by across all_of mutate ungroup bind_rows
calculate_peak_size <- function(
    df_team, df_all, team_model, peak_size_target = "peak size hosp",
    peak_group = c("origin_date", "scenario_id", "location", "target",
                   "age_group", "output_type_id"),
    quantile_vect = c(0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4,
                      0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9,
                      0.95, 0.975, 0.99)) {

  df_size_target <- dplyr::filter(df_team, target == peak_size_target)
  if (nrow(df_size_target) < 1) {
    df_inc_sample <- dplyr::filter(df_team, output_type == "sample",
                                   target == "inc hosp", age_group == "0-130")
    if (nrow(df_inc_sample) < 1) {
      warning("Missing inc hosp sample to calculate peak size hosp.")
      df_all <- df_all
    } else {
      message("The submission file from: ", team_model,
              " is missing value for the target: ", peak_size_target,
              ". The missing value will be calculated.")
      df_size <- dplyr::group_by(df_inc_sample, across(all_of(peak_group))) %>%
        dplyr::summarise(max = max(value)) %>%
        dplyr::ungroup() %>%
        reframe(
          value = quantile(max, quantile_vect),
          output_type = "quantile",
          output_type_id = quantile_vect,
          .by = all_of(grep("output_type", peak_group, value = TRUE,
                            invert = TRUE))) %>%
        mutate(horizon = NA,
               target = peak_size_target)
    }
    df_all <- rbind(df_all, df_size)
  } else {
    message("The submission file from: ", team_model,
            " contains value for the target: ", peak_size_target)
    df_all <- rbind(df_all, df_size_target)
  }

  return(df_all)
}
