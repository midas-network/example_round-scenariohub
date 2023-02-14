
################################################################################
######                                                                     #####
#                         DEPRECATED                                           #
######                                                                     #####
################################################################################

##' Prepare Target information for Validation
##'
##' Prepare list of information for one target for Validation
##'
##' @param target_name name of the target
##' @param req_quantiles "all" if all required quantiles are required for this
##' target, a subset of value, or NA if no quantiles required. By default, "all"
##' @param opt_quantiles "all" if all optional uantiles are accepted, a subset
##' of value, or NA if no quantiles expected. By default, "NA"
##' @param req_type required type, by default "quantile"
##' @param opt_type optional type, by default "point"
##' @param req_loc "all" if all locations are accepted for this
##' target, a subset of value, or NA if no location required. By default, "all"
##' @param opt_loc "all" if all additional optional locations are accepted for
##' this target, a subset of value, or NA if no optional location is accepted.
##' By default, NA
##' @param req_horizon "all" if all the complete required time series is required
##' for this  target, a subset of value, or NA if no time series required.
##' By default, "all"
##' @param opt_horizon "all" if all the all additional weeks are accepted for
##' this target, a subset of value, or NA if no optional additional weeks
##' expected. By default, NA
##' @param req_scenario "all" if all scenarios are required for this
##' target, a subset of value, or NA if no scenario required. By default, "all"
##' @param opt_scenario "all" if all optional scenarios are accepted for this
##' target, a subset of value, or NA if no additional optional scenario
##' expected. By default,  NA
##' @param value description of the expected value, for example: c(">= 0",
##' "<= 1"). By defaut, ">= 0"
##' @param req_agegroup "all" if all possible age group are required for this
##' target, a subset of value, or NA if no age group required. By default, NA
##' @param opt_agegroup "all" if all possible age group are accepted for this
##' target, a subset of value, or NA if no age group required. By default, NA
##'
##' @importFrom stats setNames
##' @export
##'
##' @examples
##' target_list("inc_death", opt_quantiles= "all", req_loc = "US",
##'     req_agegroup = "0-130")
#target_list <- function(target_name, req_quantiles = "all",
#                        opt_quantiles = NA, req_type = "quantile",
#                        opt_type = "point", req_loc = "all", opt_loc = NA,
#                        req_horizon = "all", opt_horizon = NA,
#                        req_scenario = "all", opt_scenario = NA,
#                        value = ">= 0", req_agegroup = NA, opt_agegroup = NA) {
#
#  target_info <- list(
#    quantiles = list(required = req_quantiles, optional = opt_quantiles),
#    type = list(required = req_type, optional = opt_type),
#    location = list(required = req_loc, optional = opt_loc),
#    horizons = list(required = req_horizon, optional = opt_horizon),
#    scenarios = list(required = req_scenario, optional =opt_scenario),
#    value = value,
#    agegroup = list(required = req_agegroup, optional =  opt_agegroup))
#
# target_info <- setNames(list(target_info), target_name)
# return(target_info)
#
#}
#
#
##' Prepare JSON file for Validation
##'
##' Each JSON should have an associated JSON file containing the information
##' for the validation. THis functions helps generating it
##'
##' @param req_quantiles vector of required quantiles
##' @param column vector of required column names
##' @param path_loc path to CSV file contains location information
##' @param req_horizons numeric, horizons
##' @param df_scenario data frame containing scenario, start date information
##' per round (output of scenario_information() function)
##' @param round_number numeric, number of the round
##' @param req_target_list list of information on the required target (output
##' of target_list() function)
##' @param write_path path to the folder to write the output in JSON format
##' @param opt_quantiles vector of optional quantiles. By default, NULL
##' @param age_min vector of minimal value for each required age group. By
##' default, NULL
##' @param age_max vector of minax value for each required age group. By
##' default, NULL
##' @param col_loc name of the column in the "path_loc" CSV file containing the
##'  location information to use in the validation. By default, "location'
##' @param opt_horizons_start numeric, start of optional horizons. By default,
##' NULL
##' @param opt_horizons_end numeric, end of optional horizons. By default, NULL
##' @param opt_target_list list of information on the optional target (output
##' of target_list() function). By default, NULL
##' @param append_filename string to add at the end of the filename. By default,
##' NULL
##'
##' @export
##' @importFrom dplyr distinct
##' @importFrom jsonlite toJSON
##'
##'@examples
##'\dontrun{
##'  req_quantiles <- c(0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4,
##'      0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.975,
##'      0.99)
##'  column <- c("model_projection_date", "scenario_name", "scenario_id",
##'      "target", "target_end_date", "location", "type", "quantile", "value",
##'      "age_group")
##'  path_loc <- "data-locations/locations.csv"
##'  all_path <- c(
##'       round1 = "flu-scenario-modeling-hub/master/README.md"
##'  )
##'  df_scenario <- scenario_information(all_path, write_md = FALSE,
##'      date_approx = FALSE,sel_date = "start date")
##'  req_target_list <- c(
##'    target_list("inc death", opt_quantiles= "all", req_loc = "US",
##'                req_agegroup = "0-130"),
##'    target_list("cum death", opt_quantiles= "all", req_loc = "US",
##'                req_agegroup = "0-130"),
##'    target_list("inc hosp", opt_quantiles= "all", req_agegroup = "0-130"),
##'    target_list("cum hosp", opt_quantiles= "all", req_agegroup = "0-130"),
##'    target_list("peak time hosp", opt_quantiles= NA, req_quantiles = NA,
##'                opt_type = NA, req_type = "point", req_agegroup = "0-130",
##'                value = c(">= 0", "<= 1")),
##'    target_list("peak size hosp", opt_quantiles= "all", req_horizon = NA,
##'                req_agegroup = "0-130"))
##'  write_path <- ""
##'
##'  json_metadata(req_quantiles, column, path_loc, 42, df_scenario, 1,
##'                req_target_list, write_path, opt_quantiles = c(0, 1),
##'                age_min = c(0, 5, 18, 50, 65),
##'                age_max = c(4, 17, 49, 64, 130))
##'}
##'
##'
#json_metadata <- function(req_quantiles, column, path_loc, req_horizons,
#                          df_scenario, round_number, req_target_list,
#                          write_path, opt_quantiles = NULL, age_min = NULL,
#                          age_max = NULL, col_loc = "location",
#                          opt_horizons_start = NULL, opt_horizons_end = NULL,
#                          opt_target_list = NULL, append_filename = NULL) {
#  # Location
#  loc_info <- read.csv(path_loc)
#  locations <- loc_info[["location"]]
#  # Horizons
#  req_horizons <- seq(1, req_horizons)
#  if (!is.null(opt_horizons_start) & !is.null(opt_horizons_end)) {
#    opt_horizons <- seq(opt_horizons_start, opt_horizons_end)
#  } else {
#    opt_horizons <- NULL
#  }
#  # Scenario/round information
#  scen_info <- df_scenario[df_scenario$round == paste0("round", round_number), ]
#  # Start date
#  first_week_ahead <- as.Date(unique(scen_info$date_round)) + 6
#  if (length(first_week_ahead) > 1)
#    stop("Start date is not unique in this selection.")
#  # Scenario
#  scen_df <- scen_info[, c("scenario_id", "scenario_name")]
#  scen_df <- dplyr::distinct(scen_df)
#  scen_list <- NULL
#  for (i in seq_along(scen_df$scenario_id)) {
#    scen_list_unique <- list(list(name = scen_df$scenario_name[i],
#                             id = scen_df$scenario_id[i]))
#    scen_list <- append(scen_list_unique, scen_list)
#  }
#  scen_list <- setNames(scen_list, seq_len(length(scen_list)))
#  # Complete list
#  json_list <- list(
#    column_names = column,
#    quantiles = list(required = req_quantiles, optional = opt_quantiles),
#    ages = list(age_min = age_min, age_max = age_max),
#    locations = locations,
#    horizons = list(required = req_horizons, optional = opt_horizons),
#    scenarios = scen_list,
#    first_week_ahead = first_week_ahead,
#    targets = list(required = req_target_list, optional = opt_target_list)
#  )
#  json_list <- jsonlite::toJSON(json_list)
#  write(json_list, paste0(write_path, unique(scen_info$date_round),
#                          "_metadata", append_filename, ".json"))
#
#}
#
