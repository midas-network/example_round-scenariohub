# Functions
make_quantiles <- function(df,
                           quantile_vect = c(0.01, 0.025, 0.05, 0.1, 0.15, 0.2,
                                             0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                             0.55, 0.6, 0.65, 0.7, 0.75, 0.8,
                                             0.85, 0.9, 0.95, 0.975, 0.99),
                           quant_group = c("origin_date", "scenario_id",
                                           "location", "target", "horizon",
                                           "age_group", "team_model"),
                           cumul_group = c("origin_date",  "scenario_id",
                                           "location", "target", "output_type",
                                           "output_type_id",
                                           "age_group", "team_model"),
                           keep_cumul = FALSE) {
  # cumulative
  if (!is.null(cumul_group)) {
    print("Calculate cumulative ...")
    calc_cum <- function(df) {
      for (i in 2:length(df$horizon)) {
        df$value_cum[i] <- df$value[i] + df$value_cum[i - 1]
      }
      return(df$value_cum)
    }
    df_cum <- dplyr::arrange(df, dplyr::pick("horizon"))
    df_cum$value_cum <- df_cum$value
    df_cum <- data.table::data.table(df_cum)
    df_cum <- df_cum[, value_cum := calc_cum(.SD), by = cumul_group] %>%
      dplyr::select(-value) %>%
      dplyr::rename(value = value_cum)
    if (any("team_model" %in% colnames(df))) {
      df_cum <- dplyr::mutate(df_cum, target = gsub("inc", "cum", target),
                              team_model = unique(df$team_model))
    } else {
      df_cum <- dplyr::mutate(df_cum, target = gsub("inc", "cum", target))
    }
    df <- rbind(df, df_cum)
  }
  # quantiles
  print("Calculate quantiles ...")
  df_quant <- dplyr::reframe(df,
                             value = quantile(value, probs = quantile_vect),
                             output_type = "quantile",
                             output_type_id = quantile_vect,
                             .by = all_of(quant_group))
  if (all(c("stochastic_run", "run_grouping") %in% colnames(df))) {
    df_quant$run_grouping <- NA
    df_quant$stochastic_run <- NA
  }
  if (keep_cumul) {
    df <- rbind(df, df_quant)
  } else {
    df <- rbind(dplyr::filter(df, grepl("inc ", target)), df_quant)
  }
  return(df)
}

make_peak <- function(df,
                      quantile_vect = c(0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25,
                                        0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6,
                                        0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95,
                                        0.975, 0.99),
                      horizon_max = 39,
                      col_name_id = c("origin_date", "scenario_id", "location",
                                      "target", "age_group", "team_model"),
                      peak_target = "inc hosp", age_grp = "0-130") {
  df_size_quant <- df %>%
    dplyr::filter(target == peak_target, output_type == "sample",
                  age_group == age_grp) %>%
    dplyr::group_by(dplyr::pick(c(col_name_id, "output_type_id"))) %>%
    dplyr::summarise(max = max(value)) %>%
    dplyr::ungroup() %>%
    dplyr::reframe(value = quantile(max, quantile_vect),
                   output_type = "quantile", output_type_id = quantile_vect,
                   .by = dplyr::all_of(col_name_id)) %>%
    dplyr::mutate(horizon = NA,
                  target = gsub("inc ", "peak size ", peak_target))
  df_time <- df %>%
    dplyr::filter(target == peak_target, output_type == "sample",
                  age_group == age_grp) %>%
    dplyr::group_by(dplyr::pick(c(col_name_id, "output_type_id"))) %>%
    dplyr::mutate(sel = ifelse(max(value) == value, horizon, NA)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(!is.na(sel))
  lst_time <- split(df_time, list(df_time$scenario_id, df_time$location,
                                  df_time$age_group))
  peak_time <- lapply(lst_time, function(dft) {
    df_epitime <- NULL
    for (i in 1:horizon_max) {
      peak_prob <- nrow(dplyr::filter(dft, horizon == i)) / 100
      if (!is.null(df_epitime)) {
        peak_cum <- dplyr::filter(df_epitime, horizon == i - 1) %>% .$value
        peak_cum <- peak_cum + peak_prob
      }   else {
        peak_cum <- peak_prob
      }
      if (peak_cum >= 1) peak_cum <- 1
      date <- MMWRweek::MMWRweek(unique(dft$origin_date) + (i * 7) - 1)
      if (nchar(date$MMWRweek) < 2) {
        date <- paste0("EW", date$MMWRyear, "0", date$MMWRweek)
      } else {
        date <- paste0("EW", date$MMWRyear, date$MMWRweek)
      }
      df_epi <- dplyr::distinct(dft[, col_name_id]) %>%
        dplyr::mutate(horizon = i, output_type = "cdf", output_type_id = date,
                      value = peak_cum,
                      target = gsub("inc ", "peak time ", peak_target))
      df_epitime <- rbind(df_epi, df_epitime)
    }
    df_epitime$horizon <- NA
    return(df_epitime)
  }) %>%
    dplyr::bind_rows()
  tot_df <- rbind(df, df_size_quant, peak_time)
  return(tot_df)
}

# Prepare sample information for a specific column (col_update parameter)
prep_sample_information <- function(df, col_update, pairing, rep = 100,
                                    same_rep = FALSE) {

  df0 <- dplyr::filter(df, output_type != "sample")
  df_id <- dplyr::filter(df, output_type == "sample")
  if (any(grepl("inc", df_id$target)) && any(grepl("cum", df_id$target))) {
    pairing <- unique(c(pairing, "target"))
  }
  col_sel <- !colnames(df_id) %in% c(pairing, "output_type", "output_type_id",
                                     "value", "run_grouping",
                                     "stochastic_run")
  unpaired_col <- colnames(df_id)[col_sel]
  list_df <- split(df_id, df_id[, unpaired_col], drop = TRUE)
  id_df <- lapply(seq_along(list_df), function(x) {
    test <- list_df[[x]]
    # Extract the unique pairing values
    index <- dplyr::distinct(test[, pairing, FALSE])
    # Create an Index: repeat `rep` number of times the unique
    # pairing values and assign an index to each time
    all_index <- data.frame()
    n <- rep * x - rep
    if (isFALSE(same_rep)) {
      for (n in seq(n + 1, rep * x, 1)) {
        index[[col_update]] <- n
        all_index <- rbind(all_index, index)
      }
    } else {
      all_index <- index[rep(seq_len(nrow(index)), rep), , FALSE]
      all_index[[col_update]] <- n
    }
    # Arrange both the index and original data by pairing variables
    # and add the index information on the original data
    all_index <- dplyr::arrange(all_index, dplyr::pick(pairing))
    test <- dplyr::arrange(test, dplyr::pick(pairing))
    test[[col_update]] <- all_index[[col_update]]
    return(test)
  })
  id_df <- bind_rows(id_df)
  df_tot <- rbind(df0, id_df)
  return(df_tot)
}


make_df_sample <-
  function(round_spec, max_sample = 100,
           team_model = c("team1-modela", "team2-modelb", "team3-modelc",
                          "team4-modeld", "team5-modele")) {
    req_df <- lapply(round_spec, function(x) {
      if (!is.null(x$task_ids$target$required)) {
        task_id_col <-
          setNames(lapply(names(x$task_ids),
                          function(y) {
                            if (y == "location") {
                              loc_info <- x$task_ids[[y]]
                              loc <- c(as.character(loc_info$optional),
                                       as.character(loc_info$required))
                            } else {
                              loc <- x$task_ids[[y]]$required
                            }
                            return(loc)
                          }), names(x$task_ids))
        output_col <- lapply(names(x$output_type),
                             function(y) {
                               list(output_type = y,
                                    output_type_id = 1:max_sample)
                             })
        col <- c(task_id_col, unlist(output_col, FALSE),
                 list(value = NA, team_model = team_model))
        df <- expand.grid(col, stringsAsFactors = FALSE)
      } else {
        df <- NULL
      }
      return(df)
    })
    req_df <- dplyr::bind_rows(req_df)
    attr(req_df, "out.attrs") <- NULL
    return(req_df)
  }


update_df_val_sample <- function(df, max_value = 100000, quantile = FALSE,
                                 peak = FALSE,
                                 quantile_vect = c(0.01, 0.025, 0.05, 0.1, 0.15,
                                                   0.2, 0.25, 0.3, 0.35, 0.4,
                                                   0.45, 0.5, 0.55, 0.6, 0.65,
                                                   0.7, 0.75, 0.8, 0.85, 0.9,
                                                   0.95, 0.975, 0.99),
                                 quant_group = c("origin_date", "scenario_id",
                                                 "location", "target",
                                                 "horizon", "age_group",
                                                 "team_model"),
                                 cumul_group = c("origin_date", "scenario_id",
                                                 "location", "target",
                                                 "output_type",
                                                 "output_type_id",
                                                 "age_group", "team_model"),
                                 peak_target = "inc hosp", age_grp = "0-130",
                                 keep_cumul = FALSE) {
  df <- dplyr::mutate(df,
                      output_type_id = as.character(output_type_id),
                      origin_date = as.Date(origin_date))
  df$value <-  sample(seq(0, 100000, by = 0.0001), nrow(df), replace = TRUE)
  if (quantile) df <- make_quantiles(df, quantile_vect = quantile_vect,
                                     quant_group = quant_group,
                                     cumul_group = cumul_group,
                                     keep_cumul = keep_cumul)
  if (peak) df <- make_peak(df, horizon_max = max(df$horizon, na.rm = TRUE),
                            quantile_vect = quantile_vect, age_grp = age_grp,
                            col_name_id = quant_group,
                            peak_target = peak_target)
  return(df)
}

# Requisite:
#  - target_data: should have a column `date`, `location` and `value`
update_val_obs <- function(df, target_data, start_date = "2020-01-01",
                           end_date = "2023-09-01", target = "inc hosp",
                           add_col = NULL, limit_date = NULL) {
  # - Filter to date of interest
  if (!is.null(start_date) && !is.null(end_date))
    target_data <- dplyr::filter(target_data, date < end_date,
                                 date > start_date)
  if (!is.null(limit_date)) {
    sample_date <- dplyr::filter(target_data, date < limit_date)
    sample_date <- unique(sample_date$date)
  } else {
    sample_date <- unique(sample_date$date)
  }
  proj_st_date <- as.Date(sample(grep("\\d{4}-10|\\d{4}-09", sample_date,
                                      value = TRUE), 1))
  proj_end_date <- proj_st_date + max(df$horizon, na.rm = TRUE) * 7
  # - Filter source date to selected time window and specific target
  obs_data <- dplyr::filter(target_data, date >= proj_st_date,
                            date <= proj_end_date, target %in% !!target)
  # - Create horizon column (with start date = horizon 1) and select columns
  # to merge with example files: location, horizon, age group and value
  obs_data <-
    dplyr::mutate(obs_data,
                  horizon = as.numeric(as.Date(date) - proj_st_date) / 7)
  # Select column of interest
  sel_col <- c("location", "horizon", "value", "target", add_col)
  obs_data <- dplyr::select(obs_data, dplyr::all_of(sel_col))
  # - Replace value
  df <- dplyr::mutate(df,
                      output_type_id = as.character(output_type_id),
                      origin_date = as.Date(origin_date))
  df_val <- dplyr::left_join(dplyr::select(df, -value), obs_data,
                             by = grep("value", sel_col, invert = TRUE,
                                       value = TRUE),
                             relationship = "many-to-one")
  # - As all the scenario have the same value, multiply by small factor for
  # each scenario
  df_val <- lapply(unique(df_val$scenario_id), function(scen) {
    multi <- sample(seq(1, 1.1, by = 0.01), 1)
    df_val %>%
      dplyr::filter(scenario_id == scen) %>%
      dplyr::mutate(value = ((value + horizon + as.numeric(output_type_id)) *
                               multi))
  }) %>%
    dplyr::bind_rows()
  # return output
  return(df_val)
}


# Update samples id information for 5 teams models (required to have sample id
# information into two additional columns: stochastic_run and run_grouping)
team_sample_id <- function(all_data, default_pairing, max_sample) {
  # Recreate different example of the new format
  # Team 1: same parameter set, but each run is different because of
  #   stochasticity, paired on horizon and age group
  all_data$`team1-modela` <-
    dplyr::mutate(all_data$`team1-modela`,
                  run_grouping = ifelse(output_type == "sample", 1, NA),
                  stochastic_run = ifelse(output_type == "sample", 0, NA),
                  output_type_id = ifelse(output_type == "sample", NA,
                                          output_type_id)) %>%
    prep_sample_information("stochastic_run", default_pairing, rep = max_sample)

  # Team 2: different parameter sets for each run, runs are not stochastic,
  #   paired on horizon and age group
  all_data$`team2-modelb` <-
    dplyr::mutate(all_data$`team2-modelb`,
                  run_grouping = ifelse(output_type == "sample", 0, NA),
                  stochastic_run = ifelse(output_type == "sample", 1, NA),
                  output_type_id = ifelse(output_type == "sample", NA,
                                          output_type_id)) %>%
    prep_sample_information("run_grouping", default_pairing, rep = max_sample)

  # Team 3: different parameter sets for every stochastic run, paired on horizon
  #  and age group
  all_data$`team3-modelc` <-
    dplyr::mutate(all_data$`team3-modelc`,
                  run_grouping = ifelse(output_type == "sample", 0, NA),
                  stochastic_run = ifelse(output_type == "sample", 0, NA),
                  output_type_id = ifelse(output_type == "sample", NA,
                                          output_type_id)) %>%
    prep_sample_information("run_grouping", default_pairing,
                            rep = max_sample) %>%
    prep_sample_information("stochastic_run", default_pairing, rep = max_sample)

  # Team 4: different parameter sets, replicated in multiple stochastic runs,
  #  paired on horizon and age group
  all_data$`team4-modeld` <-
    dplyr::mutate(all_data$`team4-modeld`,
                  run_grouping = ifelse(output_type == "sample", 0, NA),
                  stochastic_run = ifelse(output_type == "sample", 0, NA),
                  output_type_id = ifelse(output_type == "sample", NA,
                                          output_type_id)) %>%
    prep_sample_information("run_grouping", default_pairing, rep = max_sample,
                            same_rep = TRUE) %>%
    prep_sample_information("stochastic_run", default_pairing, rep = max_sample)

  # Team 5:  paired on horizon and age group and scenario (scenarios are
  # assumed to have the same basic parameters, aside from scenario-specific
  # parameters, but come from different stochastic runs)
  all_data$`team5-modele` <-
    dplyr::mutate(all_data$`team5-modele`,
                  run_grouping = ifelse(output_type == "sample", 0, NA),
                  stochastic_run = ifelse(output_type == "sample", 0, NA),
                  output_type_id = ifelse(output_type == "sample", NA,
                                          output_type_id)) %>%
    prep_sample_information("run_grouping", c(default_pairing, "scenario_id"),
                            rep = max_sample, same_rep = TRUE) %>%
    prep_sample_information("stochastic_run", default_pairing, rep = max_sample)

  return(all_data)
}

# Write output file in parquet file
write_output_parquet <- function(all_data, date_file = NULL, max_size = 1e6,
                                 folder_dir = "data-processed/") {
  lapply(all_data, function(df) {
    team_name <- unique(df$team_model)
    folder_name <-  paste0("data-processed/", team_name)
    df <-  dplyr::select(df, -team_model)
    if (!dir.exists(folder_name)) dir.create(folder_name)
    if (is.null(date_file)) {
      filename <- paste0(folder_name, "/", unique(df$origin_date), "-",
                         team_name, ".parquet")
    } else {
      filename <- paste0(folder_name, "/", date_file, "-", team_name,
                         ".parquet")
    }
    arrow::write_parquet(df, filename)
    if (file.size(filename) / max_size > 60) {
      file.remove(filename)
      filename <- paste0(folder_name, "/", unique(df$origin_date), "-",
                         team_name, ".gz.parquet")
      arrow::write_parquet(df, filename, compression = "gzip",
                           compression_level = 9)
    }
  })
}

max_sample <- function(round_spec) {
  r_output <- unlist(purrr::map(round_spec$model_tasks, "output_type"))
  max_sample <- unique(r_output[grep("max_sample", names(r_output))])
  max_sample <- as.numeric(max_sample)
  return(max_sample)
}

def_grp <- function(round_spec) {
  r_output <- unlist(purrr::map(round_spec$model_tasks, "output_type"))
  default_pairing <- unique(r_output[grep("joint", names(r_output))])
  return(default_pairing)
}

# Add age group in RSV net data
# Filter age group to aggregate
# Group by location and horizon
# Aggregate the values
# Assign new age group
# Append to data frame of origin
new_age_group <- function(df, agg_age_groups, new_age_group, fct = "sum",
                          col_group = c("location", "date", "target")) {
  if (fct == "minus") {
    min_age <- dplyr::filter(df, age_group %in% agg_age_groups[-1]) %>%
      dplyr::group_by(dplyr::pick(col_group)) %>%
      dplyr::summarise(value_min = sum(value)) %>%
      dplyr::mutate(age_group = agg_age_groups[1])
    new_ag <- dplyr::filter(df, age_group %in% agg_age_groups[1]) %>%
      dplyr::left_join(min_age) %>%
      dplyr::reframe(value = value - value_min, .by = c(col_group, "age_group"))
  } else {
    new_ag <- dplyr::filter(df, age_group %in% agg_age_groups) %>%
      dplyr::group_by(dplyr::pick(col_group)) %>%
      dplyr::summarise(value = sum(value)) %>%
      dplyr::mutate(age_group = new_age_group) %>%
      dplyr::ungroup()
  }
  new_ag <- new_ag %>%
    dplyr::mutate(value = ifelse(is.na(value), 0, value)) %>%
    dplyr::mutate(value = ifelse(value < 0, 0, value)) %>%
    dplyr::mutate(age_group = new_age_group) %>%
    dplyr::ungroup()
  return(dplyr::bind_rows(df, new_ag))
}
