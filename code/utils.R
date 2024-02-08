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
                                           "age_group", "team_model")) {
  # cumulative
  if (!is.null(cumul_group)) {
    calc_cum <- function(df) {
      for (i in 2:length(df$horizon)) {
        df$value_cum[i] <- df$value[i] + df$value_cum[i - 1]
      }
      return(df$value_cum)
    }
    df_cum <- dplyr::arrange(df, dplyr::across("horizon"))
    df_cum$value_cum <- df_cum$value
    df_cum <- data.table::data.table(df_cum)
    df_cum <- df_cum[, value_cum := calc_cum(.SD), by = cumul_group] %>%
      dplyr::select(origin_date, scenario_id, location, target, horizon,
                    output_type, output_type_id, age_group,
                    value = value_cum) %>%
      dplyr::mutate(target = gsub("inc", "cum", target),
                    team_model = unique(df$team_model))
    df <- rbind(df, df_cum)
  }
  # quantiles
  df_quant <- dplyr::reframe(df,
                             value = quantile(value, probs = quantile_vect),
                             output_type = "quantile",
                             output_type_id = quantile_vect,
                             .by = all_of(quant_group))
  df <- rbind(dplyr::filter(df, grepl("inc ", target)), df_quant)
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
    dplyr::group_by(c(dplyr::all_of(col_name_id), "output_type_id")) %>%
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
    dplyr::group_by(origin_date, scenario_id, location, target, age_group,
                    team_model, output_type_id) %>%
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
  col_sel <- !colnames(df_id) %in% c(pairing, "output_type", "output_type_id",
                                     "value", "run_grouping",
                                     "stochastic_run")
  unpaired_col <- colnames(df_id)[col_sel]
  list_df <- split(df_id, df_id[, unpaired_col], drop = TRUE)
  id_df <- lapply(seq_along(list_df), function(x) {
    test <- list_df[[x]]
    # Extract the unique pairing values
    index <-  distinct(test[, pairing])
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
      all_index <- index[rep(seq_len(nrow(index)), rep), ]
      all_index[[col_update]] <- n
    }
    # Arrange both the index and original data by pairing variables
    # and add the index information on the original data
    all_index <- arrange(all_index, across(pairing))
    test <- arrange(test, across(pairing))
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
                                 peak_target = "inc hosp", age_grp = "0-130") {
  df <- dplyr::mutate(df,
                      output_type_id = as.character(output_type_id),
                      origin_date = as.Date(origin_date))
  df$value <-  sample(seq(0, 100000, by = 0.0001), nrow(df), replace = TRUE)
  if (quantile) df <- make_quantiles(df, quantile_vect = quantile_vect,
                                     quant_group = quant_group,
                                     cumul_group = cumul_group)
  if (peak) df <- make_peak(df, horizon_max = max(df$horizon, na.rm = TRUE),
                            quantile_vect = quantile_vect, age_grp = age_grp,
                            col_name_id = quant_group,
                            peak_target = peak_target)
  return(df)
}
