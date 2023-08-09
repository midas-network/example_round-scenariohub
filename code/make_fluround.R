# Library
library(dplyr)
library(hubUtils) #from GitHub: Infectious-Disease-Modeling-Hubs/hubUtils

# Functions
make_quantiles <- function(df,
                           quantile_vect = c(0.01, 0.025, 0.05, 0.1, 0.15, 0.2,
                                             0.25, 0.3, 0.35, 0.4,0.45, 0.5,
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
    calc_cum <- function(df) {
        for (i in 2:length(df$horizon)) {
            df$value_cum[i] <- df$value[i] + df$value_cum[i - 1]
        }
        return(df$value_cum)
    }
    df_cum <- mutate(arrange(df, horizon), value_cum = value) %>%
        data.table::data.table()
    df_cum <- df_cum[, value_cum := calc_cum(.SD), by = cumul_group ] %>%
        dplyr::select(origin_date, scenario_id, location, target, horizon,
                      output_type, output_type_id, value = value_cum) %>%
        mutate(target = gsub("inc", "cum", target),
               age_group = "0-130",
               team_model = unique(df$team_model))
    df <- rbind(df, df_cum)
    # quantiles
    df_quant <- dplyr::reframe(df,
                               value = quantile(value, probs = quantile_vect),
                               output_type = "quantile",
                               output_type_id = quantile_vect,
                               .by = all_of(quant_group))
    df <- rbind(dplyr::filter(df, grepl("inc ", target)), df_quant)
    return(df)
}

make_peak <- function(df, quantile_vect = c(0.01, 0.025, 0.05, 0.1, 0.15, 0.2,
                                            0.25, 0.3, 0.35, 0.4,0.45, 0.5,
                                            0.55, 0.6, 0.65, 0.7, 0.75, 0.8,
                                            0.85, 0.9, 0.95, 0.975, 0.99)) {
    df_size_quant <- df %>%
        filter(target == "inc hosp", output_type == "sample") %>%
        group_by(origin_date, scenario_id, location, target, age_group,
                 team_model, output_type_id) %>%
        summarise(max = max(value)) %>%
        ungroup() %>%
        reframe(
            value = quantile(max, quantile_vect),
            output_type = "quantile",
            output_type_id = quantile_vect,
            .by = all_of(c("origin_date", "scenario_id", "location",
                           "target", "age_group", "team_model"))) %>%
        mutate(horizon = NA,
               target = "peak size hosp")
    df_time <- df %>%
        filter(target == "inc hosp", output_type == "sample") %>%
        group_by(origin_date, scenario_id, location, target, age_group,
                 team_model, output_type_id) %>%
        mutate(sel = ifelse(max(value) == value, horizon, NA)) %>%
        ungroup() %>%
        filter(!is.na(sel))
    lst_time <- split(df_time, list(df_time$scenario_id, df_time$location,
                                    df_time$age_group))
    peak_time <- lapply(lst_time, function(dft) {
        df_epitime <- NULL
        for (i in 1:39) {
            peak_prob = nrow(dplyr::filter(dft, horizon == i)) / 100
            if (!is.null(df_epitime)) {
                peak_cum <- filter(df_epitime, horizon == i - 1) %>% .$value
                peak_cum <- peak_cum + peak_prob
            }   else {
                peak_cum <- peak_prob
            }
            date <- MMWRweek::MMWRweek(
                unique(dft$origin_date) + (i * 7) - 1)
            date <- paste0("EW", date$MMWRyear, date$MMWRweek)
            df_epi <- distinct(
                dft[, c("origin_date", "scenario_id", "location", "target",
                       "age_group", "team_model")]) %>%
                mutate(horizon = i,
                       output_type = "cdf",
                       output_type_id = date,
                       value = peak_cum,
                       target = "peak time hosp")
            df_epitime <- rbind(df_epi, df_epitime)
        }
        df_epitime$horizon <- NA
        return(df_epitime)
    }) %>% bind_rows()
    tot_df <- rbind(df, df_size_quant, peak_time)
    return(tot_df)
}

# Prerequisite
config <- hubUtils::read_config(getwd(), config = "tasks")
config_round1 <- config$rounds[[1]]

# Data Creation
req_df <- lapply(config_round1$model_tasks[1:2], function(x) {
    task_id_col <- setNames(
        lapply(names(x$task_ids),
               function(y) {
                   return(x$task_ids[[y]]$required)
                   }),
        names(x$task_ids))
    output_col <- lapply(
        names(x$output_type),
        function(y) list(
            output_type = y,
            output_type_id = x$output_type[[y]]$output_type_id$required))
    col <- c(task_id_col, unlist(output_col, FALSE),
             list(value = NA,
                  team_model = c("team1_modela", "team2_modelb", "team3_modelc",
                                 "team4_modeld", "team5_modele", "team6_modelf")
                  ))
    if (is.null(col$location)) {
        col$location <- x$task_ids$location$optional
    }
    if (is.null(col$horizon)) {
        col$horizon <- NA
    }
    df <- expand.grid(col, stringsAsFactors = FALSE)
    df <- dplyr::mutate(df,
        output_type_id = as.character(output_type_id),
        origin_date = as.Date(origin_date))

    if ("inc death" %in% col$target & "sample" %in% col$output_type)
        df$value <- sample(seq(1, 1000, by = 0.0001), nrow(df), replace = TRUE)
    if ("inc hosp" %in% col$target & "sample" %in% col$output_type)
        df$value <- sample(seq(1, 6000, by = 0.0001), nrow(df), replace = TRUE)
    df$output_type_id <- as.character(df$output_type_id)
    return(df)
})

# output file for tests
req_df <- dplyr::bind_rows(req_df)
attr(req_df, "out.attrs") <- NULL

# Update values per team:
# Team1: all possible data
# Team 2: no death data
# Team 3: no peak target
# Team 4: only samples
# Team 5: only 4 locations
# Team 6: only samples hosp
all_data <- lapply(unique(req_df$team_model), function(model_id) {
    df <- dplyr::filter(req_df, team_model == model_id)
    if (model_id %in% c("team2_modelb", "team6_modelf"))
        df <- filter(df, grepl("hosp", target))
    if (model_id == "team5_model5")
        df <- filter(df, location %in% c("US", "10", "24", "36"))
    if (model_id %in% c("team1_modela", "team2_modelb", "team3_modelc",
                        "team5_model5")) {
        # Calculate quantiles and cumulative
        df <- make_quantiles(df)
    }
    if (model_id %in% c("team1_modela", "team2_modelb", "team5_model5")) {
        # # Calculate peak targets
        df <- make_peak(df)
    }
    return(df)
}) %>% setNames(unique(req_df$team_model))

lapply(all_data, function(df) {
    team_name <- unique(df$team_model)
    folder_name <-  paste0("data-processed/", team_name)
    df <- df %>% dplyr::select(-team_model)
    if (team_name != "team1_modela") df$value <- round(df$value, 6)
    if (!dir.exists(folder_name)) dir.create(folder_name)
    filename <- paste0(folder_name, "/", unique(df$origin_date), "-",
                       team_name, ".parquet")
    arrow::write_parquet(df, filename)
})
