# Clean environment
rm(list = ls())

# Library
library(dplyr)
library(hubUtils) #from GitHub: Infectious-Disease-Modeling-Hubs/hubUtils

# Prerequisite
source("code/utils.R")
config <- hubUtils::read_config(getwd(), config = "tasks")
config_round1 <- config$rounds[[3]]

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
                  team_model = c("team1-modela", "team2-modelb", "team3-modelc",
                                 "team4-modeld", "team5-modele", "team6-modelf",
                                 "team7-modelg")
                  ))
    if (is.null(col$location)) {
        col$location <- x$task_ids$location$optional[1:52]
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
# Team1: all possible data (no age group)
# Team 2: no death data
# Team 3: no peak target
# Team 4: only samples
# Team 5: only 4 locations
# Team 6: only samples hosp
# team 7: all possible data (with optional age group)
all_data <- lapply(unique(req_df$team_model), function(model_id) {
    df <- dplyr::filter(req_df, team_model == model_id)
    if (model_id %in% c("team2-modelb", "team6-modelf"))
        df <- filter(df, grepl("hosp", target))
    if (model_id %in% c("team7-modelg")) {
        df_child <- mutate(df, age_group = "0-17",
                           value = value / 4)
        df_adult <- mutate(df, age_group = "18-130", value = value * 3/4)
        df <- rbind(df, df_child, df_adult)
    }
    if (model_id == "team5-model5")
        df <- filter(df, location %in% c("US", "10", "24", "36"))
    if (model_id %in% c("team1-modela", "team2-modelb", "team3-modelc",
                        "team5-modele", "team7-modelg")) {
        # Calculate quantiles and cumulative
        df <- make_quantiles(df)
    }
    if (model_id %in% c("team1-modela", "team2-modelb", "team5-modele",
                        "team7-modelg")) {
        # # Calculate peak targets
        df <- make_peak(df)
    }
    return(df)
}) %>% setNames(unique(req_df$team_model))

lapply(all_data, function(df) {
    team_name <- unique(df$team_model)
    folder_name <-  paste0("data-processed/", team_name)
    df <- df %>% dplyr::select(-team_model)
    if (team_name != "team1-modela") df$value <- round(df$value, 6)
    if (!dir.exists(folder_name)) dir.create(folder_name)
    filename <- paste0(folder_name, "/", unique(df$origin_date), "-",
                       team_name, ".parquet")
    arrow::write_parquet(df, filename)
})

# Clean environment
rm(list = ls())
