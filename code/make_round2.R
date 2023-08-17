# Library
library(dplyr)
library(hubUtils) #from GitHub: Infectious-Disease-Modeling-Hubs/hubUtils

# Prerequisite
config <- hubUtils::read_config(getwd(), config = "tasks")
config_round2 <- config$rounds[[2]]

req_df <- lapply(config_round2$model_tasks, function(x) {
    task_id_col <- setNames(
        lapply(names(x$task_ids),
               function(y) return(x$task_ids[[y]]$required)),
        names(x$task_ids))
    output_col <- lapply(
        names(x$output_type),
        function(y) list(output_type = y,
                         output_type_id = x$output_type[[y]]$type_id$required))
    col <- c(task_id_col, unlist(output_col, FALSE),
             list(value = 0,
                  team_model = c("team1_modela", "team2_modelb", "team3_modelc",
                                 "team4_modeld", "team5_modele", "team6_modelf",
                                 "team7_modelg")))
    df <- expand.grid(col, stringsAsFactors = FALSE)
    return(df)
})

# output file for tests
req_df <- dplyr::bind_rows(req_df) %>%
    dplyr::mutate(
        origin_date = as.Date(origin_date),
        value = ifelse(
            target == "inc case",
            sample(seq(1, 6000, by = 0.0001), nrow(.), replace = TRUE),
            sample(seq(1, 1000, by = 0.0001), nrow(.), replace = TRUE)
                  ))
attr(req_df, "out.attrs") <- NULL

lapply(unique(req_df$team_model), function(x) {
    df <- dplyr::filter(req_df, team_model == x) %>% dplyr::select(-team_model)
    if (x == "team1_modela") df$value <- as.integer(df$value)
    or_date <- config_round2$model_tasks[[1]]$task_ids$origin_date$required
    filename <- paste0("data-processed/", x, "/", or_date, "-", x, ".parquet")
    arrow::write_parquet(df, filename)
})
