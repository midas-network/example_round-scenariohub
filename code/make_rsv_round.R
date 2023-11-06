
# Library
library(dplyr)
library(hubUtils) #from GitHub: Infectious-Disease-Modeling-Hubs/hubUtils


# Function

# Add age group in RSV net data
# Filter age group to aggregate
# Group by location and horizon
# Aggregate the values
# Assign new age group
# Append to data frame of origin
new_age_group <- function(df, agg_age_groups, new_age_group, fct = "sum") {
    if (fct == "minus") {
        min_age <- df %>% filter(age_group %in% agg_age_groups[-1]) %>%
            group_by(location, horizon) %>% summarise(value_min = sum(value)) %>%
            mutate(age_group = agg_age_groups[1])
        new_ag <- df %>% filter(age_group %in% agg_age_groups[1]) %>%
            left_join(min_age) %>%
            reframe(value = value - value_min, .by = location, horizon,
                    age_group)
    } else {
        new_ag <- df %>% filter(age_group %in% agg_age_groups) %>%
            group_by(location, horizon) %>% summarise(value = sum(value)) %>%
            mutate(age_group = new_age_group) %>%
            ungroup()
    }
    new_ag <- new_ag %>%
        mutate(value = ifelse(is.na(value), 0, value)) %>%
        mutate(value = ifelse(value < 0, 0, value)) %>%
        mutate(age_group = new_age_group) %>%
        ungroup()
    return(bind_rows(df, new_ag))
}


# Add value from RSV net data, by selecting 29 weeks of the beginning of a
# random season
make_value_col <- function(df, rsv_net) {
    # - Select a random date before 2023-2024 season, from the start of the
    # season(only September, October included)
    rsv_net <- filter(rsv_net, date < "2023-09-01", date >"2020-01-01")
    start_date <- as.Date(sample(grep("\\d{4}-10|\\d{4}-09",
                                      unique(rsv_net$date), value = TRUE), 1))
    end_date <- start_date + 29 * 7
    # - Filter source date to selected time window and specific target
    rsv_data <- filter(rsv_net, date >= start_date, date <= end_date,
                       target == "inc hosp")
    # - Create horizon column (with start date = horizon 1) and select columns
    # to merge with example files: location, horizon, age group and value
    rsv_data <- mutate(rsv_data,
                       horizon = as.numeric(as.Date(date) - start_date) / 7)
    rsv_data <- select(rsv_data, location, horizon, age_group, value)

    # - Add missing age group by aggregating detailed age group
    rsv_data <- new_age_group(rsv_data, c("0-0.49", "0.5-0.99"), "0-0.99")
    rsv_data <- new_age_group(rsv_data, c("1-1.99", "2-4"), "1-4")
    rsv_data <- new_age_group(rsv_data, c("0-130", "65-130", "0-4"), "5-64",
                              "minus")
    # - Replace value
    df_val <- dplyr::left_join(select(df, -value), rsv_data,
                               by = c("horizon", "location", "age_group"))
    # - As all the scenario have the same value, multiply by small factor for
    # each scenario
    df_val <- lapply(unique(df_val$scenario_id), function(scen) {
        multi <- sample(seq(1, 1.3, by = 0.0001), 1)
        df_val %>% filter(scenario_id == scen) %>%
            mutate(value = value * multi)
    }) %>% bind_rows()
    # return output
    return(df_val)
}



# Prerequisite
source("code/utils.R")
# Round 4 in the examble tasks.json is round 1 of RSV
config <- hubUtils::read_config(getwd(), config = "tasks")
rsv1 <- config$rounds[[4]]
rsv_net <- read.csv("https://raw.githubusercontent.com/midas-network/rsv-scenario-modeling-hub/main/target-data/rsvnet_hospitalization.csv")

# Workflow
req_df <- lapply(rsv1$model_tasks, function(x) {
    if (!is.null(x$task_ids$target$required)) {
        task_id_col <- setNames(
            lapply(names(x$task_ids),
                   function(y) {
                       if (y == "location") {
                           return(as.character(x$task_ids[[y]]$optional))
                       } else {
                           return(x$task_ids[[y]]$required)
                       }
                   }),
            names(x$task_ids))
        output_col <- lapply(
            names(x$output_type),
            function(y) list(output_type = y,
                             output_type_id = x$output_type[[y]]$output_type_id$required))
        col <- c(task_id_col, unlist(output_col, FALSE),
                 list(value = NA,
                      team_model = c("team1-modela", "team2-modelb", "team3-modelc",
                                     "team4-modeld", "team5-modele")))
        df <- expand.grid(col, stringsAsFactors = FALSE)

    } else {
        df <- NULL
    }
    return(df)
})

req_df <- dplyr::bind_rows(req_df)
attr(req_df, "out.attrs") <- NULL


# Update values per team:
# - update value column from RSV-NET data
# - calculate peak and quantiles column
all_data <- lapply(unique(req_df$team_model), function(model_id) {
    print(model_id)
    df <- dplyr::filter(req_df, team_model == model_id)
    df <- dplyr::mutate(df,
                        output_type_id = as.character(output_type_id),
                        origin_date = as.Date(origin_date))
    df <- make_value_col(df, rsv_net) %>%
        mutate(value = ifelse(is.na(value), 0, value)) %>%
        mutate(value = ifelse(value < 0, 0, value))
    df <- make_quantiles(df)
    df <- make_peak(df)
    return(df)
}) %>% setNames(unique(req_df$team_model))

# Update samples id information

# Write output file in parquet format
lapply(all_data, function(df) {
    team_name <- unique(df$team_model)
    folder_name <-  paste0("data-processed/", team_name)
    df <- df %>% dplyr::select(-team_model)
    if (!dir.exists(folder_name)) dir.create(folder_name)
    filename <- paste0(folder_name, "/", unique(df$origin_date), "-",
                       team_name, ".parquet")
    arrow::write_parquet(df, filename)
})

# Clean environment
rm(list = ls())
