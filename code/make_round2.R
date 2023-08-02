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
        function(y) list(
            output_type = y,
            output_type_id = c(
                1:x$output_type[[y]]$output_type_id$max_samples_per_task)))
    col <- c(task_id_col, unlist(output_col, FALSE),
             list(value = 0,
                  team_model = c("team1-modela", "team2-modelb", "team3-modelc",
                                 "team4-modeld", "team5-modele", "team6-modelf",
                                 "team7-modelg")))
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

# Update sample output_type_id to possible different outcome:
library(magrittr)

# A model that estimates a single joint distribution for all horizons, both
# locations, scenarios, targets and all race_ethnicity might submit something
# like the following to represent a collection of 2 samples from that
# joint distribution:
# In this case, output_type_id: 1 represents: a single draw from the joint
# predictive distribution accross scenario, target, location, race_ethnicity
# and horizon
# team1 and 2 (output_type_id: 1 to 100 * 1 possible group)

req_df_team12 <- filter(req_df, team_model %in% c("team1-modela", "team2-modelb"))
req_df_team12 <- req_df_team12 %>%
    dplyr::group_by(scenario_id, target, location, race_ethnicity, horizon,
                    team_model) %>%
    dplyr::mutate(output_type_id = c(1:100)) %>%
    ungroup()

# A model that obtains separate fits for each scenario and race_ethnicity, but
# obtains sample trajectories across the horizons, targets and locations
# would have a submission with distinct output_type_ids for the
# different scenario/location combinations
# For example:
#  - sample 1: race1, scenario A
#  - sample 3: race2, scenario A
#  - sample 3: overall, scenario B
#  - sample 4: race1, scenario C
#  etc.
# team 3 and 4 (output_type_id: 1 to 100 * 11 possible group)
req_df_team34 <- filter(req_df, team_model %in% c("team3-modelc", "team4-modeld"))
req_df_team34 <- req_df_team34 %>%
    dplyr::group_by(target, location, horizon, team_model) %>%
    dplyr::mutate(output_type_id = c(1:n())) %>%
    ungroup()


# A model that obtains separate fits for each scenario, race_ethnicity and
# location, but obtains sample trajectories across the horizons, targets
# would have a submission with distinct output_type_ids for the
# different scenario/race_ethnicity/location combinations
# For example:
#  - sample 1: location 06, scenario A, race 1
#  - sample 2: location 06, scenario A, race 2
#  - sample 3: location 06, scenario B, overall
#  - sample 4: location 37, scenario A, race 1
#  etc.
# team 5 (output_type_id: 1 to 100 * 22 possible group)
req_df_team5 <- filter(req_df, team_model == "team5-modele")
req_df_team5 <- req_df_team5 %>%
    dplyr::group_by(target, horizon, team_model) %>%
    dplyr::mutate(output_type_id = c(1:n())) %>%
    ungroup()


# A model that obtains separate fits for each scenario, target, race_ethnicity
# and location, but obtains sample trajectories across the horizons
# would have a submission with distinct output_type_ids for the
# different scenario/race_ethnicity/location/target combinations
# For example:
#  - sample 1: location 06, scenario A, race 1, incident death
#  - sample 2: location 06, scenario A, race 1, incident hosp
#  - sample 3: location 06, scenario A, race 2, incident death
#  - sample 4: location 06, scenario A, race 2, incident hosp
#  etc.
# team 6 (output_type_id: 1 to 100 * 44 possible group)
req_df_team6 <- filter(req_df, team_model == "team6-modelf")
req_df_team6 <- req_df_team6 %>%
    dplyr::group_by(horizon, team_model) %>%
    dplyr::mutate(output_type_id = c(1:n())) %>%
    ungroup()


# A model that obtains separate marginal distributions for each scenario,
# target, location, race_ethnicity and horizon would use a distinct
# output_type_id in every row of its submission to indicate that the
# samples are all different draws from different distributions.
# team 7 (output_type_id: 1 to 100 * 880 possible group)
req_df_team7 <- filter(req_df, team_model == "team7-modelg")
req_df_team7 <- req_df_team7 %>%
    dplyr::group_by(team_model) %>%
    dplyr::mutate(output_type_id = c(1:n())) %>%
    ungroup()

# Join all files
tot_df <- rbind(req_df_team12, req_df_team34, req_df_team5, req_df_team6,
                req_df_team7)

# Write output
lapply(unique(tot_df$team_model), function(x) {
    df <- dplyr::filter(tot_df, team_model == x) %>% dplyr::select(-team_model)
    if (x == "team1_modela") df$value <- as.integer(df$value)
    or_date <- config_round2$model_tasks[[1]]$task_ids$origin_date$required
    filename <- paste0("data-processed/", x, "/", or_date, "-", x, ".parquet")
    arrow::write_parquet(df, filename)
})
