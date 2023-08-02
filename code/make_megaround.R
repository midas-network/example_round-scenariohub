#### DUMMY SCRIPT
rm(list = ls())

# Library
library(dplyr)

# Prerequisite
loc_list <- c("US", "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
              "13", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24",
              "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
              "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47",
              "48", "49", "50", "51", "53", "54", "55", "56", "60", "66", "69",
              "72", "74", "78")
team_model <- c("team1-modela", "team2-modelb", "team3-modelc","team4-modeld",
                "team5-modele", "team6-modelf", "team7-modelg")

df_team <- lapply(team_model, function(tm) {
  print(tm)
  if (tm == team_model[3]) {
    max_horizon = 250
  } else {
    max_horizon = 104
  }
  sam_var <- sample(seq(0.01, 0.25, by = 0.01), 1)
  df_loc <- lapply(loc_list, function(loc) {
    print(loc)
    df_targ <- lapply(c("inc death", "inc hosp"), function(targ) {
      ## Sample file format ----
      if (targ == "inc death") {
        df_obs <- read.csv("data-goldstandard/deaths_incidence_num.csv")
        x <- sample(seq(10:60), 1)
      }
      if (targ == "inc hosp") {
        df_obs <- read.csv("data-goldstandard/hospitalization.csv")
        if (tm %in% c("team6_modelf", "team7_modelg")) {
          df_obs <- arrange(df_obs, order(as.Date(time_value), decreasing = T))
        }
        x <- sample(seq(28:60), 1)
      }
      # data frame with one group
      df <- data.frame(
        origin_date = "2023-04-16",
        scenario_id = "A-2023-04-16",
        location = loc,
        target = targ,
        horizon = c(1:max_horizon),
        output_type = "sample",
        output_type_id = 1,
        value = filter(df_obs, fips == loc)[x:(x + (max_horizon - 1)), "value"]
      )

      # for all sample
      df_tot <- df
      for (i in 2:100) {
        x2 <- sample(seq(x, x + 13), 1)
        val = filter(df_obs, fips == loc)[x2:(x2 + (max_horizon - 1)), "value"] *
          sample(seq(1 - sam_var, 1 + sam_var, by = 0.01), 1)
        df2 <- data.frame(
          origin_date = "2023-04-16",
          scenario_id = "A-2023-04-16",
          location = loc,
          target = targ,
          horizon = c(1:max_horizon),
          output_type = "sample",
          output_type_id = i,
          value = val
        )
        df_tot <- rbind(df_tot, df2)
      }
      # for all scenario
      dfscen <- df_tot
      scen_list <- c("A-2023-04-16", "B-2023-04-16", "C-2023-04-16",
                     "D-2023-04-16", "E-2023-04-16", "F-2023-04-16")
      for (i in 2:length(scen_list)) {
        df2 <- data.frame(
          origin_date = "2023-04-16",
          scenario_id = scen_list[[i]],
          location = loc,
          target = targ,
          horizon = dfscen$horizon,
          output_type = "sample",
          output_type_id = dfscen$output_type_id,
          value = dfscen$value * as.numeric(paste0("1.", i))
        )
        df_tot <- rbind(df_tot, df2)
      }
      return(df_tot)
    }) %>% bind_rows()
    return(df_targ)
  }) %>% bind_rows()

  df_loc$value <- as.numeric(df_loc$value)
  df_loc[which(df_loc$value < 0), "value"] <- 0
  df_loc[which(is.na(df_loc$value)), "value"] <- 0
  df_loc <- dplyr::mutate(df_loc,
    origin_date = as.Date(origin_date))

  if (tm == team_model[4]) {
    quant_group = c("origin_date", "scenario_id",
                    "location", "target", "horizon")
    quantile_vect = c(0.01, 0.025, 0.05, 0.1, 0.15, 0.2,
                      0.25, 0.3, 0.35, 0.4,0.45, 0.5,
                      0.55, 0.6, 0.65, 0.7, 0.75, 0.8,
                      0.85, 0.9, 0.95, 0.975, 0.99)
    df_quant <- dplyr::reframe(df_loc,
                               value = quantile(value, probs = quantile_vect),
                               output_type = "quantile",
                               output_type_id = quantile_vect,
                               .by = all_of(quant_group))
    df_loc <- rbind(df_loc, df_quant)
  }

  filename <- paste0("data-processed/", tm, "/2023-04-16-", tm, ".gz.parquet")
  print(filename)
  arrow::write_parquet(df_loc, filename,
                       compression = "gzip", compression_level = 9)
  df_loc$model_name <- tm
  return(df_loc)

}) %>% bind_rows()

# Plotting test
library(ggplot2)
test <- dplyr::filter(df_team, location == "45", grepl("A-", scenario_id),
                      target == "inc hosp") %>%
  mutate(group = paste0(model_name, output_type_id))
ggplot2::ggplot(data = test, aes(x = horizon, y = value, group = group,
                        color = model_name)) +
  geom_line()

# Special split file for team3
df <- arrow::read_parquet("data-processed/team3-modelc/2023-04-16-team3-modelc.gz.parquet")
lapply(unique(df$target), function(x) {
  df_target <- dplyr::filter(df, target == x)
  name_target <- gsub(" ", "_", x)
  filename <- paste0("data-processed/team3-modelc/2023-04-16-team3-modelc-",
                     name_target, ".gz.parquet")
  arrow::write_parquet(df_target, filename,
                       compression = "gzip", compression_level = 9)
})
file.remove("data-processed/team3-modelc/2023-04-16-team3-modelc.gz.parquet")

# Clean environment
rm(list = ls())
