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
                      output_type, output_type_id, age_group,
                      value = value_cum) %>%
        mutate(target = gsub("inc", "cum", target),
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
                                            0.85, 0.9, 0.95, 0.975, 0.99),
                      horizon_max = 39) {
    df_size_quant <- df %>%
        filter(target == "inc hosp", output_type == "sample",
               age_group == "0-130") %>%
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
        filter(target == "inc hosp", output_type == "sample",
               age_group == "0-130") %>%
        group_by(origin_date, scenario_id, location, target, age_group,
                 team_model, output_type_id) %>%
        mutate(sel = ifelse(max(value) == value, horizon, NA)) %>%
        ungroup() %>%
        filter(!is.na(sel))
    lst_time <- split(df_time, list(df_time$scenario_id, df_time$location,
                                    df_time$age_group))
    peak_time <- lapply(lst_time, function(dft) {
        df_epitime <- NULL
        for (i in 1:horizon_max) {
            peak_prob = nrow(dplyr::filter(dft, horizon == i)) / 100
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
