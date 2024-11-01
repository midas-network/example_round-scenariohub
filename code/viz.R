# Basic visualization for checking
# Library and system
library(arrow)
library(dplyr)
library(ggplot2)
library(hubData)

# Load data
tasks_json <- jsonlite::fromJSON("hub-config/tasks.json",
                                 simplifyDataFrame = FALSE)
schema <- hubData::create_hub_schema(tasks_json)
schema <- c(schema$fields,
            arrow::Field$create("run_grouping", arrow::int64()),
            arrow::Field$create("stochastic_run", arrow::int64()))
schema <- arrow::schema(schema)
# partition <- "model_id"
partition <- c("model_id", "origin_date", "target")

data <-
  arrow::open_dataset("data-processed/", partitioning = partition,
                      schema = schema,
                      factory_options = list(exclude_invalid_files = TRUE)) %>%
  dplyr::filter(origin_date == "2024-07-28")#"2024-04-28")#, grepl("2020-05-01", scenario_id))

# Sample -----
test <- dplyr::filter(data, output_type == "sample") %>%
  dplyr::collect()
test$output_type_id <- as.numeric(as.factor(paste0(test$run_grouping, "-",
                                                   test$stochastic_run)))
test <- dplyr::select(test, -run_grouping, -stochastic_run)
test <- janitor::remove_empty(test, which = c("rows", "cols"))
gc()
# Identify each individual group
tasksid_col_names <- grep("output_type|value|horizon|model_id", names(test),
                          invert = TRUE, value = TRUE)
test <- dplyr::group_by(test, dplyr::pick(tasksid_col_names)) %>%
  dplyr::mutate(group_tag = dplyr::cur_group_id())
test_tag <- unique(test$group_tag)
if (length(test_tag) > 40) {
  tag_sel <- sample(test_tag, 40)
  test <- dplyr::filter(dplyr::ungroup(test), group_tag %in% tag_sel)
}
# Graph
ggplot2::ggplot(test, ggplot2::aes(horizon, value, group = output_type_id,
                                   color = model_id)) +
  ggplot2::geom_line() +
  ggplot2::facet_wrap(vars(group_tag, model_id), labeller = "label_both")


# Quantile -----
test <- dplyr::filter(data, output_type == "quantile",
                      output_type_id == 0.5) %>%
  dplyr::collect()
test <- dplyr::select(test, -run_grouping, -stochastic_run)
test <- janitor::remove_empty(test, which = c("rows", "cols"))
# Identify each individual group
tasksid_col_names <- grep("output_type|value|horizon|model_id", names(test),
                          invert = TRUE, value = TRUE)
test <- dplyr::group_by(test, dplyr::pick(tasksid_col_names)) %>%
  dplyr::mutate(group_tag = dplyr::cur_group_id())
test_tag <- unique(test$group_tag)
if (length(test_tag) > 40) {
  tag_sel <- sample(test_tag, 40)
  test <- dplyr::filter(dplyr::ungroup(test), group_tag %in% tag_sel)
}
ggplot2::ggplot(test, ggplot2::aes(horizon, value, group = model_id,
                                   color = model_id)) +
  ggplot2::geom_line() +
  ggplot2::facet_wrap(~group_tag, labeller = "label_both")

