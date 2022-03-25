
library(tidycensus)
library(data.table)
library(dplyr)
library(sf)
library(RPostgreSQL)
library(tidyr)

# function that takes loadings from
# fit factor analysis and applies to new data
newFactors <- function(model_data, new_data, fitted_data) {
  coef <- solve(fitted_data$correlation) %*% fitted_data$loadings
  means <- apply(model_data, 2, mean)
  sds <- apply(model_data, 2, sd)
  scale(new_data, means, sds) %*% coef
}


# define ACS vars to get
acs_vars <- c(
  # % 65+
  "B01001_020", "B01001_021", "B01001_022", "B01001_023", "B01001_024", "B01001_025",
  "B01001_044", "B01001_045", 'B01001_046', "B01001_047", "B01001_048", "B01001_049",
  "B01001_001",
  # % 25+ with less than HS diploma
  "B15001_012", "B15001_013", "B15001_020", "B15001_021", "B15001_028",
  "B15001_029", "B15001_036", "B15001_037",
  "B15001_053", "B15001_054", "B15001_061", "B15001_062", "B15001_069",
  "B15001_070", "B15001_077", "B15001_078",
  "B15001_001",
  # poverty status
  "B17001_001", "B17001_002",
  # % noninstitutionalized pop with disability
  "B18101_004", "B18101_007", "B18101_010", "B18101_013", "B18101_016", "B18101_019",
  "B18101_023", "B18101_026", "B18101_029", "B18101_032", "B18101_035", "B18101_038",
  "B18101_001",
  # # of HHs
  "B28004_001",
  # HHs with <35K and no Internet
  "B28004_005", "B28004_009", "B28004_013",
  # HHs with 75K+ and access to dial or BB Internet
  "B28004_023", "B28004_024",
  # total population
  "B01001_001",
  # % without a computing device
  # (desktop, laptop, smartphone, tablet, etc)
  "B28001_011", "B28001_001",
  # % no internet access (no internet sub,
  # cellular data plans, dial up)
  "B28002_013", "B28002_001",
  # % with any broadband
  "B28002_004"
)


### tract level DEI ###

### get ACS data ###
acs_tract_2019 <- get_acs(geography = "tract",
                          state = 51,
                          variables = acs_vars,
                          year = 2019,
                          survey = "acs5",
                          cache_table = TRUE,
                          output = "wide",
                          geometry = FALSE,
                          keep_geo_vars = FALSE)

# for now, replace 0 counts in IIR denom cats with 1
acs_tract_2019$B28004_005E[acs_tract_2019$B28004_005E == 0] <- 1
acs_tract_2019$B28004_009E[acs_tract_2019$B28004_009E == 0] <- 1
acs_tract_2019$B28004_013E[acs_tract_2019$B28004_013E == 0] <- 1

acs_tract_2019 <- acs_tract_2019 %>% transmute(
  tract_fips = GEOID,
  tract_name = NAME,
  perc_65_under = 100 - ((B01001_020E + B01001_021E + B01001_022E + B01001_023E +
                            B01001_024E + B01001_025E + B01001_044E + B01001_045E +
                            B01001_046E + B01001_047E + B01001_048E + B01001_049E) / B01001_001E * 100),
  perc_over_HS = 100 - ((B15001_012E + B15001_013E + B15001_020E + B15001_021E + B15001_028E +
                           B15001_029E + B15001_036E + B15001_037E +
                           B15001_053E + B15001_054E + B15001_061E + B15001_062E + B15001_069E +
                           B15001_070E + B15001_077E + B15001_078E) / B15001_001E * 100),
  not_pov = 100 - ((B17001_002E / B17001_001E) * 100),
  perc_no_disability = 100 - ((B18101_004E + B18101_007E + B18101_010E + B18101_013E +
                                 B18101_016E + B18101_019E +
                                 B18101_035E + B18101_038E) / B18101_001E * 100),
  IIR = ((B28004_023E + B28004_024E) / B28004_001E) / ((B28004_005E + B28004_009E + B28004_013E) / B28004_001E),
  perc_have_computer = 100 - ((B28001_011E / B28001_001E) * 100),
  perc_have_internet_access = 100 - ((B28002_013E / B28002_001E) * 100),
  perc_have_broadband = (B28002_004E / B28002_001E) * 100
) %>%
  as.data.frame()


### get CHAS data ###
chas <- read.csv("/project/biocomplexity/sdad/projects_data/mc/data_commons/ACS_5_YR_CHAS_Estimate_Data_by_Tract.csv")

va_cnty_fips <- get(data("fips_codes")) %>% filter(state_code == 51)
va_cnty_fips$cnty_fips <- paste0(va_cnty_fips$state_code, va_cnty_fips$county_code)

va_chas <- chas[chas$CNTY_FIPS %in% va_cnty_fips$cnty_fips,]

va_chas_tract <- va_chas %>%
  select(tract_fips = GEOID,
         total_hh = T2_EST1,
         num_low_income_house_burden_30 = T8_LE50_CB) %>%
  mutate(perc_low_income_house_burden_30 = 100 - ((num_low_income_house_burden_30 / total_hh) *100)) %>%
  as.data.frame()

va_chas_tract$tract_fips <- as.character(va_chas_tract$tract_fips)


### get Ookla data ###
conn <- dbConnect(drv = PostgreSQL(), dbname = "sdad",
                  host = "10.250.124.195",
                  port = 5432,
                  user = Sys.getenv(x = "db_userid"),
                  password = Sys.getenv(x = "db_pwd"))

ookla_data_tr <- st_read(conn, query = "SELECT * FROM dc_digital_communications.va_tr_ookla_2019_2021_speed_measurements")

dbDisconnect(conn)

ookla_data_tr <- ookla_data_tr %>%
  select(tract_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(tract_fips, names_from = c(measure, year), values_from = value)


ookla_tr_full <- ookla_data_tr %>%
  mutate(total_down_x_devices = avg_down_using_devices_2019 * devices_2019 +
           avg_down_using_devices_2020 * devices_2020 +
           avg_down_using_devices_2021 * devices_2021,
         total_up_x_devices = avg_up_using_devices_2019 * devices_2019 +
           avg_up_using_devices_2020 * devices_2020 +
           avg_up_using_devices_2021 * devices_2021,
         total_lat_x_devices = avg_lat_using_devices_2019 * devices_2019 +
           avg_lat_using_devices_2020 * devices_2020 +
           avg_lat_using_devices_2021 * devices_2021,
         total_devices = devices_2019 + devices_2020 + devices_2021,
         download = total_down_x_devices / total_devices,
         upload = total_up_x_devices / total_devices,
         latency = total_lat_x_devices / total_devices) %>%
  select(tract_fips,
         download,
         upload,
         latency) %>%
  as.data.frame()

ookla_tr_2019 <- ookla_data_tr %>%
  select(tract_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  as.data.frame()

ookla_tr_2020 <- ookla_data_tr %>%
  select(tract_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  as.data.frame()

ookla_tr_2021 <- ookla_data_tr %>%
  select(tract_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  as.data.frame()


## combine data

dei_tract <- acs_tract_2019 %>%
  left_join(va_chas_tract, by = "tract_fips") %>%
  left_join(ookla_tr_full, by = "tract_fips") %>%
  select(tract_fips,
         tract_name,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

## factor analysis

dei_vars <- dei_tract %>%
  select(-c(tract_fips, tract_name))

dei_vars <- dei_vars[complete.cases(dei_vars),]


# factor analysis
fa_varimax_tract <- factanal(scale(dei_vars),
                             factors = 4,
                             rotation = "varimax",
                             scores = "regression")

## get predictions for 2019, 2020, 2021 ##

# 2019
dei_tract_pred_2019 <- acs_tract_2019 %>%
  left_join(va_chas_tract, by = "tract_fips") %>%
  left_join(ookla_tr_2019, by = "tract_fips") %>%
  select(tract_fips,
         tract_name,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_tract_pred_2019 <- dei_tract_pred_2019[complete.cases(dei_tract_pred_2019),]

tract_index_mat_2019 <- newFactors(scale(dei_vars),
                                   scale(dei_tract_pred_2019 %>% select(-c(tract_fips, tract_name))),
                                   fa_varimax_tract)
colnames(tract_index_mat_2019) <- c("fct1", "fct2", "fct3", "fct4")
dei_tract_mat_2019 <- as.data.frame(cbind(dei_tract_pred_2019, tract_index_mat_2019))

dei_tract_mat_2019 <- dei_tract_mat_2019 %>%
  mutate(year = 2019,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(tract_fips, tract_name, year, fct1, fct2, fct3, fct4, norm_dei)

# 2020
dei_tract_pred_2020 <- acs_tract_2019 %>%
  left_join(va_chas_tract, by = "tract_fips") %>%
  left_join(ookla_tr_2020, by = "tract_fips") %>%
  select(tract_fips,
         tract_name,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_tract_pred_2020 <- dei_tract_pred_2020[complete.cases(dei_tract_pred_2020),]

tract_index_mat_2020 <- newFactors(scale(dei_vars),
                                   scale(dei_tract_pred_2020 %>% select(-c(tract_fips, tract_name))),
                                   fa_varimax_tract)
colnames(tract_index_mat_2020) <- c("fct1", "fct2", "fct3", "fct4")
dei_tract_mat_2020 <- as.data.frame(cbind(dei_tract_pred_2020, tract_index_mat_2020))

dei_tract_mat_2020 <- dei_tract_mat_2020 %>%
  mutate(year = 2020,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(tract_fips, tract_name, year, fct1, fct2, fct3, fct4, norm_dei)

# 2021
dei_tract_pred_2021 <- acs_tract_2019 %>%
  left_join(va_chas_tract, by = "tract_fips") %>%
  left_join(ookla_tr_2021, by = "tract_fips") %>%
  select(tract_fips,
         tract_name,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_tract_pred_2021 <- dei_tract_pred_2021[complete.cases(dei_tract_pred_2021),]

tract_index_mat_2021 <- newFactors(scale(dei_vars),
                                   scale(dei_tract_pred_2021 %>% select(-c(tract_fips, tract_name))),
                                   fa_varimax_tract)
colnames(tract_index_mat_2021) <- c("fct1", "fct2", "fct3", "fct4")
dei_tract_mat_2021 <- as.data.frame(cbind(dei_tract_pred_2021, tract_index_mat_2021))

dei_tract_mat_2021 <- dei_tract_mat_2021 %>%
  mutate(year = 2021,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(tract_fips, tract_name, year, fct1, fct2, fct3, fct4, norm_dei)

# combine all years
dei_tract_index <- rbind(dei_tract_mat_2019,
                         dei_tract_mat_2020,
                         dei_tract_mat_2021)

dei_tract_index <- dei_tract_index %>%
  group_by(tract_fips) %>%
  arrange(year, .by_group = TRUE) %>%
  rename(geoid = tract_fips,
         region_name = tract_name) %>%
  pivot_longer(!c(geoid, region_name, year), names_to = "measure", values_to = "value") %>%
  mutate(region_type = "tract",
         measure_type = "index") %>%
  select(geoid, region_type, region_name, year, measure, value, measure_type) %>%
  as.data.frame()



### block group level DEI ###

### get ACS data ###
acs_bg_2019 <- get_acs(geography = "block group",
                       state = 51,
                       variables = acs_vars,
                       year = 2019,
                       survey = "acs5",
                       cache_table = TRUE,
                       output = "wide",
                       geometry = FALSE,
                       keep_geo_vars = FALSE)

# for now, replace 0 counts in IIR denom cats with 1
acs_bg_2019$B28004_005E[acs_bg_2019$B28004_005E == 0] <- 1
acs_bg_2019$B28004_009E[acs_bg_2019$B28004_009E == 0] <- 1
acs_bg_2019$B28004_013E[acs_bg_2019$B28004_013E == 0] <- 1

acs_bg_2019 <- acs_bg_2019 %>% transmute(
  bg_fips = GEOID,
  bg_name = NAME,
  perc_65_under = 100 - ((B01001_020E + B01001_021E + B01001_022E + B01001_023E +
                            B01001_024E + B01001_025E + B01001_044E + B01001_045E +
                            B01001_046E + B01001_047E + B01001_048E + B01001_049E) / B01001_001E * 100),
  perc_over_HS = 100 - ((B15001_012E + B15001_013E + B15001_020E + B15001_021E + B15001_028E +
                           B15001_029E + B15001_036E + B15001_037E +
                           B15001_053E + B15001_054E + B15001_061E + B15001_062E + B15001_069E +
                           B15001_070E + B15001_077E + B15001_078E) / B15001_001E * 100),
  not_pov = 100 - ((B17001_002E / B17001_001E) * 100),
  perc_no_disability = 100 - ((B18101_004E + B18101_007E + B18101_010E + B18101_013E +
                                 B18101_016E + B18101_019E +
                                 B18101_035E + B18101_038E) / B18101_001E * 100),
  IIR = ((B28004_023E + B28004_024E) / B28004_001E) / ((B28004_005E + B28004_009E + B28004_013E) / B28004_001E),
  perc_have_computer = 100 - ((B28001_011E / B28001_001E) * 100),
  perc_have_internet_access = 100 - ((B28002_013E / B28002_001E) * 100),
  perc_have_broadband = (B28002_004E / B28002_001E) * 100
) %>%
  as.data.frame()

acs_bg_2019$tract_fips <- substr(acs_bg_2019$bg_fips, 1, 11)

tract_bg_merge <- merge(acs_bg_2019, acs_tract_2019, by = "tract_fips")

acs_bg_2019$perc_65_under[is.na(acs_bg_2019$perc_65_under)] <- tract_bg_merge$perc_65_under.y[is.na(tract_bg_merge$perc_65_under.x)]
acs_bg_2019$perc_over_HS[is.na(acs_bg_2019$perc_over_HS)] <- tract_bg_merge$perc_over_HS.y[is.na(tract_bg_merge$perc_over_HS.x)]
acs_bg_2019$not_pov[is.na(acs_bg_2019$not_pov)] <- tract_bg_merge$not_pov.y[is.na(tract_bg_merge$not_pov.x)]
acs_bg_2019$perc_no_disability[is.na(acs_bg_2019$perc_no_disability)] <- tract_bg_merge$perc_no_disability.y[is.na(tract_bg_merge$perc_no_disability.x)]
acs_bg_2019$IIR[is.na(acs_bg_2019$IIR)] <- tract_bg_merge$IIR.y[is.na(tract_bg_merge$IIR.x)]
acs_bg_2019$perc_have_computer[is.na(acs_bg_2019$perc_have_computer)] <- tract_bg_merge$perc_have_computer.y[is.na(tract_bg_merge$perc_have_computer.x)]
acs_bg_2019$perc_have_internet_access[is.na(acs_bg_2019$perc_have_internet_access)] <- tract_bg_merge$perc_have_internet_access.y[is.na(tract_bg_merge$perc_have_internet_access.x)]
acs_bg_2019$perc_have_broadband[is.na(acs_bg_2019$perc_have_broadband)] <- tract_bg_merge$perc_have_broadband.y[is.na(tract_bg_merge$perc_have_broadband.x)]


### get Ookla data ###
conn <- dbConnect(drv = PostgreSQL(), dbname = "sdad",
                  host = "10.250.124.195",
                  port = 5432,
                  user = Sys.getenv(x = "db_userid"),
                  password = Sys.getenv(x = "db_pwd"))

ookla_data_bg <- st_read(conn, query = "SELECT * FROM dc_digital_communications.va_bg_ookla_2019_2021_speed_measurements")

dbDisconnect(conn)

ookla_data_bg <- ookla_data_bg %>%
  select(bg_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(bg_fips, names_from = c(measure, year), values_from = value)

ookla_bg_2019 <- ookla_data_bg %>%
  select(bg_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  as.data.frame()

ookla_bg_2020 <- ookla_data_bg %>%
  select(bg_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  as.data.frame()

ookla_bg_2021 <- ookla_data_bg %>%
  select(bg_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  as.data.frame()


## get predictions for 2019, 2020, 2021 ##

# 2019
dei_bg_pred_2019 <- acs_bg_2019 %>%
  left_join(va_chas_tract, by = "tract_fips") %>%
  left_join(ookla_bg_2019, by = "bg_fips") %>%
  select(bg_fips,
         bg_name,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_bg_pred_2019 <- dei_bg_pred_2019[complete.cases(dei_bg_pred_2019),]

bg_index_mat_2019 <- newFactors(scale(dei_vars),
                                scale(dei_bg_pred_2019 %>% select(-c(bg_fips, bg_name))),
                                fa_varimax_tract)
colnames(bg_index_mat_2019) <- c("fct1", "fct2", "fct3", "fct4")
dei_bg_mat_2019 <- as.data.frame(cbind(dei_bg_pred_2019, bg_index_mat_2019))

dei_bg_mat_2019 <- dei_bg_mat_2019 %>%
  mutate(year = 2019,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(bg_fips, bg_name, year, fct1, fct2, fct3, fct4, norm_dei)

# 2020
dei_bg_pred_2020 <- acs_bg_2019 %>%
  left_join(va_chas_tract, by = "tract_fips") %>%
  left_join(ookla_bg_2020, by = "bg_fips") %>%
  select(bg_fips,
         bg_name,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_bg_pred_2020 <- dei_bg_pred_2020[complete.cases(dei_bg_pred_2020),]

bg_index_mat_2020 <- newFactors(scale(dei_vars),
                                scale(dei_bg_pred_2020 %>% select(-c(bg_fips, bg_name))),
                                fa_varimax_tract)
colnames(bg_index_mat_2020) <- c("fct1", "fct2", "fct3", "fct4")
dei_bg_mat_2020 <- as.data.frame(cbind(dei_bg_pred_2020, bg_index_mat_2020))

dei_bg_mat_2020 <- dei_bg_mat_2020 %>%
  mutate(year = 2020,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(bg_fips, bg_name, year, fct1, fct2, fct3, fct4, norm_dei)


# 2021
dei_bg_pred_2021 <- acs_bg_2019 %>%
  left_join(va_chas_tract, by = "tract_fips") %>%
  left_join(ookla_bg_2021, by = "bg_fips") %>%
  select(bg_fips,
         bg_name,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_bg_pred_2021 <- dei_bg_pred_2021[complete.cases(dei_bg_pred_2021),]

bg_index_mat_2021 <- newFactors(scale(dei_vars),
                                scale(dei_bg_pred_2021 %>% select(-c(bg_fips, bg_name))),
                                fa_varimax_tract)
colnames(bg_index_mat_2021) <- c("fct1", "fct2", "fct3", "fct4")
dei_bg_mat_2021 <- as.data.frame(cbind(dei_bg_pred_2021, bg_index_mat_2021))

dei_bg_mat_2021 <- dei_bg_mat_2021 %>%
  mutate(year = 2021,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(bg_fips, bg_name, year, fct1, fct2, fct3, fct4, norm_dei)

# combine all years
dei_bg_index <- rbind(dei_bg_mat_2019,
                      dei_bg_mat_2020,
                      dei_bg_mat_2021)

dei_bg_index <- dei_bg_index %>%
  group_by(bg_fips) %>%
  arrange(year, .by_group = TRUE) %>%
  rename(geoid = bg_fips,
         region_name = bg_name) %>%
  pivot_longer(!c(geoid, region_name, year), names_to = "measure", values_to = "value") %>%
  mutate(region_type = "block group",
         measure_type = "index") %>%
  select(geoid, region_type, region_name, year, measure, value, measure_type) %>%
  as.data.frame()


### county level DEI ###

### get ACS data ###
acs_cnty_2019 <- get_acs(geography = "county",
                         state = 51,
                         variables = acs_vars,
                         year = 2019,
                         survey = "acs5",
                         cache_table = TRUE,
                         output = "wide",
                         geometry = FALSE,
                         keep_geo_vars = FALSE)

acs_cnty_2019 <- acs_cnty_2019 %>% transmute(
  cnty_fips = GEOID,
  cnty_name = NAME,
  perc_65_under = 100 - ((B01001_020E + B01001_021E + B01001_022E + B01001_023E +
                            B01001_024E + B01001_025E + B01001_044E + B01001_045E +
                            B01001_046E + B01001_047E + B01001_048E + B01001_049E) / B01001_001E * 100),
  perc_over_HS = 100 - ((B15001_012E + B15001_013E + B15001_020E + B15001_021E + B15001_028E +
                           B15001_029E + B15001_036E + B15001_037E +
                           B15001_053E + B15001_054E + B15001_061E + B15001_062E + B15001_069E +
                           B15001_070E + B15001_077E + B15001_078E) / B15001_001E * 100),
  not_pov = 100 - ((B17001_002E / B17001_001E) * 100),
  perc_no_disability = 100 - ((B18101_004E + B18101_007E + B18101_010E + B18101_013E +
                                 B18101_016E + B18101_019E +
                                 B18101_035E + B18101_038E) / B18101_001E * 100),
  IIR = ((B28004_023E + B28004_024E) / B28004_001E) / ((B28004_005E + B28004_009E + B28004_013E) / B28004_001E),
  perc_have_computer = 100 - ((B28001_011E / B28001_001E) * 100),
  perc_have_internet_access = 100 - ((B28002_013E / B28002_001E) * 100)
)


### get CHAS data ###
va_chas_cnty <- va_chas %>%
  select(cnty_fips = CNTY_FIPS,
         total_hh = T2_EST1,
         num_low_income_house_burden_30 = T8_LE50_CB) %>%
  group_by(cnty_fips) %>%
  summarise(total_hh_cnty = sum(total_hh),
            num_low_income_house_burden_30_cnty = sum(num_low_income_house_burden_30)) %>%
  mutate(perc_low_income_house_burden_30 = 100 - ((num_low_income_house_burden_30_cnty / total_hh_cnty) *100)) %>%
  as.data.frame()

va_chas_cnty$cnty_fips <- as.character(va_chas_cnty$cnty_fips)


### get Ookla data ###
conn <- dbConnect(drv = PostgreSQL(), dbname = "sdad",
                  host = "10.250.124.195",
                  port = 5432,
                  user = Sys.getenv(x = "db_userid"),
                  password = Sys.getenv(x = "db_pwd"))

ookla_data_co <- st_read(conn, query = "SELECT * FROM dc_digital_communications.va_ct_ookla_2019_2021_speed_measurements")

dbDisconnect(conn)

ookla_data_co <- ookla_data_co %>%
  select(cnty_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(cnty_fips, names_from = c(measure, year), values_from = value)

ookla_co_2019 <- ookla_data_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  as.data.frame()

ookla_co_2020 <- ookla_data_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  as.data.frame()

ookla_co_2021 <- ookla_data_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  as.data.frame()


## get predictions for 2019, 2020, 2021 ##

# 2019
dei_cnty_pred_2019 <- acs_cnty_2019 %>%
  left_join(va_chas_cnty, by = "cnty_fips") %>%
  left_join(ookla_co_2019, by = "cnty_fips") %>%
  select(cnty_fips,
         cnty_name,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_cnty_pred_2019 <- dei_cnty_pred_2019[complete.cases(dei_cnty_pred_2019),]

cnty_index_mat_2019 <- newFactors(scale(dei_vars),
                                  scale(dei_cnty_pred_2019 %>% select(-c(cnty_fips, cnty_name))),
                                  fa_varimax_tract)
colnames(cnty_index_mat_2019) <- c("fct1", "fct2", "fct3", "fct4")
dei_cnty_mat_2019 <- as.data.frame(cbind(dei_cnty_pred_2019, cnty_index_mat_2019))

dei_cnty_mat_2019 <- dei_cnty_mat_2019 %>%
  mutate(year = 2019,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(cnty_fips, cnty_name, year, fct1, fct2, fct3, fct4, norm_dei)

# 2020
dei_cnty_pred_2020 <- acs_cnty_2019 %>%
  left_join(va_chas_cnty, by = "cnty_fips") %>%
  left_join(ookla_co_2020, by = "cnty_fips") %>%
  select(cnty_fips,
         cnty_name,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_cnty_pred_2020 <- dei_cnty_pred_2020[complete.cases(dei_cnty_pred_2020),]

cnty_index_mat_2020 <- newFactors(scale(dei_vars),
                                  scale(dei_cnty_pred_2020 %>% select(-c(cnty_fips, cnty_name))),
                                  fa_varimax_tract)
colnames(cnty_index_mat_2020) <- c("fct1", "fct2", "fct3", "fct4")
dei_cnty_mat_2020 <- as.data.frame(cbind(dei_cnty_pred_2020, cnty_index_mat_2020))

dei_cnty_mat_2020 <- dei_cnty_mat_2020 %>%
  mutate(year = 2020,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(cnty_fips, cnty_name, year, fct1, fct2, fct3, fct4, norm_dei)

# 2021
dei_cnty_pred_2021 <- acs_cnty_2019 %>%
  left_join(va_chas_cnty, by = "cnty_fips") %>%
  left_join(ookla_co_2021, by = "cnty_fips") %>%
  select(cnty_fips,
         cnty_name,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_cnty_pred_2021 <- dei_cnty_pred_2021[complete.cases(dei_cnty_pred_2021),]

cnty_index_mat_2021 <- newFactors(scale(dei_vars),
                                  scale(dei_cnty_pred_2021 %>% select(-c(cnty_fips, cnty_name))),
                                  fa_varimax_tract)
colnames(cnty_index_mat_2021) <- c("fct1", "fct2", "fct3", "fct4")
dei_cnty_mat_2021 <- as.data.frame(cbind(dei_cnty_pred_2021, cnty_index_mat_2021))

dei_cnty_mat_2021 <- dei_cnty_mat_2021 %>%
  mutate(year = 2021,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(cnty_fips, cnty_name, year, fct1, fct2, fct3, fct4, norm_dei)

# combine all years
dei_cnty_index <- rbind(dei_cnty_mat_2019,
                        dei_cnty_mat_2020,
                        dei_cnty_mat_2021)

dei_cnty_index <- dei_cnty_index %>%
  group_by(cnty_fips) %>%
  arrange(year, .by_group = TRUE) %>%
  rename(geoid = cnty_fips,
         region_name = cnty_name) %>%
  pivot_longer(!c(geoid, region_name, year), names_to = "measure", values_to = "value") %>%
  mutate(region_type = "county",
         measure_type = "index") %>%
  select(geoid, region_type, region_name, year, measure, value, measure_type) %>%
  as.data.frame()


### health district level

cnty_hd_map <- read.csv("/project/biocomplexity/sdad/projects_data/vdh/va_county_to_hd.csv")
cnty_hd_map$county_id <- as.character(cnty_hd_map$county_id)

acs_hd_2019 <- get_acs(geography = "county",
                       state = 51,
                       variables = acs_vars,
                       year = 2019,
                       survey = "acs5",
                       cache_table = TRUE,
                       output = "wide",
                       geometry = FALSE,
                       keep_geo_vars = FALSE)

acs_hd_2019 <- acs_hd_2019 %>%
  left_join(cnty_hd_map[,c("county_id", "health_district")], by = c("GEOID" = "county_id")) %>%
  select(cnty_fips = GEOID,
         seq(3, 115, 2)) %>%
  group_by(health_district) %>%
  summarise_if(is.numeric, sum) %>%
  as.data.frame()

acs_hd_2019 <- acs_hd_2019 %>% transmute(
  health_district,
  perc_65_under = 100 - ((B01001_020E + B01001_021E + B01001_022E + B01001_023E +
                            B01001_024E + B01001_025E + B01001_044E + B01001_045E +
                            B01001_046E + B01001_047E + B01001_048E + B01001_049E) / B01001_001E * 100),
  perc_over_HS = 100 - ((B15001_012E + B15001_013E + B15001_020E + B15001_021E + B15001_028E +
                           B15001_029E + B15001_036E + B15001_037E +
                           B15001_053E + B15001_054E + B15001_061E + B15001_062E + B15001_069E +
                           B15001_070E + B15001_077E + B15001_078E) / B15001_001E * 100),
  not_pov = 100 - ((B17001_002E / B17001_001E) * 100),
  perc_no_disability = 100 - ((B18101_004E + B18101_007E + B18101_010E + B18101_013E +
                                 B18101_016E + B18101_019E +
                                 B18101_035E + B18101_038E) / B18101_001E * 100),
  IIR = ((B28004_023E + B28004_024E) / B28004_001E) / ((B28004_005E + B28004_009E + B28004_013E) / B28004_001E),
  perc_have_computer = 100 - ((B28001_011E / B28001_001E) * 100),
  perc_have_internet_access = 100 - ((B28002_013E / B28002_001E) * 100)
)

va_chas$CNTY_FIPS <- as.character(va_chas$CNTY_FIPS)

va_chas_hd <- va_chas %>%
  select(cnty_fips = CNTY_FIPS,
         total_hh = T2_EST1,
         num_low_income_house_burden_30 = T8_LE50_CB) %>%
  left_join(cnty_hd_map, by = c("cnty_fips" = "county_id")) %>%
  group_by(health_district) %>%
  summarise_if(is.numeric, sum) %>%
  mutate(perc_low_income_house_burden_30 = 100 - ((num_low_income_house_burden_30 / total_hh) *100)) %>%
  as.data.frame()


### get Ookla data ###
conn <- dbConnect(drv = PostgreSQL(), dbname = "sdad",
                  host = "10.250.124.195",
                  port = 5432,
                  user = Sys.getenv(x = "db_userid"),
                  password = Sys.getenv(x = "db_pwd"))

ookla_data_hd <- st_read(conn, query = "SELECT * FROM dc_digital_communications.va_hd_ookla_2019_2021_speed_measurements")

dbDisconnect(conn)

ookla_data_hd <- ookla_data_hd %>%
  select(health_district = region_name,
         year,
         measure,
         value) %>%
  pivot_wider(health_district, names_from = c(measure, year), values_from = value)

ookla_hd_2019 <- ookla_data_hd %>%
  select(health_district,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  as.data.frame()

ookla_hd_2020 <- ookla_data_hd %>%
  select(health_district,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  as.data.frame()

ookla_hd_2021 <- ookla_data_hd %>%
  select(health_district,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  as.data.frame()


## get predictions for 2019, 2020, 2021 ##

# 2019
dei_hd_pred_2019 <- acs_hd_2019 %>%
  left_join(va_chas_hd, by = "health_district") %>%
  left_join(ookla_hd_2019, by = "health_district") %>%
  select(health_district,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_hd_pred_2019 <- dei_hd_pred_2019[complete.cases(dei_hd_pred_2019),]

hd_index_mat_2019 <- newFactors(scale(dei_vars),
                                scale(dei_hd_pred_2019 %>% select(-c(health_district))),
                                fa_varimax_tract)
colnames(hd_index_mat_2019) <- c("fct1", "fct2", "fct3", "fct4")
dei_hd_mat_2019 <- as.data.frame(cbind(dei_hd_pred_2019, hd_index_mat_2019))

dei_hd_mat_2019 <- dei_hd_mat_2019 %>%
  mutate(year = 2019,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(health_district, year, fct1, fct2, fct3, fct4, norm_dei)

# 2020
dei_hd_pred_2020 <- acs_hd_2019 %>%
  left_join(va_chas_hd, by = "health_district") %>%
  left_join(ookla_hd_2020, by = "health_district") %>%
  select(health_district,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_hd_pred_2020 <- dei_hd_pred_2020[complete.cases(dei_hd_pred_2020),]

hd_index_mat_2020 <- newFactors(scale(dei_vars),
                                scale(dei_hd_pred_2020 %>% select(-c(health_district))),
                                fa_varimax_tract)
colnames(hd_index_mat_2020) <- c("fct1", "fct2", "fct3", "fct4")
dei_hd_mat_2020 <- as.data.frame(cbind(dei_hd_pred_2020, hd_index_mat_2020))

dei_hd_mat_2020 <- dei_hd_mat_2020 %>%
  mutate(year = 2020,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(health_district, year, fct1, fct2, fct3, fct4, norm_dei)

# 2021
dei_hd_pred_2021 <- acs_hd_2019 %>%
  left_join(va_chas_hd, by = "health_district") %>%
  left_join(ookla_hd_2021, by = "health_district") %>%
  select(health_district,
         perc_65_under,
         perc_over_HS,
         not_pov,
         perc_no_disability,
         IIR,
         perc_have_computer,
         perc_have_internet_access,
         perc_low_income_house_burden_30,
         download,
         upload,
         latency) %>%
  as.data.frame()

dei_hd_pred_2021 <- dei_hd_pred_2021[complete.cases(dei_hd_pred_2021),]

hd_index_mat_2021 <- newFactors(scale(dei_vars),
                                scale(dei_hd_pred_2021 %>% select(-c(health_district))),
                                fa_varimax_tract)
colnames(hd_index_mat_2021) <- c("fct1", "fct2", "fct3", "fct4")
dei_hd_mat_2021 <- as.data.frame(cbind(dei_hd_pred_2021, hd_index_mat_2021))

dei_hd_mat_2021 <- dei_hd_mat_2021 %>%
  mutate(year = 2021,
         dei = fct1 + fct2 + fct3 + fct4,
         min_dei = min(dei, na.rm = TRUE),
         max_dei = max(dei, na.rm = TRUE),
         norm_dei = ((dei - min_dei) / (max_dei - min_dei)) * 100) %>%
  select(health_district, year, fct1, fct2, fct3, fct4, norm_dei)

# combine all years
dei_hd_index <- rbind(dei_hd_mat_2019,
                      dei_hd_mat_2020,
                      dei_hd_mat_2021)

# load in standard health district geographic names
conn <- dbConnect(drv = PostgreSQL(),
                  dbname = "sdad",
                  host = "10.250.124.195",
                  port = 5432,
                  user = Sys.getenv(x = "db_userid"),
                  password = Sys.getenv(x = "db_pwd"))

hd_geography_names <- st_read(conn, query = "SELECT * FROM dc_geographies.va_hd_vdh_2021_health_district_geo_names")

dbDisconnect(conn)

dei_hd_index <- dei_hd_index %>%
  rename(region_name = health_district) %>%
  left_join(hd_geography_names, by = "region_name")

dei_hd_index <- dei_hd_index %>%
  group_by(region_name) %>%
  arrange(year, .by_group = TRUE) %>%
  pivot_longer(!c(geoid, region_name, region_type, year), names_to = "measure", values_to = "value") %>%
  mutate(measure_type = "index") %>%
  select(geoid, region_type, region_name, year, measure, value, measure_type) %>%
  as.data.frame()



### write DEI results to database ###

conn <- dbConnect(drv = PostgreSQL(),
                  dbname = "sdad",
                  host = "10.250.124.195",
                  port = 5432,
                  user = Sys.getenv(x = "db_userid"),
                  password = Sys.getenv(x = "db_pwd")
)

# write to data commons database, auto-detect if writing geom or not, change owner to data_commons
dc_dbWriteTable <-
  function(
    db_conn,
    schema_name,
    table_name,
    table_data,
    table_owner = "data_commons"
  ) {
    # check for geometry/geography columns
    tf <- sapply(table_data, {function(x) inherits(x, 'sfc')})
    # if TRUE, use sf
    if (TRUE %in% tf) {
      sf_write_result <- sf::st_write(obj = table_data, dsn = db_conn, layer = c(schema_name, table_name), row.names = FALSE)
      print(sf_write_result)
      # if FALSE, use DBI
    } else {
      write_result <- DBI::dbWriteTable(conn = db_conn, name = c(schema_name, table_name), value = table_data, row.names = FALSE, overwrite = TRUE)
      print(write_result)
    }
    # change table owner
    chgown_result <- DBI::dbSendQuery(conn = db_conn, statement = paste0("ALTER TABLE ", schema_name, ".", table_name, " OWNER TO ", table_owner))
    print(chgown_result)
  }

dc_dbWriteTable(conn,
                "dc_digital_communications",
                "va_tr_sdad_2019_2021_dei",
                dei_tract_index,
                "data_commons")

dc_dbWriteTable(conn,
                "dc_digital_communications",
                "va_bg_sdad_2019_2021_dei",
                dei_bg_index,
                "data_commons")

dc_dbWriteTable(conn,
                "dc_digital_communications",
                "va_ct_sdad_2019_2021_dei",
                dei_cnty_index,
                "data_commons")

dc_dbWriteTable(conn,
                "dc_digital_communications",
                "va_hd_sdad_2019_2021_dei",
                dei_hd_index,
                "data_commons")

dbDisconnect(conn)
