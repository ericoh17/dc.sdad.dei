
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


capital_region_df <- read.csv("/project/biocomplexity/sdad/projects_data/mc/data_commons/capital_regions_df.csv")

va_capital_region <- capital_region_df %>% filter(state_code == 51)
va_capital_reg_fips <- substr(va_capital_region$cnty_fips, 3, 5)

dc_capital_region <- capital_region_df %>% filter(state_code == 11)
dc_capital_reg_fips <- substr(dc_capital_region$cnty_fips, 3, 5)

md_capital_region <- capital_region_df %>% filter(state_code == 24)
md_capital_reg_fips <- substr(md_capital_region$cnty_fips, 3, 5)

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


### get ACS data ###

# VA #
acs_va_tract <- get_acs(geography = "tract",
                        state = 51,
                        county = va_capital_reg_fips,
                        variables = acs_vars,
                        year = 2019,
                        survey = "acs5",
                        cache_table = TRUE,
                        output = "wide",
                        geometry = FALSE,
                        keep_geo_vars = FALSE)

# for now, replace 0 counts in IIR denom cats with 1
acs_va_tract$B28004_005E[acs_va_tract$B28004_005E == 0] <- 1
acs_va_tract$B28004_009E[acs_va_tract$B28004_009E == 0] <- 1
acs_va_tract$B28004_013E[acs_va_tract$B28004_013E == 0] <- 1

acs_va_tract <- acs_va_tract %>% transmute(
  tract_fips = GEOID,
  tract_name = NAME,
  cnty_fips = substr(tract_fips, 1, 5),
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

# DC #
acs_dc_tract <- get_acs(geography = "tract",
                        state = 11,
                        county = dc_capital_reg_fips,
                        variables = acs_vars,
                        year = 2019,
                        survey = "acs5",
                        cache_table = TRUE,
                        output = "wide",
                        geometry = FALSE,
                        keep_geo_vars = FALSE)

# for now, replace 0 counts in IIR denom cats with 1
acs_dc_tract$B28004_005E[acs_dc_tract$B28004_005E == 0] <- 1
acs_dc_tract$B28004_009E[acs_dc_tract$B28004_009E == 0] <- 1
acs_dc_tract$B28004_013E[acs_dc_tract$B28004_013E == 0] <- 1

acs_dc_tract <- acs_dc_tract %>% transmute(
  tract_fips = GEOID,
  tract_name = NAME,
  cnty_fips = substr(tract_fips, 1, 5),
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

# MD #
acs_md_tract <- get_acs(geography = "tract",
                        state = 24,
                        county = md_capital_reg_fips,
                        variables = acs_vars,
                        year = 2019,
                        survey = "acs5",
                        cache_table = TRUE,
                        output = "wide",
                        geometry = FALSE,
                        keep_geo_vars = FALSE)

# for now, replace 0 counts in IIR denom cats with 1
acs_md_tract$B28004_005E[acs_md_tract$B28004_005E == 0] <- 1
acs_md_tract$B28004_009E[acs_md_tract$B28004_009E == 0] <- 1
acs_md_tract$B28004_013E[acs_md_tract$B28004_013E == 0] <- 1

acs_md_tract <- acs_md_tract %>% transmute(
  tract_fips = GEOID,
  tract_name = NAME,
  cnty_fips = substr(tract_fips, 1, 5),
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

acs_cr_tract <- rbind(acs_va_tract,
                      acs_dc_tract,
                      acs_md_tract)

### get CHAS data ###
chas <- read.csv("/project/biocomplexity/sdad/projects_data/mc/data_commons/ACS_5_YR_CHAS_Estimate_Data_by_Tract.csv")

cr_chas <- chas[chas$CNTY_FIPS %in% capital_region_df$cnty_fips,]

cr_chas_tract <- cr_chas %>%
  select(tract_fips = GEOID,
         total_hh = T2_EST1,
         num_low_income_house_burden_30 = T8_LE50_CB) %>%
  mutate(perc_low_income_house_burden_30 = 100 - ((num_low_income_house_burden_30 / total_hh) *100)) %>%
  as.data.frame()

cr_chas_tract$tract_fips <- as.character(cr_chas_tract$tract_fips)


### get Ookla data ###
conn <- dbConnect(drv = PostgreSQL(), dbname = "sdad",
                  host = "10.250.124.195",
                  port = 5432,
                  user = Sys.getenv(x = "db_userid"),
                  password = Sys.getenv(x = "db_pwd"))

ookla_va_tr <- st_read(conn, query = "SELECT * FROM dc_digital_communications.va_tr_ookla_2019_2021_speed_measurements")
ookla_dc_tr <- st_read(conn, query = "SELECT * FROM dc_digital_communications.dc_tr_ookla_2019_2021_speed_measurements")
ookla_md_tr <- st_read(conn, query = "SELECT * FROM dc_digital_communications.md_tr_ookla_2019_2021_speed_measurements")

dbDisconnect(conn)

ookla_va_tr <- ookla_va_tr %>%
  select(tract_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(tract_fips, names_from = c(measure, year), values_from = value)


ookla_va_tr_full <- ookla_va_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5),
         total_down_x_devices = avg_down_using_devices_2019 * devices_2019 +
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
         cnty_fips,
         download,
         upload,
         latency) %>%
  filter(cnty_fips %in% va_capital_region$cnty_fips) %>%
  as.data.frame()


ookla_dc_tr <- ookla_dc_tr %>%
  select(tract_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(tract_fips, names_from = c(measure, year), values_from = value)


ookla_dc_tr_full <- ookla_dc_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5),
         total_down_x_devices = avg_down_using_devices_2019 * devices_2019 +
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
         cnty_fips,
         download,
         upload,
         latency) %>%
  filter(cnty_fips %in% dc_capital_region$cnty_fips) %>%
  as.data.frame()


ookla_md_tr <- ookla_md_tr %>%
  select(tract_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(tract_fips, names_from = c(measure, year), values_from = value)


ookla_md_tr_full <- ookla_md_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5),
         total_down_x_devices = avg_down_using_devices_2019 * devices_2019 +
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
         cnty_fips,
         download,
         upload,
         latency) %>%
  filter(cnty_fips %in% md_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_tr_full <- rbind(ookla_va_tr_full,
                       ookla_dc_tr_full,
                       ookla_md_tr_full)

## combine data
dei_tract <- acs_cr_tract %>%
  left_join(cr_chas_tract, by = "tract_fips") %>%
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
         latency)

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
ookla_va_tr_2019 <- ookla_va_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5)) %>%
  select(tract_fips,
         cnty_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  filter(cnty_fips %in% va_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_dc_tr_2019 <- ookla_dc_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5)) %>%
  select(tract_fips,
         cnty_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  filter(cnty_fips %in% dc_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_md_tr_2019 <- ookla_md_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5)) %>%
  select(tract_fips,
         cnty_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  filter(cnty_fips %in% md_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_tr_2019 <- rbind(ookla_va_tr_2019,
                       ookla_dc_tr_2019,
                       ookla_md_tr_2019)

dei_tract_pred_2019 <- acs_cr_tract %>%
  left_join(cr_chas_tract, by = "tract_fips") %>%
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
ookla_va_tr_2020 <- ookla_va_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5)) %>%
  select(tract_fips,
         cnty_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  filter(cnty_fips %in% va_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_dc_tr_2020 <- ookla_dc_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5)) %>%
  select(tract_fips,
         cnty_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  filter(cnty_fips %in% dc_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_md_tr_2020 <- ookla_md_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5)) %>%
  select(tract_fips,
         cnty_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  filter(cnty_fips %in% md_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_tr_2020 <- rbind(ookla_va_tr_2020,
                       ookla_dc_tr_2020,
                       ookla_md_tr_2020)

dei_tract_pred_2020 <- acs_cr_tract %>%
  left_join(cr_chas_tract, by = "tract_fips") %>%
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
ookla_va_tr_2021 <- ookla_va_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5)) %>%
  select(tract_fips,
         cnty_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  filter(cnty_fips %in% va_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_dc_tr_2021 <- ookla_dc_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5)) %>%
  select(tract_fips,
         cnty_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  filter(cnty_fips %in% dc_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_md_tr_2021 <- ookla_md_tr %>%
  mutate(cnty_fips = substr(tract_fips, 1, 5)) %>%
  select(tract_fips,
         cnty_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  filter(cnty_fips %in% md_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_tr_2021 <- rbind(ookla_va_tr_2021,
                       ookla_dc_tr_2021,
                       ookla_md_tr_2021)

dei_tract_pred_2021 <- acs_cr_tract %>%
  left_join(cr_chas_tract, by = "tract_fips") %>%
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

# VA #
acs_va_bg <- get_acs(geography = "block group",
                     state = 51,
                     county = va_capital_reg_fips,
                     variables = acs_vars,
                     year = 2019,
                     survey = "acs5",
                     cache_table = TRUE,
                     output = "wide",
                     geometry = FALSE,
                     keep_geo_vars = FALSE)

# for now, replace 0 counts in IIR denom cats with 1
acs_va_bg$B28004_005E[acs_va_bg$B28004_005E == 0] <- 1
acs_va_bg$B28004_009E[acs_va_bg$B28004_009E == 0] <- 1
acs_va_bg$B28004_013E[acs_va_bg$B28004_013E == 0] <- 1

acs_va_bg <- acs_va_bg %>% transmute(
  bg_fips = GEOID,
  bg_name = NAME,
  tract_fips = substr(bg_fips, 1, 11),
  cnty_fips = substr(bg_fips, 1, 5),
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


tract_bg_merge_va <- merge(acs_va_bg, acs_va_tract, by = "tract_fips")

acs_va_bg$perc_65_under[is.na(acs_va_bg$perc_65_under)] <- tract_bg_merge_va$perc_65_under.y[is.na(tract_bg_merge_va$perc_65_under.x)]
acs_va_bg$perc_over_HS[is.na(acs_va_bg$perc_over_HS)] <- tract_bg_merge_va$perc_over_HS.y[is.na(tract_bg_merge_va$perc_over_HS.x)]
acs_va_bg$not_pov[is.na(acs_va_bg$not_pov)] <- tract_bg_merge_va$not_pov.y[is.na(tract_bg_merge_va$not_pov.x)]
acs_va_bg$perc_no_disability[is.na(acs_va_bg$perc_no_disability)] <- tract_bg_merge_va$perc_no_disability.y[is.na(tract_bg_merge_va$perc_no_disability.x)]
acs_va_bg$IIR[is.na(acs_va_bg$IIR)] <- tract_bg_merge_va$IIR.y[is.na(tract_bg_merge_va$IIR.x)]
acs_va_bg$perc_have_computer[is.na(acs_va_bg$perc_have_computer)] <- tract_bg_merge_va$perc_have_computer.y[is.na(tract_bg_merge_va$perc_have_computer.x)]
acs_va_bg$perc_have_internet_access[is.na(acs_va_bg$perc_have_internet_access)] <- tract_bg_merge_va$perc_have_internet_access.y[is.na(tract_bg_merge_va$perc_have_internet_access.x)]
acs_va_bg$perc_have_broadband[is.na(acs_va_bg$perc_have_broadband)] <- tract_bg_merge_va$perc_have_broadband.y[is.na(tract_bg_merge_va$perc_have_broadband.x)]


# DC #
acs_dc_bg <- get_acs(geography = "block group",
                     state = 11,
                     county = dc_capital_reg_fips,
                     variables = acs_vars,
                     year = 2019,
                     survey = "acs5",
                     cache_table = TRUE,
                     output = "wide",
                     geometry = FALSE,
                     keep_geo_vars = FALSE)

# for now, replace 0 counts in IIR denom cats with 1
acs_dc_bg$B28004_005E[acs_dc_bg$B28004_005E == 0] <- 1
acs_dc_bg$B28004_009E[acs_dc_bg$B28004_009E == 0] <- 1
acs_dc_bg$B28004_013E[acs_dc_bg$B28004_013E == 0] <- 1

acs_dc_bg <- acs_dc_bg %>% transmute(
  bg_fips = GEOID,
  bg_name = NAME,
  tract_fips = substr(bg_fips, 1, 11),
  cnty_fips = substr(bg_fips, 1, 5),
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


tract_bg_merge_dc <- merge(acs_dc_bg, acs_dc_tract, by = "tract_fips")

acs_dc_bg$perc_65_under[is.na(acs_dc_bg$perc_65_under)] <- tract_bg_merge_dc$perc_65_under.y[is.na(tract_bg_merge_dc$perc_65_under.x)]
acs_dc_bg$perc_over_HS[is.na(acs_dc_bg$perc_over_HS)] <- tract_bg_merge_dc$perc_over_HS.y[is.na(tract_bg_merge_dc$perc_over_HS.x)]
acs_dc_bg$not_pov[is.na(acs_dc_bg$not_pov)] <- tract_bg_merge_dc$not_pov.y[is.na(tract_bg_merge_dc$not_pov.x)]
acs_dc_bg$perc_no_disability[is.na(acs_dc_bg$perc_no_disability)] <- tract_bg_merge_dc$perc_no_disability.y[is.na(tract_bg_merge_dc$perc_no_disability.x)]
acs_dc_bg$IIR[is.na(acs_dc_bg$IIR)] <- tract_bg_merge_dc$IIR.y[is.na(tract_bg_merge_dc$IIR.x)]
acs_dc_bg$perc_have_computer[is.na(acs_dc_bg$perc_have_computer)] <- tract_bg_merge_dc$perc_have_computer.y[is.na(tract_bg_merge_dc$perc_have_computer.x)]
acs_dc_bg$perc_have_internet_access[is.na(acs_dc_bg$perc_have_internet_access)] <- tract_bg_merge_dc$perc_have_internet_access.y[is.na(tract_bg_merge_dc$perc_have_internet_access.x)]
acs_dc_bg$perc_have_broadband[is.na(acs_dc_bg$perc_have_broadband)] <- tract_bg_merge_dc$perc_have_broadband.y[is.na(tract_bg_merge_dc$perc_have_broadband.x)]


# MD #
acs_md_bg <- get_acs(geography = "block group",
                     state = 24,
                     county = md_capital_reg_fips,
                     variables = acs_vars,
                     year = 2019,
                     survey = "acs5",
                     cache_table = TRUE,
                     output = "wide",
                     geometry = FALSE,
                     keep_geo_vars = FALSE)

# for now, replace 0 counts in IIR denom cats with 1
acs_md_bg$B28004_005E[acs_md_bg$B28004_005E == 0] <- 1
acs_md_bg$B28004_009E[acs_md_bg$B28004_009E == 0] <- 1
acs_md_bg$B28004_013E[acs_md_bg$B28004_013E == 0] <- 1

acs_md_bg <- acs_md_bg %>% transmute(
  bg_fips = GEOID,
  bg_name = NAME,
  tract_fips = substr(bg_fips, 1, 11),
  cnty_fips = substr(bg_fips, 1, 5),
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
  #IIR = ((B28004_005E + B28004_009E + B28004_013E) / B28004_001E) / ((B28004_023E + B28004_024E) / B28004_001E),
  IIR = ((B28004_023E + B28004_024E) / B28004_001E) / ((B28004_005E + B28004_009E + B28004_013E) / B28004_001E),
  perc_have_computer = 100 - ((B28001_011E / B28001_001E) * 100),
  perc_have_internet_access = 100 - ((B28002_013E / B28002_001E) * 100),
  perc_have_broadband = (B28002_004E / B28002_001E) * 100
) %>%
  as.data.frame()


tract_bg_merge_md <- merge(acs_md_bg, acs_md_tract, by = "tract_fips")

acs_md_bg$perc_65_under[is.na(acs_md_bg$perc_65_under)] <- tract_bg_merge_md$perc_65_under.y[is.na(tract_bg_merge_md$perc_65_under.x)]
acs_md_bg$perc_over_HS[is.na(acs_md_bg$perc_over_HS)] <- tract_bg_merge_md$perc_over_HS.y[is.na(tract_bg_merge_md$perc_over_HS.x)]
acs_md_bg$not_pov[is.na(acs_md_bg$not_pov)] <- tract_bg_merge_md$not_pov.y[is.na(tract_bg_merge_md$not_pov.x)]
acs_md_bg$perc_no_disability[is.na(acs_md_bg$perc_no_disability)] <- tract_bg_merge_md$perc_no_disability.y[is.na(tract_bg_merge_md$perc_no_disability.x)]
acs_md_bg$IIR[is.na(acs_md_bg$IIR)] <- tract_bg_merge_md$IIR.y[is.na(tract_bg_merge_md$IIR.x)]
acs_md_bg$perc_have_computer[is.na(acs_md_bg$perc_have_computer)] <- tract_bg_merge_md$perc_have_computer.y[is.na(tract_bg_merge_md$perc_have_computer.x)]
acs_md_bg$perc_have_internet_access[is.na(acs_md_bg$perc_have_internet_access)] <- tract_bg_merge_md$perc_have_internet_access.y[is.na(tract_bg_merge_md$perc_have_internet_access.x)]
acs_md_bg$perc_have_broadband[is.na(acs_md_bg$perc_have_broadband)] <- tract_bg_merge_md$perc_have_broadband.y[is.na(tract_bg_merge_md$perc_have_broadband.x)]


acs_cr_bg <- rbind(acs_va_bg,
                   acs_dc_bg,
                   acs_md_bg)


### get Ookla data ###
conn <- dbConnect(drv = PostgreSQL(), dbname = "sdad",
                  host = "10.250.124.195",
                  port = 5432,
                  user = Sys.getenv(x = "db_userid"),
                  password = Sys.getenv(x = "db_pwd"))

ookla_va_bg <- st_read(conn, query = "SELECT * FROM dc_digital_communications.va_bg_ookla_2019_2021_speed_measurements")
ookla_dc_bg <- st_read(conn, query = "SELECT * FROM dc_digital_communications.dc_bg_ookla_2019_2021_speed_measurements")
ookla_md_bg <- st_read(conn, query = "SELECT * FROM dc_digital_communications.md_bg_ookla_2019_2021_speed_measurements")

dbDisconnect(conn)

ookla_va_bg <- ookla_va_bg %>%
  select(bg_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(bg_fips, names_from = c(measure, year), values_from = value)

ookla_dc_bg <- ookla_dc_bg %>%
  select(bg_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(bg_fips, names_from = c(measure, year), values_from = value)

ookla_md_bg <- ookla_md_bg %>%
  select(bg_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(bg_fips, names_from = c(measure, year), values_from = value)


## get predictions for 2019, 2020, 2021 ##

# 2019
ookla_va_bg_2019 <- ookla_va_bg %>%
  mutate(cnty_fips = substr(bg_fips, 1, 5)) %>%
  select(bg_fips,
         cnty_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  filter(cnty_fips %in% va_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_dc_bg_2019 <- ookla_dc_bg %>%
  mutate(cnty_fips = substr(bg_fips, 1, 5)) %>%
  select(bg_fips,
         cnty_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  filter(cnty_fips %in% dc_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_md_bg_2019 <- ookla_md_bg %>%
  mutate(cnty_fips = substr(bg_fips, 1, 5)) %>%
  select(bg_fips,
         cnty_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  filter(cnty_fips %in% md_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_bg_2019 <- rbind(ookla_va_bg_2019,
                       ookla_dc_bg_2019,
                       ookla_md_bg_2019)

dei_bg_pred_2019 <- acs_cr_bg %>%
  left_join(cr_chas_tract, by = "tract_fips") %>%
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
ookla_va_bg_2020 <- ookla_va_bg %>%
  mutate(cnty_fips = substr(bg_fips, 1, 5)) %>%
  select(bg_fips,
         cnty_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  filter(cnty_fips %in% va_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_dc_bg_2020 <- ookla_dc_bg %>%
  mutate(cnty_fips = substr(bg_fips, 1, 5)) %>%
  select(bg_fips,
         cnty_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  filter(cnty_fips %in% dc_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_md_bg_2020 <- ookla_md_bg %>%
  mutate(cnty_fips = substr(bg_fips, 1, 5)) %>%
  select(bg_fips,
         cnty_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  filter(cnty_fips %in% md_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_bg_2020 <- rbind(ookla_va_bg_2020,
                       ookla_dc_bg_2020,
                       ookla_md_bg_2020)

dei_bg_pred_2020 <- acs_cr_bg %>%
  left_join(cr_chas_tract, by = "tract_fips") %>%
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
ookla_va_bg_2021 <- ookla_va_bg %>%
  mutate(cnty_fips = substr(bg_fips, 1, 5)) %>%
  select(bg_fips,
         cnty_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  filter(cnty_fips %in% va_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_dc_bg_2021 <- ookla_dc_bg %>%
  mutate(cnty_fips = substr(bg_fips, 1, 5)) %>%
  select(bg_fips,
         cnty_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  filter(cnty_fips %in% dc_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_md_bg_2021 <- ookla_md_bg %>%
  mutate(cnty_fips = substr(bg_fips, 1, 5)) %>%
  select(bg_fips,
         cnty_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  filter(cnty_fips %in% md_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_bg_2021 <- rbind(ookla_va_bg_2021,
                       ookla_dc_bg_2021,
                       ookla_md_bg_2021)

dei_bg_pred_2021 <- acs_cr_bg %>%
  left_join(cr_chas_tract, by = "tract_fips") %>%
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


#### County level DEI ####

### get ACS data ###

# VA #
acs_va_cnty <- get_acs(geography = "county",
                       state = 51,
                       county = va_capital_reg_fips,
                       variables = acs_vars,
                       year = 2019,
                       survey = "acs5",
                       cache_table = TRUE,
                       output = "wide",
                       geometry = FALSE,
                       keep_geo_vars = FALSE)

acs_va_cnty <- acs_va_cnty %>% transmute(
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
  perc_have_internet_access = 100 - ((B28002_013E / B28002_001E) * 100),
  perc_have_broadband = (B28002_004E / B28002_001E) * 100
) %>%
  as.data.frame()

# DC #
acs_dc_cnty <- get_acs(geography = "county",
                       state = 11,
                       county = dc_capital_reg_fips,
                       variables = acs_vars,
                       year = 2019,
                       survey = "acs5",
                       cache_table = TRUE,
                       output = "wide",
                       geometry = FALSE,
                       keep_geo_vars = FALSE)

acs_dc_cnty <- acs_dc_cnty %>% transmute(
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
  perc_have_internet_access = 100 - ((B28002_013E / B28002_001E) * 100),
  perc_have_broadband = (B28002_004E / B28002_001E) * 100
) %>%
  as.data.frame()

# MD #
acs_md_cnty <- get_acs(geography = "county",
                       state = 24,
                       county = md_capital_reg_fips,
                       variables = acs_vars,
                       year = 2019,
                       survey = "acs5",
                       cache_table = TRUE,
                       output = "wide",
                       geometry = FALSE,
                       keep_geo_vars = FALSE)

acs_md_cnty <- acs_md_cnty %>% transmute(
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
  perc_have_internet_access = 100 - ((B28002_013E / B28002_001E) * 100),
  perc_have_broadband = (B28002_004E / B28002_001E) * 100
) %>%
  as.data.frame()

acs_cr_cnty <- rbind(acs_va_cnty,
                     acs_dc_cnty,
                     acs_md_cnty)


### get CHAS data ###
chas <- read.csv("/project/biocomplexity/sdad/projects_data/mc/data_commons/ACS_5_YR_CHAS_Estimate_Data_by_Tract.csv")

cr_chas <- chas[chas$CNTY_FIPS %in% capital_region_df$cnty_fips,]

cr_chas_cnty <- cr_chas %>%
  select(cnty_fips = CNTY_FIPS,
         total_hh = T2_EST1,
         num_low_income_house_burden_30 = T8_LE50_CB) %>%
  group_by(cnty_fips) %>%
  summarise(total_hh_cnty = sum(total_hh),
            num_low_income_house_burden_30_cnty = sum(num_low_income_house_burden_30)) %>%
  mutate(perc_low_income_house_burden_30 = 100 - ((num_low_income_house_burden_30_cnty / total_hh_cnty) *100)) %>%
  as.data.frame()

cr_chas_cnty$cnty_fips <- as.character(cr_chas_cnty$cnty_fips)


### get Ookla data ###
conn <- dbConnect(drv = PostgreSQL(), dbname = "sdad",
                  host = "10.250.124.195",
                  port = 5432,
                  user = Sys.getenv(x = "db_userid"),
                  password = Sys.getenv(x = "db_pwd"))

ookla_va_co <- st_read(conn, query = "SELECT * FROM dc_digital_communications.va_ct_ookla_2019_2021_speed_measurements")
ookla_dc_co <- st_read(conn, query = "SELECT * FROM dc_digital_communications.dc_ct_ookla_2019_2021_speed_measurements")
ookla_md_co <- st_read(conn, query = "SELECT * FROM dc_digital_communications.md_ct_ookla_2019_2021_speed_measurements")

dbDisconnect(conn)

ookla_va_co <- ookla_va_co %>%
  select(cnty_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(cnty_fips, names_from = c(measure, year), values_from = value)

ookla_dc_co <- ookla_dc_co %>%
  select(cnty_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(cnty_fips, names_from = c(measure, year), values_from = value)

ookla_md_co <- ookla_md_co %>%
  select(cnty_fips = geoid,
         year,
         measure,
         value) %>%
  pivot_wider(cnty_fips, names_from = c(measure, year), values_from = value)


## get predictions for 2019, 2020, 2021 ##

# 2019
ookla_va_co_2019 <- ookla_va_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  filter(cnty_fips %in% va_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_dc_co_2019 <- ookla_dc_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  filter(cnty_fips %in% dc_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_md_co_2019 <- ookla_md_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2019,
         upload = avg_up_using_devices_2019,
         latency = avg_lat_using_devices_2019) %>%
  filter(cnty_fips %in% md_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_co_2019 <- rbind(ookla_va_co_2019,
                       ookla_dc_co_2019,
                       ookla_md_co_2019)

dei_cnty_pred_2019 <- acs_cr_cnty %>%
  left_join(cr_chas_cnty, by = "cnty_fips") %>%
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
ookla_va_co_2020 <- ookla_va_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  filter(cnty_fips %in% va_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_dc_co_2020 <- ookla_dc_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  filter(cnty_fips %in% dc_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_md_co_2020 <- ookla_md_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2020,
         upload = avg_up_using_devices_2020,
         latency = avg_lat_using_devices_2020) %>%
  filter(cnty_fips %in% md_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_co_2020 <- rbind(ookla_va_co_2020,
                       ookla_dc_co_2020,
                       ookla_md_co_2020)

dei_cnty_pred_2020 <- acs_cr_cnty %>%
  left_join(cr_chas_cnty, by = "cnty_fips") %>%
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
ookla_va_co_2021 <- ookla_va_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  filter(cnty_fips %in% va_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_dc_co_2021 <- ookla_dc_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  filter(cnty_fips %in% dc_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_md_co_2021 <- ookla_md_co %>%
  select(cnty_fips,
         download = avg_down_using_devices_2021,
         upload = avg_up_using_devices_2021,
         latency = avg_lat_using_devices_2021) %>%
  filter(cnty_fips %in% md_capital_region$cnty_fips) %>%
  as.data.frame()

ookla_co_2021 <- rbind(ookla_va_co_2021,
                       ookla_dc_co_2021,
                       ookla_md_co_2021)

dei_cnty_pred_2021 <- acs_cr_cnty %>%
  left_join(cr_chas_cnty, by = "cnty_fips") %>%
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
                "ncr_tr_sdad_2019_2021_dei",
                dei_tract_index,
                "data_commons")

dc_dbWriteTable(conn,
                "dc_digital_communications",
                "ncr_bg_sdad_2019_2021_dei",
                dei_bg_index,
                "data_commons")

dc_dbWriteTable(conn,
                "dc_digital_communications",
                "ncr_ct_sdad_2019_2021_dei",
                dei_cnty_index,
                "data_commons")

dbDisconnect(conn)

