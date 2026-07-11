# 04_import_spatial.R — geocodeer-/spatial-output van GeoDMS terugkoppelen en
# de analyseset opbouwen (schoning + afgeleide variabelen)
#
# Twee invoervarianten:
#   variant = "legacy"  : de bestaande spatial-CSV van de oude flow
#                         (NVM_1985_2023_cleaned_geocoded_20251024_slim_spatial.csv).
#                         Dient om de R-schattingen (stap 05) te valideren tegen de
#                         Stata-resultaten Estimates_20251024_*.csv.
#   variant = "pipeline": de nieuwe keten — geocodeerresultaat + spatial vars uit
#                         GeoDMS, gekoppeld op geocode_id (= trans_id uit stap 02).
#                         Wordt definitief aangesloten zodra de GeoDMS-run staat
#                         (incl. #18 OV-knooppunt, #19 UAI 2012, #20 zonder groen).
#
# Schoningsregels vertaald uit PrijsIndex_tbv_RS.do (regels 43-110).

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "00_config.R"))

if (!exists("variant_04")) variant_04 <- "legacy"

if (variant_04 == "legacy") {
  ri_log("Lees legacy spatial-CSV: %s", cfg$file_legacy_spatial)
  a <- fread(cfg$file_legacy_spatial, sep = ";", na.strings = c("", "null", "NULL"))
  setnames(a, old = c("size [m^2]", "lotsize [m^2]", "UAI_2021",
                      "tt_500k_inw_2020_min", "tt_trainstation_2006_min"),
              new = c("size", "lotsize", "uai", "tt_500k_min", "tt_station_min"),
           skip_absent = TRUE)
  a[, c("geometry", "rdc_10m_rel", "rdc_100m_rel") := NULL]
  setnames(a, "buildingyear", "bouwjaar", skip_absent = TRUE)
} else if (variant_04 == "pipeline") {
  stop("variant 'pipeline' wordt aangesloten zodra de nieuwe GeoDMS-run beschikbaar is ",
       "(geocodering + spatial vars op basis van stap 03).")
} else stop("Onbekende variant_04: ", variant_04)

## ---------------------------------------------------------------------------
## Schoning (volgorde als in Stata)
## ---------------------------------------------------------------------------
a[!is.na(bouwjaar) & bouwjaar > 2025, bouwjaar := NA]
a[!is.na(bouwjaar) & bouwjaar < 1000, bouwjaar := NA]
a[!is.na(bouwjaar) & bouwjaar < 1600, bouwjaar := 1600]
a[!is.na(lotsize)  & lotsize >= 99999, lotsize := NA]
a[!is.na(tt_station_min) & tt_station_min < 1, tt_station_min := 1]
a[!is.na(lotsize)  & lotsize == 0, lotsize := 1]

## ---------------------------------------------------------------------------
## Afgeleide variabelen
## ---------------------------------------------------------------------------
# dummies kunnen als logical/tekst binnenkomen uit de CSV
for (v in intersect(c("d_apartment", "d_terraced", "d_semidetached", "d_detached",
                      "d_maintgood", "d_groennabij"), names(a))) {
  if (!is.numeric(a[[v]])) a[, (v) := as.integer(as.logical(get(v)))]
}
# legacy: altijd Stata-compat (onbekende hoogte = hoogbouw), want deze variant
# dient om Estimates_20251024 bit-voor-bit te reproduceren; pipeline volgt cfg.
na_as_high <- if (variant_04 == "legacy") TRUE else isTRUE(cfg$highrise_na_as_high)
if (na_as_high) {
  a[, d_highrise := as.integer(is.na(bag_pand_hoogte) | bag_pand_hoogte >= cfg$hoogbouwgrens_cm)]
} else {
  a[, d_highrise := as.integer(!is.na(bag_pand_hoogte) & bag_pand_hoogte >= cfg$hoogbouwgrens_cm)]
}
a[, d_hoogte_onbekend := as.integer(is.na(bag_pand_hoogte))]
a[, lnprice     := log(price)]
a[, lnsize      := log(size)]
a[, lnlotsize   := log(lotsize)]
a[, lntt_500k   := log(tt_500k_min)]
a[, lntt_station := log(tt_station_min)]
a[, pricem2     := price / size]

# bouwperiode: 8 klassen; referentie (baseline) is 'va2002'
a[, bouwperiode := cut(bouwjaar,
                       breaks = c(-Inf, 1925, 1950, 1965, 1973, 1981, 1991, 2001, Inf),
                       labels = c("tm1925", "1926_1950", "1951_1965", "1966_1973",
                                  "1974_1981", "1982_1991", "1992_2001", "va2002"))]
a[, bouwperiode := relevel(bouwperiode, ref = "va2002")]

a[, building_type := fcase(d_apartment    == 1, "apartment",
                           d_terraced     == 1, "terraced",
                           d_semidetached == 1, "semidetached",
                           d_detached     == 1, "detached",
                           default = NA_character_)]

ri_log("Analyseset: %s rijen", format(nrow(a), big.mark = ","))
print(a[, .N, by = building_type])

out <- cfg$file_04_analysis(variant_04)
saveRDS(a, out, compress = FALSE)
ri_log("Weggeschreven: %s", out)
