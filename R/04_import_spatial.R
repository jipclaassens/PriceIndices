# 04_import_spatial.R — analyseset opbouwen: spatial vars + schoning + afgeleiden
#
# Twee invoervarianten:
#   variant = "pipeline" (default): 04a-basis (geocode-merge) + spatial-CSV uit
#             GeoDMS (main/SourceData/NVM/export_spatial), gekoppeld op
#             obsid (= trans_id). Bevat ook de redev-variabelen:
#             uai_2012_network (#19) en tt_OVknooppunten_2026 (#18).
#   variant = "legacy": de oude spatial-CSV van de Stata-flow (20251024), om de
#             R-schattingen te valideren tegen Estimates_20251024_* (zie 99_...).
#
# Schoningsregels vertaald uit PrijsIndex_tbv_RS.do (regels 43-110).

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "00_config.R"))

if (!exists("variant_04")) variant_04 <- "pipeline"

if (variant_04 == "legacy") {
  ri_log("Lees legacy spatial-CSV: %s", cfg$file_legacy_spatial)
  a <- fread(cfg$file_legacy_spatial, sep = ";", na.strings = c("", "null", "NULL"))
  setnames(a, old = c("size [m^2]", "lotsize [m^2]", "UAI_2021",
                      "tt_500k_inw_2020_min", "tt_trainstation_2006_min"),
              new = c("size", "lotsize", "uai", "tt_500k_min", "tt_station_min"),
           skip_absent = TRUE)
  a[, c("geometry", "rdc_10m_rel", "rdc_100m_rel") := NULL]
  setnames(a, "buildingyear", "bouwjaar", skip_absent = TRUE)

  # dummies kunnen als logical/tekst binnenkomen uit de CSV
  for (v in intersect(c("d_apartment", "d_terraced", "d_semidetached", "d_detached",
                        "d_maintgood", "d_groennabij"), names(a))) {
    if (!is.numeric(a[[v]])) a[, (v) := as.integer(as.logical(get(v)))]
  }
  # legacy: altijd Stata-compat (onbekende hoogte = hoogbouw), want deze variant
  # dient om Estimates_20251024 bit-voor-bit te reproduceren
  a[, d_highrise := as.integer(is.na(bag_pand_hoogte) | bag_pand_hoogte >= cfg$hoogbouwgrens_cm)]
  a[, d_hoogte_onbekend := as.integer(is.na(bag_pand_hoogte))]

  a[, building_type := fcase(d_apartment    == 1, "apartment",
                             d_terraced     == 1, "terraced",
                             d_semidetached == 1, "semidetached",
                             d_detached     == 1, "detached",
                             default = NA_character_)]

} else if (variant_04 == "pipeline") {
  ri_log("Lees 04a-basis: %s", cfg$file_04a_geocoded)
  a <- readRDS(cfg$file_04a_geocoded)
  ri_log("Lees spatial vars: %s", cfg$file_spatial)
  sp <- fread(cfg$file_spatial, sep = ";", na.strings = c("", "null", "NULL"))
  nodig <- c("obsid", "uai_2021", "uai_2012_network", "tt_500k_inw_2020_min",
             "tt_500k_inw_2024_min", "tt_trainstation_2006_min",
             "tt_OVknooppunten_2026_min", "d_groennabij")
  weg <- setdiff(nodig, names(sp))
  if (length(weg)) stop("Spatial-CSV mist kolommen: ", paste(weg, collapse = ", "))
  setnames(sp, c("uai_2021", "uai_2012_network", "tt_500k_inw_2020_min",
                 "tt_500k_inw_2024_min", "tt_trainstation_2006_min",
                 "tt_OVknooppunten_2026_min"),
               c("uai", "uai_2012", "tt_500k_min", "tt_500k_2024_min",
                 "tt_station_min", "tt_ovknoop_min"))
  if (anyDuplicated(sp$obsid)) stop("obsid niet uniek in spatial-CSV")

  n0 <- nrow(a)
  a <- merge(a, sp, by.x = "trans_id", by.y = "obsid")
  if (nrow(a) != n0) ri_log("LET OP: %s rijen zonder spatial-match kwijtgeraakt",
                            format(n0 - nrow(a), big.mark = ","))
  rm(sp); invisible(gc(verbose = FALSE))

  # standaardnamen gelijk aan het legacy-pad, zodat stap 05 identiek werkt
  setnames(a, c("transactieprijs", "oppervlak", "nkamers"), c("price", "size", "nrooms"))
  a[, lotsize := perceel]
  # d_highrise en d_hoogte_onbekend komen al uit 04a (verse BAG, cfg-gedrag)

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
for (v in intersect(c("tt_ovknoop_min", "tt_500k_2024_min"), names(a))) {
  a[!is.na(get(v)) & get(v) < 1, (v) := 1]   # log-transform: minimaal 1 minuut
}

## ---------------------------------------------------------------------------
## Afgeleide variabelen
## ---------------------------------------------------------------------------
a[, lnprice      := log(price)]
a[, lnsize       := log(size)]
a[, lnlotsize    := log(lotsize)]
a[, lntt_500k    := log(tt_500k_min)]
a[, lntt_station := log(tt_station_min)]
a[, pricem2      := price / size]
if ("tt_ovknoop_min" %in% names(a))   a[, lntt_ovknoop   := log(tt_ovknoop_min)]
if ("tt_500k_2024_min" %in% names(a)) a[, lntt_500k_2024 := log(tt_500k_2024_min)]

# bouwperiode: 8 klassen; referentie (baseline) is 'va2002'
a[, bouwperiode := cut(bouwjaar,
                       breaks = c(-Inf, 1925, 1950, 1965, 1973, 1981, 1991, 2001, Inf),
                       labels = c("tm1925", "1926_1950", "1951_1965", "1966_1973",
                                  "1974_1981", "1982_1991", "1992_2001", "va2002"))]
a[, bouwperiode := relevel(bouwperiode, ref = "va2002")]

ri_log("Analyseset (%s): %s rijen", variant_04, format(nrow(a), big.mark = ","))
print(a[, .N, by = building_type])

out <- cfg$file_04_analysis(variant_04)
saveRDS(a, out, compress = FALSE)
ri_log("Weggeschreven: %s", out)
