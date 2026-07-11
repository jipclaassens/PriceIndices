# 03_export_geocode.R — slank adresbestand wegschrijven voor geocodering in GeoDMS
#
# GeoDMS (BAG-Tools) koppelt op postcode/huisnummer/toevoeging/straat/woonplaats en
# levert per geocode_id: X/Y, regiocodes, matchkwaliteit en BAG-attributen
# (bag_pand_bouwjaar, bag_pand_hoogte, bag_vbo_oppervlak, bag_pand_footprint).
# Het resultaat wordt in 04_import_spatial.R teruggekoppeld op geocode_id (= trans_id).

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "00_config.R"))

d <- readRDS(cfg$file_02_cleaned)

adres <- d[, .(geocode_id = trans_id,
               straatnaam,
               huisnummer = as.integer(huisnummer),
               huisnummertoevoeging,
               postcode,
               woonplaats)]

dir.create(dirname(cfg$file_03_geocode), recursive = TRUE, showWarnings = FALSE)
fwrite(adres, cfg$file_03_geocode, sep = ";")
ri_log("Geocode-invoer weggeschreven: %s (%s adressen)",
       cfg$file_03_geocode, format(nrow(adres), big.mark = ","))
ri_log("Volgende stap: geocoderen + spatial vars in GeoDMS, daarna 04_import_spatial.R")
