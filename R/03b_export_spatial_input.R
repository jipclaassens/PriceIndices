# 03b_export_spatial_input.R — slim-invoerbestand voor de GeoDMS-spatial-pass
#
# Schrijft uit de 04a-basis exact de kolommen (en namen) die
# main/SourceData/NVM.dms verwacht, naar EnhancedData (synct via OneDrive naar
# de machine waar de spatial-pass draait). 'obsid' is hier trans_id: de sleutel
# waarop stap 04 het spatial-resultaat weer terugkoppelt.

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "00_config.R"))

d <- readRDS(cfg$file_04a_geocoded)

slim <- d[, .(obsid = trans_id, x, y,
              d_apartment, d_terraced, d_semidetached, d_detached,
              transactieprijs, oppervlak, perceel,
              trans_year, trans_month, bouwjaar,
              nkamers = as.integer(nkamers), d_maintgood,
              bag_pand_hoogte = as.integer(bag_pand_hoogte))]

fwrite(slim, cfg$file_03b_slim, sep = ";")
ri_log("Spatial-invoer weggeschreven: %s (%s rijen)",
       cfg$file_03b_slim, format(nrow(slim), big.mark = ","))
ri_log("Volgende: GeoDMS main.dms, item /SourceData/NVM/export_spatial/bestand (nvm_filedate = %s)", cfg$tag)
