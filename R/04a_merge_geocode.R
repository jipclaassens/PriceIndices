# 04a_merge_geocode.R — geocodeerresultaat (BAG-Tools) terugkoppelen aan de schone set
#
# Leest NVM_adressen_<tag>_Geocoded.csv en koppelt op geocode_id (= trans_id uit
# stap 02). Filtert op matchkwaliteit en coordinaten, schoont BAG-sentinels en
# consolideert het bouwjaar (verse BAG > Brainbay > NVM-bouwperiodeklasse).
# Output: 04a_geocoded_<tag>.rds — de basis waar stap 04 de spatial vars aan toevoegt.
#
# CSV-eigenaardigheden van de geocoder-export (bewust zo gelaten in BAG-Tools):
#  - elke datarij eindigt op een extra ';' -> 26 velden bij 25 headernamen;
#  - de ge-echode adresvelden bevatten rauwe quotes ("deleeuwerik"straat) en
#    fwrite-artefacten ("" voor lege toevoeging) -> we lezen op positie en
#    gebruiken alleen geocode_id + echo-postcode/huisnummer (integriteitscheck).

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "00_config.R"))

## ---------------------------------------------------------------------------
## 1. Inlezen (op positie; zie kolomlayout in de kop van dit script)
## ---------------------------------------------------------------------------
ri_log("Lees geocodeerresultaat: %s", cfg$file_geocoded)
geo <- suppressWarnings(fread(cfg$file_geocoded, sep = ";", header = FALSE, skip = 1,
    select = c(1, 3, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 22, 23, 24, 25),
    na.strings = c("", "null", "NULL")))
setnames(geo, c("geocode_id", "echo_huisnummer", "echo_postcode",
                "buurt_code", "wijk_code", "gemeente_code",
                "niveau_code", "x", "y", "n_bag_matches", "xy_variantie",
                "nummeraanduiding_id", "bag_pand_footprint", "bag_vbo_oppervlak",
                "bag_pand_bouwjaar", "bag_pand_hoogte"))
if (anyDuplicated(geo$geocode_id)) stop("geocode_id niet uniek in geocodeerresultaat")
ri_log("  %s rijen", format(nrow(geo), big.mark = ","))

d <- readRDS(cfg$file_02_cleaned)
if (nrow(geo) != nrow(d)) ri_log("LET OP: rijen geocoded (%s) != schone set (%s)",
                                 format(nrow(geo), big.mark = ","), format(nrow(d), big.mark = ","))

## ---------------------------------------------------------------------------
## 2. Integriteitscheck: hoort dit geocodeerresultaat bij deze schone set?
##    (na een wijziging in de schoningslogica verschuiven trans_id's; dan is
##    hergeocoderen nodig -- deze check voorkomt een stille misalignment)
## ---------------------------------------------------------------------------
chk <- merge(d[, .(geocode_id = trans_id, postcode, huisnummer)],
             geo[, .(geocode_id, echo_postcode, echo_huisnummer)], by = "geocode_id")
ongelijk <- function(a, b) !((is.na(a) & is.na(b)) | (!is.na(a) & !is.na(b) & a == b))
chk[, mis := ongelijk(fifelse(postcode == "", NA_character_, postcode), echo_postcode) |
             ongelijk(as.integer(huisnummer), as.integer(echo_huisnummer))]
mis_pct <- 100 * chk[, mean(mis)]
ri_log("Integriteitscheck: %.3f%% mismatch op postcode/huisnummer", mis_pct)
if (mis_pct > 0.1) stop(sprintf(
  "Geocodeerresultaat hoort niet bij deze 02-set (%.2f%% mismatch) — hergeocodeer na schoningswijziging (nieuwe tag).", mis_pct))
rm(chk)

## ---------------------------------------------------------------------------
## 3. Schoning geocodeerresultaat
## ---------------------------------------------------------------------------
# BAG-sentinels en onmogelijke waarden -> NA
geo[!is.na(bag_vbo_oppervlak) & bag_vbo_oppervlak >= 99999, bag_vbo_oppervlak := NA]
geo[!is.na(bag_pand_bouwjaar) & (bag_pand_bouwjaar < 1000 | bag_pand_bouwjaar > year(Sys.Date()) + 5),
    bag_pand_bouwjaar := NA]

## ---------------------------------------------------------------------------
## 4. Filters: matchkwaliteit en coordinaten (x/y-filter uit de oude Stata-flow
##    verhuisde hierheen; nieuw is het filter op matchniveau)
## ---------------------------------------------------------------------------
n0 <- nrow(geo)
geo <- geo[!is.na(niveau_code) & niveau_code >= 1L & niveau_code <= cfg$geocode_max_niveau]
ri_log("Filter matchniveau (1..%d): -%s rijen", cfg$geocode_max_niveau,
       format(n0 - nrow(geo), big.mark = ","))
n0 <- nrow(geo)
geo <- geo[!is.na(x) & !is.na(y) & x >= 0 & x <= 280000 & y >= 300000 & y <= 625000]
ri_log("Filter x/y binnen NL:      -%s rijen", format(n0 - nrow(geo), big.mark = ","))

## ---------------------------------------------------------------------------
## 5. Koppelen en consolideren
## ---------------------------------------------------------------------------
geo[, c("echo_postcode", "echo_huisnummer") := NULL]
uit <- merge(d, geo, by.x = "trans_id", by.y = "geocode_id")
rm(d, geo); invisible(gc(verbose = FALSE))

# bouwjaar: verse BAG-koppeling wint van Brainbay-bouwjaar en NVM-klassemidden
uit[!is.na(bag_pand_bouwjaar), c("bouwjaar", "bouwjaar_src") := .(bag_pand_bouwjaar, "bag_geocode")]

# hoogbouw uit verse pandhoogte (cm); onbekend expliciet
if (isTRUE(cfg$highrise_na_as_high)) {
  uit[, d_highrise := as.integer(is.na(bag_pand_hoogte) | bag_pand_hoogte >= cfg$hoogbouwgrens_cm)]
} else {
  uit[, d_highrise := as.integer(!is.na(bag_pand_hoogte) & bag_pand_hoogte >= cfg$hoogbouwgrens_cm)]
}
uit[, d_hoogte_onbekend := as.integer(is.na(bag_pand_hoogte))]

ri_log("Gekoppelde analyse-basis: %s rijen", format(nrow(uit), big.mark = ","))
print(uit[, .N, keyby = .(bouwjaar_src)])
ri_log("d_hoogte_onbekend: %.1f%% | d_highrise: %.1f%%",
       100 * mean(uit$d_hoogte_onbekend), 100 * mean(uit$d_highrise))
print(uit[, .(n = .N, pct_niveau1 = round(100 * mean(niveau_code == 1), 1)), keyby = bron])

saveRDS(uit, cfg$file_04a_geocoded, compress = FALSE)
ri_log("Weggeschreven: %s", cfg$file_04a_geocoded)
ri_log("Volgende: spatial vars (GeoDMS, PriceIndices/main), daarna 04_import_spatial.R variant 'pipeline'")
