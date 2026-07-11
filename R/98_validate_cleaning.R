# 98_validate_cleaning.R — schone set (stap 02) vergelijken met de oude Stata-referentie
#
# Referentie: EnhancedData/NVM_1985_2023_20250519_cleaned.dta (oude flow).
# Verwachte, verklaarbare verschillen (zie R/README.md):
#   - nieuwe set is GROTER doordat het x/y-filter pas in stap 04 valt
#     (referentie gooide niet-geocodeerde records al weg);
#   - nieuwe set is KLEINER waar de strakkere ontdubbeling (adres+type+datum+prijs
#     over de leveringen heen) dubbelingen vindt die Stata's fuzzy sleutel miste;
#   - bouwjaar pre-2022 is hier nog de NVM-klassemiddenwaarde (BAG volgt in stap 04),
#     in de referentie al het BAG-bouwjaar van de oude geocodering.

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "00_config.R"))

ri_log("Lees referentie: %s", cfg$file_ref_cleaned)
ref <- as.data.table(haven::read_dta(cfg$file_ref_cleaned,
  col_select = any_of(c("trans_year", "transactieprijs", "oppervlak", "nkamers",
                        "bouwjaar", "d_apartment", "d_terraced", "d_semidetached",
                        "d_detached", "x", "y", "houseid"))))
nieuw <- readRDS(cfg$file_02_cleaned)

cat(sprintf("\nTotalen:  referentie %s | nieuw %s (verschil %+d)\n",
            format(nrow(ref), big.mark = ","), format(nrow(nieuw), big.mark = ","),
            nrow(nieuw) - nrow(ref)))

## N per jaar naast elkaar
per_jaar <- merge(
  ref[,   .(n_ref  = .N), keyby = trans_year],
  nieuw[, .(n_nieuw = .N), keyby = trans_year], all = TRUE)
per_jaar[, verschil := n_nieuw - n_ref]
per_jaar[, pct := round(100 * verschil / n_ref, 1)]
cat("\nN per transactiejaar:\n"); print(per_jaar, nrows = 50)

## typeverdeling
type_ref <- ref[, .(
  apartment = sum(d_apartment), terraced = sum(d_terraced),
  semidetached = sum(d_semidetached), detached = sum(d_detached))]
cat("\nTypeverdeling referentie:\n"); print(type_ref)
cat("Typeverdeling nieuw:\n"); print(dcast(nieuw[, .N, by = building_type],
                                           . ~ building_type, value.var = "N"))

## kernstatistieken
vgl <- rbind(
  data.table(set = "referentie",
             prijs_mediaan = median(ref$transactieprijs),
             m2_mediaan    = median(ref$oppervlak),
             bouwjaar_med  = median(ref$bouwjaar, na.rm = TRUE)),
  data.table(set = "nieuw",
             prijs_mediaan = median(nieuw$transactieprijs),
             m2_mediaan    = median(nieuw$oppervlak),
             bouwjaar_med  = median(nieuw$bouwjaar, na.rm = TRUE)))
cat("\nKernstatistieken:\n"); print(vgl)

## hoeveel van het verschil komt door ontbrekende geocodering in de referentie?
if ("x" %in% names(ref)) {
  n_xy_ok <- ref[!is.na(x) & x >= 0 & x <= 280000 & !is.na(y) & y >= 300000 & y <= 625000, .N]
  cat(sprintf("\nReferentie binnen x/y-grenzen: %s (referentie paste dit filter al toe)\n",
              format(n_xy_ok, big.mark = ",")))
}
cat(sprintf("Nieuw: nog zonder x/y-filter; adressen zonder geldige postcode+huisnummer: %s\n",
            format(nieuw[postcode == "" | is.na(huisnummer), .N], big.mark = ",")))
