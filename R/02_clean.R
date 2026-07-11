# 02_clean.R — filteren, adressen opschonen, ontdubbelen, huis-id's
#
# Vertaling van NVM_merge_pre2022_metna2022.do (regels 149-324) met twee bewuste
# afwijkingen (zie ook R/README.md):
#  1. Geen x/y-filter hier: coordinaten bestaan pas na geocodering (stap 04).
#  2. Ontdubbeling op (adres, woningtype, datum, prijs) met de post-2022-levering
#     als prioriteit, i.p.v. Stata's fuzzy houseid1+datum+prijs. Hierdoor worden
#     dubbelingen tussen de twee leveringen ook gevonden wanneer m2/bouwjaar net
#     verschillen (ander meetprotocol oud/nieuw), en is het resultaat deterministisch.

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "00_config.R"))

d <- readRDS(cfg$file_01_merged)
ri_log("Gelezen: %s rijen uit %s", format(nrow(d), big.mark = ","), basename(cfg$file_01_merged))

## ---------------------------------------------------------------------------
## 1. Filters (Stata-semantiek: NA in prijs/oppervlak/kamers valt ook af)
## ---------------------------------------------------------------------------
drop_stat <- function(lbl, cond) {
  n0 <- nrow(d); d <<- d[!(cond)]
  ri_log("  filter %-28s -%s rijen (blijft: %s)", lbl,
         format(n0 - nrow(d), big.mark = ","), format(nrow(d), big.mark = ","))
}
drop_stat("prijs > max of NA",   is.na(d$transactieprijs) | d$transactieprijs > cfg$maxprice)
drop_stat("prijs < min",         d$transactieprijs < cfg$minprice)
drop_stat("oppervlak > max/NA",  is.na(d$oppervlak) | d$oppervlak > cfg$maxsize)
drop_stat("oppervlak < min",     d$oppervlak < cfg$minsize)
prijsm2 <- d$transactieprijs / d$oppervlak
drop_stat("prijs/m2 buiten grens", prijsm2 > cfg$maxpricesqm | prijsm2 < cfg$minpricesqm)
rm(prijsm2)
drop_stat("kamers > max of NA",  is.na(d$nkamers) | d$nkamers > cfg$maxrooms)

## ---------------------------------------------------------------------------
## 2. Adresvelden opschonen
## ---------------------------------------------------------------------------
d[, huisnummertoevoeging := clean_toevoeging(huisnummertoevoeging)]
d[, postcode             := clean_postcode(postcode)]
d[, straatnaam           := clean_straatnaam(straatnaam)]
d[!is.na(huisnummer) & huisnummer == 0, huisnummer := NA]

## ---------------------------------------------------------------------------
## 3. Bouwjaar aanvullen vanuit andere records van hetzelfde adres+type
## ---------------------------------------------------------------------------
d[, houseid0 := .GRP, by = .(postcode, huisnummer, huisnummertoevoeging, building_type)]
d[, bj_med := suppressWarnings(as.integer(trunc(median(bouwjaar, na.rm = TRUE)))), by = houseid0]
n_fill <- d[is.na(bouwjaar) & !is.na(bj_med), .N]
d[is.na(bouwjaar) & !is.na(bj_med), c("bouwjaar", "bouwjaar_src") := .(bj_med, "afgeleid_ander_record")]
d[, bj_med := NULL]
ri_log("Bouwjaar aangevuld vanuit duplicaatrecords: %s rijen", format(n_fill, big.mark = ","))

## ---------------------------------------------------------------------------
## 4. Ontdubbelen: zelfde adres, type, transactiedatum en prijs = zelfde transactie
##    Nieuwste levering (post2022) wint; ontbrekende velden op de bewaarde rij
##    worden gevuld vanuit de weggegooide duplicaatrijen.
## ---------------------------------------------------------------------------
d[, rij_id := .I]
d[, bron_prio := fifelse(bron == "post2022", 0L, 1L)]
d[, dup_id := .GRP, by = .(postcode, huisnummer, huisnummertoevoeging, building_type,
                           trans_date, transactieprijs)]
d[, dup_n := .N, by = dup_id]

n_multi <- d[dup_n > 1L, uniqueN(dup_id)]
ri_log("Duplicaatgroepen: %s (rijen daarin: %s)",
       format(n_multi, big.mark = ","), format(d[dup_n > 1L, .N], big.mark = ","))

d_multi <- d[dup_n > 1L]
setorder(d_multi, dup_id, bron_prio, rij_id)
sleutel <- c("postcode", "huisnummer", "huisnummertoevoeging", "building_type",
             "trans_date", "transactieprijs")
vulvars <- setdiff(names(d_multi), c(sleutel, "rij_id", "bron_prio", "dup_id", "dup_n",
                                     "bron", "houseid0", "trans_year", "trans_month", "trans_day",
                                     "d_apartment", "d_terraced", "d_semidetached", "d_detached"))
fill_from_dups(d_multi, vulvars, grp = "dup_id")
d_multi <- d_multi[d_multi[, .I[1L], by = dup_id]$V1]

n0 <- nrow(d)
d <- rbindlist(list(d[dup_n == 1L], d_multi), use.names = TRUE)
rm(d_multi); invisible(gc(verbose = FALSE))
ri_log("Ontdubbeld: -%s rijen (blijft: %s)",
       format(n0 - nrow(d), big.mark = ","), format(nrow(d), big.mark = ","))

## ---------------------------------------------------------------------------
## 5. Huis-id voor herhaalde verkopen (repeat sales), zoals in Stata:
##    adres+type met tolerantiegroepen voor m2 (~10), kamers (~5) en bouwjaar (~5)
##    rond het groepsgemiddelde; onwaarschijnlijk vaak verhandelde 'huizen' worden
##    met een gezaaide random opgesplitst.
## ---------------------------------------------------------------------------
d[, houseid0 := .GRP, by = .(postcode, huisnummer, huisnummertoevoeging, building_type)]
d[, `:=`(m2_gem = mean(oppervlak, na.rm = TRUE),
         nk_gem = mean(nkamers,   na.rm = TRUE),
         bj_gem = mean(bouwjaar,  na.rm = TRUE)), by = houseid0]
d[, `:=`(b_m2 = trunc_bucket(oppervlak, m2_gem, 10),
         b_nk = trunc_bucket(nkamers,   nk_gem,  5),
         b_bj = trunc_bucket(bouwjaar,  bj_gem,  5))]
d[, houseid1 := .GRP, by = .(houseid0, b_m2, b_nk, b_bj)]
d[, c("m2_gem", "nk_gem", "bj_gem", "b_m2", "b_nk", "b_bj") := NULL]

d[, times := .N, by = houseid1]
d[, yeartimes := .N, by = .(houseid1, trans_year)]
set.seed(cfg$seed)
d[, rnd := 0]
d[times > 15,     rnd := runif(.N)]   # >15 transacties in ~35 jaar: geen echt huis
d[yeartimes > 1,  rnd := runif(.N)]   # >1 transactie per jaar: opsplitsen
d[, houseid := .GRP, by = .(houseid1, rnd)]
d[, c("houseid0", "houseid1", "times", "yeartimes", "rnd", "rij_id",
      "bron_prio", "dup_id", "dup_n") := NULL]

## ---------------------------------------------------------------------------
## 6. Onderhoudsdummies afleiden (na de dedup, zodat een uit de andere levering
##    aangevulde maint_score meetelt)
## ---------------------------------------------------------------------------
if (isTRUE(cfg$maint_na_as_good)) {
  d[, d_maintgood := as.integer(is.na(maint_score) | maint_score > 1.375)]
} else {
  d[, d_maintgood := as.integer(!is.na(maint_score) & maint_score > 1.375)]
}
d[, d_maint_onbekend := as.integer(is.na(maint_score))]

## ---------------------------------------------------------------------------
## 7. Definitieve sortering en transactie-id (= geocode-sleutel in stap 03/04)
## ---------------------------------------------------------------------------
setorder(d, trans_date, postcode, huisnummer, huisnummertoevoeging,
         transactieprijs, na.last = FALSE)
d[, trans_id := .I]
setcolorder(d, c("trans_id", "houseid", "trans_date", "trans_year", "trans_month", "trans_day",
                 "transactieprijs", "oppervlak", "perceel", "nkamers", "bouwjaar", "bouwjaar_src",
                 "building_type", "d_apartment", "d_terraced", "d_semidetached", "d_detached",
                 "d_maintgood", "d_monument", "straatnaam", "huisnummer", "huisnummertoevoeging",
                 "postcode", "woonplaats", "bron"))

ri_log("Schone set: %s rijen; herhaalde verkopen: %s huizen met >1 transactie",
       format(nrow(d), big.mark = ","),
       format(d[, .N, by = houseid][N > 1, .N], big.mark = ","))
print(d[, .N, keyby = .(trans_year)][, .(trans_year, N)][order(trans_year)][seq(1, .N, by = 5)])
print(d[, .N, by = .(bron, building_type)])

saveRDS(d, cfg$file_02_cleaned, compress = FALSE)
ri_log("Weggeschreven: %s", cfg$file_02_cleaned)

if (isTRUE(cfg$write_onedrive_exports)) {
  out_csv <- file.path(cfg$dir_enhanced, sprintf("NVM_1985_2023_%s_cleaned.csv", cfg$tag))
  fwrite(d, out_csv, sep = ";")
  ri_log("CSV-export: %s", out_csv)
}
