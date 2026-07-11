# 01_merge_sources.R — ruwe NVM-leveringen inlezen, harmoniseren en stapelen
#
# Bronnen (zie 00_config.R):
#   pre2022  : nvm19852021_raw.dta   (oud NVM-format, 1985-2021)
#   post2022 : NVM_2000_2023_raw.dta (Brainbay-format, 2000-2023)
# Overlap 2000-2021 wordt in 02_clean.R ontdubbeld (nieuwe levering leidend).
#
# Vertaling van NVM_merge_pre2022_metna2022.do (regels 17-147), met één
# structurele wijziging: de bronnen zijn hier de échte ruwe leveringen, zonder
# eerdere geocodering. BAG-attributen (o.a. bouwjaar) komen pas in stap 04 terug.

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "00_config.R"))

## ---------------------------------------------------------------------------
## pre-2022 (oud format)
## ---------------------------------------------------------------------------
ri_log("Lees pre-2022 bron: %s", cfg$file_raw_pre2022)
oud <- read_dta_dt(cfg$file_raw_pre2022)
ri_log("  %s rijen, %d kolommen", format(nrow(oud), big.mark = ","), ncol(oud))

oud[, c("pc6code", "gem_id", "prov_id", "nvmreg_id", "afd_id") := NULL]

# oppervlak: m2, met woonopp als vervanging wanneer m2 <= 0
oud[!is.na(m2) & m2 <= 0, m2 := woonopp]
setnames(oud, c("m2", "monument"), c("oppervlak", "d_monument"))

# transactiedatum (datum_afmelding is een Stata-datetime -> UTC-datum)
oud[, trans_date := as.IDate(datum_afmelding, tz = "UTC")]

# onderhoudsstaat: onbu/onbi 1..9 -> 0..1; som > 1.375 = 'goed onderhouden'
oud[, maint_out := fifelse(onbu > 0, (onbu - 1) * 0.125, NA_real_)]
oud[, maint_in  := fifelse(onbi > 0, (onbi - 1) * 0.125, NA_real_)]

# woningtype uit 'type': -1/0 appartement, 1-3 rijtjes, 4 2^1-kap, 5 vrijstaand
oud[, building_type := fcase(type %in% c(-1, 0), "apartment",
                             type %in% 1:3,      "terraced",
                             type %in% 4,        "semidetached",
                             type %in% 5,        "detached",
                             default = NA_character_)]

# bouwjaar: alleen NVM-bouwperiodeklasse beschikbaar (BAG-bouwjaar volgt in stap 04)
bw_mid <- c(`1` = 1900, `2` = 1920, `3` = 1935, `4` = 1950, `5` = 1965,
            `6` = 1975, `7` = 1985, `8` = 1995, `9` = 2001)
oud[, bouwjaar := unname(bw_mid[as.character(bwper)])]
oud[, bouwjaar_src := fifelse(!is.na(bouwjaar), "nvm_bouwperiode", NA_character_)]

oud[, c("woonopp", "type", "bwper", "datum_afmelding") := NULL]
oud[, bron := "pre2022"]

## ---------------------------------------------------------------------------
## post-2022 (Brainbay-format)
## ---------------------------------------------------------------------------
ri_log("Lees post-2022 bron: %s", cfg$file_raw_post2022)
nieuw <- read_dta_dt(cfg$file_raw_post2022)
ri_log("  %s rijen, %d kolommen", format(nrow(nieuw), big.mark = ","), ncol(nieuw))

# administratieve/geo-kolommen eruit; onze eigen geocodering (stap 03/04) is leidend.
# Brainbay-coordinaten blijven bewaard als x_nvm/y_nvm voor QA van de geocodering.
drop_geo <- grep("^(buurt|wijk|gem)_(nr|naam)_20|^(corop|prov)_(nr|naam)$", names(nieuw), value = TRUE)
nieuw[, c(drop_geo, "object_id", "pc4", "bag_adresseerbaarobject_id", "bag_nad_id",
          "lat", "lon", "year", "year2") := NULL]
setnames(nieuw, c("xcoordinaat", "ycoordinaat"), c("x_nvm", "y_nvm"))
nieuw[, x_nvm := trunc(x_nvm)]
nieuw[, y_nvm := trunc(y_nvm)]

setnames(nieuw,
         c("gebruiksoppervlaktewoonfunctie", "perceeloppervlakte", "aantalkamers", "monument"),
         c("oppervlak", "perceel", "nkamers", "d_monument"))

nieuw[, trans_date := as.IDate(datumondertekeningakte, tz = "UTC")]

nieuw[, maint_out := fifelse(onderhoud_buiten > 0, (onderhoud_buiten - 1) * 0.125, NA_real_)]
nieuw[, maint_in  := fifelse(onderhoud_binnen > 0, (onderhoud_binnen - 1) * 0.125, NA_real_)]

# woningtype uit nvm_cijfersnr: 2-4 rijtjes (tussen/schakel/hoek), 5 2^1-kap,
# 6 vrijstaand, >=7 appartement
nieuw[, building_type := fcase(!is.na(nvm_cijfersnr) & nvm_cijfersnr >= 7, "apartment",
                               nvm_cijfersnr %in% 2:4,                     "terraced",
                               nvm_cijfersnr %in% 5,                       "semidetached",
                               nvm_cijfersnr %in% 6,                       "detached",
                               default = NA_character_)]

# bouwjaar wordt door Brainbay meegeleverd (BAG-gebaseerd)
nieuw[, bouwjaar_src := fifelse(!is.na(bouwjaar), "brainbay", NA_character_)]

nieuw[, c("datumondertekeningakte", "onderhoud_binnen", "onderhoud_buiten", "nvm_cijfersnr") := NULL]
nieuw[, bron := "post2022"]

## ---------------------------------------------------------------------------
## stapelen en gedeelde afleidingen
## ---------------------------------------------------------------------------
# onderhoudsscore 0..2 (som binnen+buiten, elk 0..1); NA = onderhoudsstaat onbekend.
# De dummies (d_maintgood, d_maint_onbekend) worden pas in 02 afgeleid, ná de
# ontdubbeling: dan kan een ontbrekende score eerst uit de andere levering komen.
for (dt in list(oud, nieuw)) {
  dt[, maint_score := maint_out + maint_in]
  dt[, c("maint_out", "maint_in") := NULL]
}

harmonize_types(oud, nieuw)
d <- rbindlist(list(oud, nieuw), use.names = TRUE, fill = TRUE)
rm(oud, nieuw); invisible(gc(verbose = FALSE))

d[, trans_year  := year(trans_date)]
d[, trans_month := month(trans_date)]
d[, trans_day   := mday(trans_date)]

d[, postcode := gsub(" ", "", postcode, fixed = TRUE)]
d[!is.na(nkamers) & nkamers == 0, nkamers := 1]

# typedummies voor downstream-compatibiliteit
d[, d_apartment    := as.integer(!is.na(building_type) & building_type == "apartment")]
d[, d_terraced     := as.integer(!is.na(building_type) & building_type == "terraced")]
d[, d_semidetached := as.integer(!is.na(building_type) & building_type == "semidetached")]
d[, d_detached     := as.integer(!is.na(building_type) & building_type == "detached")]

ri_log("Gestapeld: %s rijen, %d kolommen", format(nrow(d), big.mark = ","), ncol(d))
print(d[, .N, by = bron])
print(d[, .(n = .N, prijs_mediaan = as.numeric(median(transactieprijs, na.rm = TRUE))),
        keyby = .(jaar5 = 5L * (trans_year %/% 5L))])

saveRDS(d, cfg$file_01_merged, compress = FALSE)
ri_log("Weggeschreven: %s", cfg$file_01_merged)
