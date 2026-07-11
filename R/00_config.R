# 00_config.R — centrale configuratie van de NVM-prijsindex-pipeline
# Elk stap-script source't dit bestand; alle paden/parameters staan hier.

suppressPackageStartupMessages({
  library(data.table)
  library(haven)
})

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "utils.R"))

cfg <- list()

## -- versietag: onderdeel van alle outputbestandsnamen van deze run ---------
cfg$tag <- "20260711"

## -- databronnen (OneDrive-projectmap, machine-onafhankelijk gezocht) --------
cfg$dir_project <- {
  cand <- c("C:/Users/Jip/OneDrive - Objectvision",
            "C:/Users/JipClaassens/OneDrive - Objectvision",
            "D:/OneDrive/OneDrive - Objectvision")
  hit <- cand[dir.exists(cand)]
  if (!length(hit)) stop("OneDrive-map niet gevonden; vul cfg$dir_project handmatig in (00_config.R)")
  file.path(hit[1], "VU/Projects/NVM Prijsindex")
}
cfg$dir_bron     <- file.path(cfg$dir_project, "Brondata")
cfg$dir_enhanced <- file.path(cfg$dir_project, "EnhancedData")
cfg$dir_output   <- file.path(cfg$dir_project, "Output")

## -- lokale werkmap voor (grote) tussenbestanden -----------------------------
cfg$dir_work <- file.path(get_geodms_dir("LocalDataDir") %||% "C:/LocalData", "PriceIndices")
dir.create(cfg$dir_work, recursive = TRUE, showWarnings = FALSE)

## -- map waar de geocode-invoer voor GeoDMS heen gaat ------------------------
cfg$dir_geocode_src <- {
  sd <- get_geodms_dir("SourceDataDir")
  if (!is.null(sd) && dir.exists(file.path(sd, "BAG"))) file.path(sd, "BAG/xy_source") else cfg$dir_work
}

## -- canonieke ruwe bronbestanden --------------------------------------------
# oud NVM-format, 1985-2021 (obj_hid_*-namen, geen coordinaten)
cfg$file_raw_pre2022  <- file.path(cfg$dir_bron, "Archief/nvm19852021_raw.dta")
# nieuw Brainbay-format, 2000-2023 (nvm20082022_raw_20240416.dta is hier een subset van)
cfg$file_raw_post2022 <- file.path(cfg$dir_bron, "NVM_2000_2023_raw.dta")

## -- referentiebestanden (validatie tegen de oude Stata-flow) -----------------
cfg$file_ref_cleaned    <- file.path(cfg$dir_enhanced, "NVM_1985_2023_20250519_cleaned.dta")
cfg$file_legacy_spatial <- file.path(cfg$dir_enhanced, "NVM_1985_2023_cleaned_geocoded_20251024_slim_spatial.csv")
cfg$dir_legacy_estimates <- cfg$dir_output   # Estimates_20251024_*.csv (in gebruik bij RS: niet overschrijven)

## -- schoningsparameters (identiek aan de Stata-globals) ----------------------
cfg$minprice    <- 25000
cfg$maxprice    <- 5000000
cfg$minsize     <- 10      # m2
cfg$maxsize     <- 500     # m2
cfg$minpricesqm <- 100
cfg$maxpricesqm <- 20000
cfg$maxrooms    <- 25

## -- overige aannames ---------------------------------------------------------
# De oude Stata-flow telde 'missing' impliciet als waar in vergelijkingen
# (missing > x is waar in Stata). Twee gevolgen daarvan zijn hier gefixt:
# onbekend onderhoud is NIET 'goed' en onbekende pandhoogte is NIET hoogbouw.
# Er zijn indicatoren (d_maint_onbekend, d_hoogte_onbekend) zodat specs de
# onbekend-categorie apart kunnen meenemen. Zet de vlaggen alleen op TRUE om
# de oude Stata-uitkomsten bit-voor-bit te reproduceren (de legacy-variant in
# stap 04 doet dat automatisch voor de validatie).
cfg$maint_na_as_good    <- FALSE
cfg$highrise_na_as_high <- FALSE
cfg$hoogbouwgrens_cm    <- 1500   # bag_pand_hoogte >= 15 m => d_highrise
cfg$seed             <- 20260711

## -- outputopties --------------------------------------------------------------
# CSV-kopieën van de geschoonde set naar OneDrive/EnhancedData schrijven (groot, sync!)
cfg$write_onedrive_exports <- FALSE

## -- afgeleide bestandsnamen ----------------------------------------------------
cfg$file_01_merged  <- file.path(cfg$dir_work, sprintf("01_merged_%s.rds", cfg$tag))
cfg$file_02_cleaned <- file.path(cfg$dir_work, sprintf("02_cleaned_%s.rds", cfg$tag))
cfg$file_03_geocode <- file.path(cfg$dir_geocode_src, sprintf("NVM_adressen_%s.csv", cfg$tag))
cfg$file_04_analysis <- function(variant) file.path(cfg$dir_work, sprintf("04_analysis_%s_%s.rds", variant, cfg$tag))
