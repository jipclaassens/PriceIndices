# run_all.R — draai pipelinestappen na elkaar
#
#   Rscript run_all.R            # stappen 1 t/m 3 (tot aan de GeoDMS-overdracht)
#   Rscript run_all.R 1 2        # alleen stap 1 en 2
#   Rscript run_all.R 4a         # geocodeerresultaat terugkoppelen
#   Rscript run_all.R 4 5        # na de spatial-vars-run: import + schattingen

f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
.ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()

stappen <- commandArgs(trailingOnly = TRUE)
if (!length(stappen)) stappen <- c("1", "2", "3")

scripts <- c(`1` = "01_merge_sources.R", `2` = "02_clean.R", `3` = "03_export_geocode.R",
             `4a` = "04a_merge_geocode.R", `3b` = "03b_export_spatial_input.R",
             `4` = "04_import_spatial.R", `5` = "05_estimate.R")

for (s in stappen) {
  if (is.na(scripts[s])) stop("Onbekende stap: ", s)
  cat("\n========== STAP", s, "-", scripts[s], "==========\n")
  source(file.path(.ri_script_dir, scripts[s]), echo = FALSE)
}
cat("\nKlaar met stappen:", paste(stappen, collapse = ", "), "\n")
