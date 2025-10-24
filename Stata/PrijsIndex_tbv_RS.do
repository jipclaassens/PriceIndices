ssc install  parmest

clear
capture log close
cd "D:\OneDrive\OneDrive - Objectvision\VU\Projects\NVM Prijsindex" //laptop
cd "C:\Users\JipClaassens\OneDrive - Objectvision\VU\Projects\NVM Prijsindex" //ovsrv8
// cd "C:\Users\Jip\OneDrive - Objectvision\VU\Projects\NVM Prijsindex" //ovsrv6
log using temp/prijs_index.txt, text replace

///////////////////////////////////////////////////////////////////////////
//////////////////////// 		Prepare data		///////////////////////
///////////////////////////////////////////////////////////////////////////

global filename = "NVM_1985_2023"
global importdate = 20251024
global exportdate = 20251024

//1. first use EnhancedData/${filename}_${exportdate}_allvars_cleaned_geocoded.csv, this should contain bag attributes such as building year, building height, etc
//2. that one can be imported into GeoDMS, PriceIndices.dms. Before importing drop unnecesary attributes.
//3. There add spatial vars such as UAI, traveltimes etc
//4. Export from GeoDMS to EnhancedData/NVM_1985_2023_${filedate}_cleaned_spatial.csv
//5. Continue in this file to the regressions



**# 1. first use EnhancedData/${filename}_${exportdate}_allvars_cleaned_geocoded.csv, this should contain bag attributes such as building year, building height, etc
use EnhancedData/${filename}_cleaned_geocoded_${importdate}.dta, clear 

**# 2. that one can be imported into GeoDMS, PriceIndices.dms. Before importing drop unnecesary attributes.
keep obsid x y d_apartment d_terraced d_semidetached d_detached transactieprijs oppervlak perceel trans_year trans_month bouwjaar nkamers d_maintgood bag_pand_hoogte
		
**# 3. Export slim set to be used in GeoDMS
export delimited using Temp/${filename}_cleaned_geocoded_${importdate}_slim.csv, delimiter(";") replace

**# 4. In GeoDMS, import csv, add spatial vars such as UAI, traveltimes etc, and export again.

**# 5. Import file with spatial vars
import delimited using EnhancedData/${filename}_cleaned_geocoded_${importdate}_slim_spatial.csv, delimiter(";") clear
save EnhancedData/${filename}_cleaned_geocoded_${exportdate}_slim_spatial.dta, replace

use EnhancedData/${filename}_cleaned_geocoded_${exportdate}_slim_spatial.dta, clear

**# 6. Clean up
drop geometry rdc_10m_rel rdc_100m_rel

local replaceNullList = "buildingyear lotsize tt_train bag_pand_hoogte"
foreach x of local replaceNullList{
	replace `x' = "" if `x' == "null"
	destring `x', replace
}

replace buildingyear = . if buildingyear > 2025
replace buildingyear = . if buildingyear < 1000
replace buildingyear = 1600 if buildingyear < 1600
replace lotsize = . if lotsize >= 99999
replace tt_train = 1 if tt_train < 1
replace lotsize = 1 if lotsize == 0

**# 7. Prep

g d_highrise = 0 
replace d_highrise = 1 if bag_pand_hoogte >= 1500
g lntt_500k_inw = ln(tt_500k_inw)
g lntt_stations = ln(tt_train)
g lnprice = ln(price)
g lnsize = ln(size)
g lnlotsize = ln(lotsize)
g pricem2 = price / size

///nieuwe version op basis van analyse verderop.
g construction_period_label = ""
replace construction_period_label = "Construction 1925 and earlier" if buildingyear <= 1925 & buildingyear != . 
replace construction_period_label = "Construction 1926-1950" if buildingyear >= 1926 & buildingyear <= 1950
replace construction_period_label = "Construction 1951-1965" if buildingyear >= 1951 & buildingyear <= 1965
replace construction_period_label = "Construction 1966-1973" if buildingyear >= 1966 & buildingyear <= 1973
replace construction_period_label = "Construction 1974-1981" if buildingyear >= 1974 & buildingyear <= 1981
replace construction_period_label = "Construction 1982-1991" if buildingyear >= 1982 & buildingyear <= 1991
replace construction_period_label = "Construction 1992-2001" if buildingyear >= 1992 & buildingyear <= 2001
replace construction_period_label = "Construction 2002 and later " if buildingyear >= 2002 & buildingyear != .
encode construction_period_label, generate(construction_period)

g building_type_label = ""
replace building_type_label = "Terraced" if d_terraced == 1
replace building_type_label = "Semi-detached" if d_semidetached == 1
replace building_type_label = "Detached" if d_detached == 1
replace building_type_label = "Apartment" if d_apartment == 1
encode building_type_label, generate(building_type)

label variable price "Transaction price in euro"
label variable pricem2 "transaction price in euro per m2"
label variable size "size of property in m2"
label variable lotsize "size of parcel in m2"
label variable nroom "Number of rooms"
label variable d_apartment "Building type: apartment"
label variable d_terraced "Building type: terraced"
label variable d_semidetached "Building type: semi-detached"
label variable d_detached "Building type: detached"
label variable d_maintgood "Maintenance state is good"
label variable trans_year "transaction year"
label variable trans_month "transaction month"
label variable d_highrise "Building is high-rise"
label variable lnprice "log (transaction price)"
label variable lnsize "log (size)"
label variable lnlotsize "log (lotsize)"
label variable lntt_stations "log (traveltime to a trainstation)"
label variable lntt_500k_inw "log (traveltime to 500.000 inhabitants)"
label variable d_groennabij "Has green space nearby"
label variable uai_2021 "Urban attractivity index"

save Temp/${filename}_forRegression_${exportdate}.dta, replace
use Temp/${filename}_forRegression_${exportdate}.dta, clear


*************SAVE COEFFICIENTS*****************


//gg
// fvset base 8 construction_period  //8 is na 2001
// reg lnprice lnsize lnlotsize nroom i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_apartment != 1 & trans_year >= 2000, r allbaselevels
// parmest, saving(Temp/priceindex_${filedate}_eengezins) 
// use Temp/priceindex_${filedate}_eengezins.dta, clear
// export delimited using Output\Estimates_${filedate}_eengezins.csv, delimiter(";") replace
//
//app
// use Temp/${filename}_forRegression_${exportdate}.dta, clear
// fvset base 8 construction_period  //8 is na 2001
// reg lnprice lnsize nroom i.d_highrise i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_apartment == 1 & trans_year >= 2000,r
// parmest, saving(Temp/priceindex_${filedate}_meergezins) 
// use Temp/priceindex_${filedate}_meergezins.dta, clear
// export delimited using Output\Estimates_${filedate}_meergezins.csv, delimiter(";") replace



///// PER WP4

//Apartment
use Temp/${filename}_forRegression_${exportdate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize nroom i.d_highrise i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations uai i.d_groennabij if d_apartment == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${exportdate}_apartment) 
use Temp/priceindex_${exportdate}_apartment.dta, clear
export delimited using Output\Estimates_${exportdate}_apartment.csv, delimiter(";") replace

//Terraced
use Temp/${filename}_forRegression_${exportdate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize lnlotsize nroom i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations uai_2021 i.d_groennabij if d_terraced == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${exportdate}_terraced) 
use Temp/priceindex_${exportdate}_terraced.dta, clear
export delimited using Output\Estimates_${exportdate}_terraced.csv, delimiter(";") replace

//Semi-detached
use Temp/${filename}_forRegression_${exportdate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize lnlotsize nroom i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations uai_2021 i.d_groennabij if d_semidetached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${exportdate}_semidetached) 
use Temp/priceindex_${exportdate}_semidetached.dta, clear
export delimited using Output\Estimates_${exportdate}_semidetached.csv, delimiter(";") replace

//Detached
use Temp/${filename}_forRegression_${exportdate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize lnlotsize nroom i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations uai_2021 i.d_groennabij if d_detached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${exportdate}_detached) 
use Temp/priceindex_${exportdate}_detached.dta, clear
export delimited using Output\Estimates_${exportdate}_detached.csv, delimiter(";") replace



////LIMITED INFO, voor gemodelleerde realiseerde panden
///// PER WP4
use Temp/${filename}_forRegression_${exportdate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
//Apartment
reg lnprice lnsize i.d_highrise i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations uai_2021 i.d_groennabij if d_apartment == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${exportdate}_apartment_limit) 
use Temp/priceindex_${exportdate}_apartment_limit.dta, clear
export delimited using Output\Estimates_${exportdate}_apartment_limit.csv, delimiter(";") replace

//Terraced
use Temp/${filename}_forRegression_${exportdate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations uai_2021 i.d_groennabij if d_terraced == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${exportdate}_terraced_limit) 
use Temp/priceindex_${exportdate}_terraced_limit.dta, clear
export delimited using Output\Estimates_${exportdate}_terraced_limit.csv, delimiter(";") replace

//Semi-detached
use Temp/${filename}_forRegression_${exportdate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations uai_2021 i.d_groennabij if d_semidetached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${exportdate}_semidetached_limit) 
use Temp/priceindex_${exportdate}_semidetached_limit.dta, clear
export delimited using Output\Estimates_${exportdate}_semidetached_limit.csv, delimiter(";") replace

//Detached
use Temp/${filename}_forRegression_${exportdate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations uai_2021 i.d_groennabij if d_detached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${exportdate}_detached_limit) 
use Temp/priceindex_${exportdate}_detached_limit.dta, clear
export delimited using Output\Estimates_${exportdate}_detached_limit.csv, delimiter(";") replace






*** BEPAAL BOUWJAAR KLASSEN VOOR ALLE WAARNEMINGEN ***
**# STAP 1: Bouwjaar-klassen berekenen (8 gelijke groepen) **
xtile bouwjaar_klasse = bouwjaar, nq(8)
display "Bouwjaar-klassen succesvol gegenereerd!"

**# STAP 2: Min en Max bouwjaar per klasse bepalen **
* Zoek het laagste bouwjaar per bouwjaar_klasse
egen min_bouwjaar = min(bouwjaar), by(bouwjaar_klasse)
* Zoek het hoogste bouwjaar per bouwjaar_klasse
egen max_bouwjaar = max(bouwjaar), by(bouwjaar_klasse)
* Tel het aantal gebouwen per bouwjaar_klasse
egen count = count(bouwjaar), by(bouwjaar_klasse)

**# STAP 3: Opslaan in Excel **
preserve  
collapse (min) min_bouwjaar (max) max_bouwjaar (count) count, by(bouwjaar_klasse)


export excel bouwjaar_klasse min_bouwjaar max_bouwjaar count using bouwjaarK_data.xlsx, replace firstrow(variables)
restore  
display "Summary table successfully exported to bouwjaarK_data.xlsx!"






