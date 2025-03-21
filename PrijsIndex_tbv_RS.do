ssc install  parmest

clear
capture log close
cd "D:\OneDrive\OneDrive - Objectvision\VU\Projects\NVM Prijsindex" //laptop
// cd "C:\Users\Jip\OneDrive - Objectvision\VU\Projects\NVM Prijsindex" //ovsrv6
log using temp/prijs_index_maa2025.txt, text replace

///////////////////////////////////////////////////////////////////////////
//////////////////////// 		Prepare data		///////////////////////
///////////////////////////////////////////////////////////////////////////

global filedate = 20250320

import delimited EnhancedData/NVM_${filedate}_points.csv, clear
save Temp/NVM_points_${filedate}.dta, replace

use Temp/NVM_points_${filedate}.dta, clear

// local replaceNullList = "bouwjaar lotsize tt_stations_2006 nroom"
// foreach x of local replaceNullList{
// 	replace `x' = "" if `x' == "null"
// 	destring `x', replace
// }

g lntt_500k_inw = ln(tt_500k_inw)
replace tt_stations_2006 = 1 if tt_stations_2006 < 1
g lntt_stations_2006 = ln(tt_stations_2006)
g lnprice = ln(price)
g lnsize = ln(size)
replace lotsize = 1 if lotsize == 0
g lnlotsize = ln(lotsize)
g pricem2 = price / size


///nieuwe version op basis van analyse verderop.
g construction_period_label = ""
replace construction_period_label = "Construction 1925 and earlier" if bouwjaar <= 1925 & bouwjaar != . 
replace construction_period_label = "Construction 1926-1950" if bouwjaar >= 1926 & bouwjaar <= 1950
replace construction_period_label = "Construction 1951-1965" if bouwjaar >= 1951 & bouwjaar <= 1965
replace construction_period_label = "Construction 1966-1973" if bouwjaar >= 1966 & bouwjaar <= 1973
replace construction_period_label = "Construction 1974-1981" if bouwjaar >= 1974 & bouwjaar <= 1981
replace construction_period_label = "Construction 1982-1991" if bouwjaar >= 1982 & bouwjaar <= 1991
replace construction_period_label = "Construction 1992-2001" if bouwjaar >= 1992 & bouwjaar <= 2001
replace construction_period_label = "Construction 2002 and later " if bouwjaar >= 2002 & bouwjaar != .
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
label variable d_highrise "Building is high-rise"
label variable lnprice "log (transaction price)"
label variable lnsize "log (size)"
label variable lnlotsize "log (lotsize)"
label variable lntt_stations_2006 "log (traveltime to a trainstation)"
label variable lntt_500k_inw "log (traveltime to 500.000 inhabitants)"
label variable d_groennabij_2017 "Has green space nearby"
label variable uai_2021 "Urban attractivity index"

save Temp/NVM_points_enhanced_${filedate}.dta, replace
use Temp/NVM_points_enhanced_${filedate}.dta, clear


*************SAVE COEFFICIENTS*****************


//gg
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize lnlotsize nroom i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_apartment != 1 & trans_year >= 2000, r allbaselevels
parmest, saving(Temp/priceindex_${filedate}_eengezins) 
use Temp/priceindex_${filedate}_eengezins.dta, clear
export delimited using Output\Estimates_${filedate}_eengezins.csv, delimiter(";") replace

//app
use Temp/NVM_points_enhanced_${filedate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize nroom i.d_highrise i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_apartment == 1 & trans_year >= 2000,r
parmest, saving(Temp/priceindex_${filedate}_meergezins) 
use Temp/priceindex_${filedate}_meergezins.dta, clear
export delimited using Output\Estimates_${filedate}_meergezins.csv, delimiter(";") replace



///// PER WP4

//Apartment
use Temp/NVM_points_enhanced_${filedate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize nroom i.d_highrise i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_apartment == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_apartment) 
use Temp/priceindex_${filedate}_apartment.dta, clear
export delimited using Output\Estimates_${filedate}_apartment.csv, delimiter(";") replace

//Terraced
use Temp/NVM_points_enhanced_${filedate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize lnlotsize nroom i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_terraced == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_terraced) 
use Temp/priceindex_${filedate}_terraced.dta, clear
export delimited using Output\Estimates_${filedate}_terraced.csv, delimiter(";") replace

//Semi-detached
use Temp/NVM_points_enhanced_${filedate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize lnlotsize nroom i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_semidetached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_semidetached) 
use Temp/priceindex_${filedate}_semidetached.dta, clear
export delimited using Output\Estimates_${filedate}_semidetached.csv, delimiter(";") replace

//Detached
use Temp/NVM_points_enhanced_${filedate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize lnlotsize nroom i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_detached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_detached) 
use Temp/priceindex_${filedate}_detached.dta, clear
export delimited using Output\Estimates_${filedate}_detached.csv, delimiter(";") replace



////LIMITED INFO, voor gemodelleerde realiseerde panden
///// PER WP4
use Temp/NVM_points_enhanced_${filedate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
//Apartment
reg lnprice lnsize i.d_highrise i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_apartment == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_apartment_limit) 
use Temp/priceindex_${filedate}_apartment_limit.dta, clear
export delimited using Output\Estimates_${filedate}_apartment_limit.csv, delimiter(";") replace

//Terraced
use Temp/NVM_points_enhanced_${filedate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_terraced == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_terraced_limit) 
use Temp/priceindex_${filedate}_terraced_limit.dta, clear
export delimited using Output\Estimates_${filedate}_terraced_limit.csv, delimiter(";") replace

//Semi-detached
use Temp/NVM_points_enhanced_${filedate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_semidetached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_semidetached_limit) 
use Temp/priceindex_${filedate}_semidetached_limit.dta, clear
export delimited using Output\Estimates_${filedate}_semidetached_limit.csv, delimiter(";") replace

//Detached
use Temp/NVM_points_enhanced_${filedate}.dta, clear
fvset base 8 construction_period  //8 is na 2001
reg lnprice lnsize i.d_maintgood i.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_detached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_detached_limit) 
use Temp/priceindex_${filedate}_detached_limit.dta, clear
export delimited using Output\Estimates_${filedate}_detached_limit.csv, delimiter(";") replace






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

